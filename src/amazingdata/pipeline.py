"""数据管道调度入口

支持全量回填、增量更新、缺失补拉三种模式。

用法:
    python -m src.amazingdata.pipeline --mode backfill
    python -m src.amazingdata.pipeline --mode incremental
    python -m src.amazingdata.pipeline --mode backfill-missing --table etf_daily --days 7
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# 确保能找到 src/ 包
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.amazingdata.client import AmazingDataClient
from src.amazingdata.config import Config
from src.amazingdata.fetcher import UnifiedFetcher
from src.amazingdata.storage import ParquetStore, MetaStore

logger = logging.getLogger("amazingdata.pipeline")


class Pipeline:
    """数据管道编排器"""

    def __init__(self, mode: str = "incremental"):
        self._cfg = Config()
        self._client = AmazingDataClient()
        self._fetcher = UnifiedFetcher()
        self._store = ParquetStore(self._cfg.storage_root)
        self._meta = MetaStore(self._cfg.storage_root)
        self._mode = mode

    # ── 公开方法 ──────────────────────────────────────────────

    def run(self) -> int:
        """运行管道

        Returns:
            失败任务数（0=全部成功）
        """
        self._client.login()
        cal = self._client.get_calendar()

        if not cal:
            logger.error("No calendar available, aborting")
            return 1

        if self._mode == "backfill":
            return self._run_backfill()
        elif self._mode == "backfill-missing":
            return self._run_backfill_missing()
        else:
            return self._run_incremental()

    # ── 全量回填 ─────────────────────────────────────────────

    def _run_backfill(self) -> int:
        """全量回填（按 P0→P1→P2 顺序）"""
        logger.info("=== Starting full backfill ===")
        failed = 0

        # P0: 清单
        failed += self._backfill_universe()
        failed += self._backfill_code_info()

        # P0: 日K线（核心）
        failed += self._backfill_kline("etf_daily", "day", "ETF", "EXTRA_ETF")

        # P0: 分钟K线
        failed += self._backfill_kline("etf_min1", "1min", "ETF", "EXTRA_ETF", is_minute=True)

        logger.info(f"=== Backfill complete: {failed} failures ===")
        return failed

    def _backfill_universe(self) -> int:
        """回填资产清单"""
        failed = 0
        for stype, fname in [
            ("EXTRA_ETF", "etf_universe"),
            ("EXTRA_INDEX_A", "index_universe"),
            ("EXTRA_STOCK_A", "stock_universe"),
        ]:
            try:
                codes = self._client.get_base_data().get_code_list(security_type=stype)
                if codes:
                    import pandas as pd
                    df = pd.DataFrame({"code": codes, "name": codes})
                    path = Path(self._cfg.storage_root) / "meta" / f"{fname}.parquet"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(path, index=False)
                    logger.info(f"  {fname}: {len(df)} items")
                    self._meta.log_update(fname, "backfill", "amazingdata", "ok", rows_count=len(df))
            except Exception as e:
                logger.error(f"  {fname} failed: {e}")
                self._meta.log_update(fname, "backfill", "amazingdata", "error", error_msg=str(e))
                failed += 1
        return failed

    def _backfill_code_info(self) -> int:
        """回填代码信息（涨跌停价）"""
        try:
            info = self._client.get_base_data().get_code_info(security_type="EXTRA_ETF")
            if info is not None and not info.empty:
                path = Path(self._cfg.storage_root) / "meta" / "code_info.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                info.to_parquet(path, index=False)
                logger.info(f"  code_info: {len(info)} items")
                self._meta.log_update("code_info", "backfill", "amazingdata", "ok", rows_count=len(info))
        except Exception as e:
            logger.warning(f"  code_info failed (expected if no permission): {e}")
            self._meta.log_update("code_info", "backfill", "amazingdata", "error", error_msg=str(e))
        return 0

    def _backfill_kline(
        self,
        table: str,
        period: str,
        label: str,
        security_type: str,
        is_minute: bool = False,
    ) -> int:
        """回填K线数据"""
        import pandas as pd
        try:
            codes = self._client.get_base_data().get_code_list(security_type=security_type)
        except Exception as e:
            logger.error(f"  Cannot get code list for {security_type}: {e}")
            return 1

        if not codes:
            logger.warning(f"  No codes for {security_type}")
            return 0

        # 限制测试数量
        sample = codes[:50] if self._cfg.get("pipeline.test_mode", False) else codes
        batch_size = (
            self._cfg.get("pipeline.batch_size_etf_min1", 10) if is_minute
            else self._cfg.get("pipeline.batch_size_etf_kline", 50)
        )

        failed = 0
        for i in range(0, len(sample), batch_size):
            batch = sample[i : i + batch_size]
            try:
                result = self._fetcher.fetch_kline(
                    batch, "2013-01-01", datetime.now().strftime("%Y-%m-%d"), period
                )
                for sym, df in result.items():
                    if df.empty:
                        continue
                    year = pd.to_datetime(df["kline_time"].iloc[0]).year
                    self._store.write_kline(table, df, year, partition_by_month=is_minute)
                    self._meta.log_update(
                        table, "backfill", "amazingdata", "ok",
                        rows_count=len(df), symbol=sym,
                    )
                logger.info(f"  {label} {period} [{i}/{len(sample)}]: +{sum(len(v) for v in result.values())} rows")
            except Exception as e:
                logger.error(f"  Batch {i} failed: {e}")
                failed += 1

        return failed

    # ── 增量更新 ─────────────────────────────────────────────

    def _run_incremental(self) -> int:
        """增量更新（仅拉取最新数据）"""
        logger.info("=== Incremental update ===")
        cal = self._client.get_calendar()
        today_str = datetime.now().strftime("%Y-%m-%d")

        # 判断是否为交易日
        today_int = int(datetime.now().strftime("%Y%m%d"))
        if today_int not in cal:
            logger.info(f"{today_int} is not a trade day, skip")
            return 0

        failed = 0

        # 固定任务：ETF日K线
        last = self._meta.get_last_update("etf_daily")
        if last and last >= today_str:
            logger.info("  etf_daily already up to date")
        else:
            failed += self._incremental_kline("etf_daily", "day", "EXTRA_ETF")

        logger.info(f"=== Incremental done: {failed} failures ===")
        return failed

    def _incremental_kline(self, table: str, period: str, security_type: str) -> int:
        """增量更新K线"""
        import pandas as pd  # noqa: F811

        try:
            codes = self._client.get_base_data().get_code_list(security_type=security_type)
        except Exception:
            return 1

        if not codes:
            return 0

        today = datetime.now().strftime("%Y-%m-%d")
        sample = codes[:50]

        try:
            result = self._fetcher.fetch_kline(sample, today, today, period)
            for sym, df in result.items():
                if df.empty:
                    continue
                year = pd.to_datetime(df["kline_time"].iloc[0]).year
                self._store.write_kline(table, df, year)
                self._meta.log_update(table, "incremental", "amazingdata", "ok", rows_count=len(df), symbol=sym)
            logger.info(f"  {table}: +{sum(len(v) for v in result.values())} rows")
        except Exception as e:
            logger.error(f"  {table} failed: {e}")
            return 1
        return 0

    # ── 缺失补拉 ─────────────────────────────────────────────

    def _run_backfill_missing(self) -> int:
        """补拉缺失天数"""
        logger.info("=== Backfill missing days ===")
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="AmazingData 数据管道")
    parser.add_argument("--mode", default="incremental", choices=["backfill", "incremental", "backfill-missing"])
    parser.add_argument("--table", help="目标表名")
    parser.add_argument("--days", type=int, default=7, help="回补天数")
    parser.add_argument("--test", action="store_true", help="测试模式（仅拉50只）")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    # 日志配置
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    if args.test:
        import os
        os.environ["AMAZINGDATA_TEST_MODE"] = "1"

    pipeline = Pipeline(mode=args.mode)
    failed = pipeline.run()
    sys.exit(failed if failed > 0 else 0)


if __name__ == "__main__":
    main()
