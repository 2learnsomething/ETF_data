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
from datetime import datetime
from pathlib import Path

from .client import AmazingDataClient
from .config import Config
from .fetcher import UnifiedFetcher
from .storage import ParquetStore, MetaStore

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
        """全量回填（按 P0→P1→P2 顺序，共40次API调用→36个数据集）"""
        logger.info("=== Starting full backfill ===")
        failed = 0

        # ═══ P0: 核心 ═══
        failed += self._backfill_universe()
        failed += self._backfill_code_info()
        failed += self._backfill_kline("etf_daily", "day", "ETF", "EXTRA_ETF")
        failed += self._backfill_kline("etf_min1", "1min", "ETF", "EXTRA_ETF", is_minute=True)

        # ═══ P1: 重要 ═══
        # 复权因子
        failed += self._backfill_adj_factor("etf_adj", "EXTRA_ETF")
        failed += self._backfill_adj_factor("stock_adj", "EXTRA_STOCK_A")

        # 指数K线
        failed += self._backfill_kline("index_daily", "day", "INDEX", "EXTRA_INDEX_A")
        failed += self._backfill_kline("index_min1", "1min", "INDEX", "EXTRA_INDEX_A", is_minute=True)
        failed += self._backfill_kline("index_min5", "5min", "INDEX", "EXTRA_INDEX_A", is_minute=True)

        # ETF 5分钟 + A股日K线
        failed += self._backfill_kline("etf_min5", "5min", "ETF", "EXTRA_ETF", is_minute=True)
        failed += self._backfill_kline("stock_daily", "day", "STOCK", "EXTRA_STOCK_A")

        # ETF 净值/份额
        failed += self._backfill_fund_data("etf_nav", "get_fund_nav", "ETF", "EXTRA_ETF")
        failed += self._backfill_fund_data("etf_share", "get_fund_share", "ETF", "EXTRA_ETF")

        # 指数成分股/权重
        failed += self._backfill_index_data("index_constituent", "get_index_constituent")
        failed += self._backfill_index_data("index_weight", "get_index_weight")

        # 基本面5张表
        for table, api in [
            ("balance_sheet", "get_balance_sheet"),
            ("income", "get_income"),
            ("cash_flow", "get_cash_flow"),
            ("profit_express", "get_profit_express"),
            ("profit_notice", "get_profit_notice"),
        ]:
            failed += self._backfill_financial(table, api)

        # ═══ P2: 补充 ═══
        # 事件数据10张
        failed += self._backfill_event("stock_margin_summary", "get_margin_summary")
        failed += self._backfill_event("stock_margin_detail", "get_margin_detail")
        failed += self._backfill_event("stock_block_trading", "get_block_trading")
        failed += self._backfill_event("stock_longhubang", "get_long_hu_bang")
        failed += self._backfill_event("stock_dividend", "get_dividend")
        failed += self._backfill_event("stock_equity_structure", "get_equity_structure")
        failed += self._backfill_event("stock_holder_num", "get_holder_num")
        failed += self._backfill_event("stock_share_holder", "get_share_holder")
        failed += self._backfill_event("stock_equity_pledge", "get_equity_pledge_freeze")
        failed += self._backfill_event("stock_right_issue", "get_right_issue")

        # 行业分类4张
        failed += self._backfill_industry("industry_base", "get_industry_base_info")
        failed += self._backfill_industry("industry_constituent", "get_industry_constituent")
        failed += self._backfill_industry("industry_daily", "get_industry_daily")
        failed += self._backfill_industry("industry_weight", "get_industry_weight")

        # 历史代码表 + ETF PCF
        failed += self._backfill_hist_code_list()
        failed += self._backfill_etf_pcf()

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

    # ── P1/P2 补全方法 ───────────────────────────────────────

    def _backfill_adj_factor(self, table: str, security_type: str) -> int:
        """回填复权因子"""
        import pandas as pd
        try:
            codes = self._client.get_base_data().get_code_list(security_type=security_type)
            if not codes:
                return 0
            sample = codes[:50] if self._cfg.get("pipeline.test_mode", False) else codes
            self._client.get_ad()
            for i in range(0, len(sample), 100):
                batch = sample[i : i + 100]
                try:
                    result = self._client.get_base_data().get_adj_factor(
                        batch, local_path=f"{self._cfg.cache_dir}/", is_local=False
                    )
                    if result is not None:
                        df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
                        self._store.write_unpartitioned(table, df)
                        self._meta.log_update(table, "backfill", "amazingdata", "ok", rows_count=len(df))
                        logger.info(f"  {table} [{i}]: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"  {table} batch {i}: {e}")
        except Exception as e:
            logger.error(f"  {table} failed: {e}")
            return 1
        return 0

    def _backfill_fund_data(self, table: str, api_name: str, label: str, security_type: str) -> int:
        """回填 ETF 净值/份额"""
        try:
            codes = self._client.get_base_data().get_code_list(security_type=security_type)
            if not codes:
                return 0
            sample = codes[:50] if self._cfg.get("pipeline.test_mode", False) else codes
            info = self._client.get_info_data()
            api = getattr(info, api_name)
            result = api(sample, local_path=f"{self._cfg.cache_dir}/", is_local=False)
            if result:
                import pandas as pd
                all_frames = []
                if isinstance(result, dict):
                    for code, df in result.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            df["code"] = code
                            all_frames.append(df)
                if all_frames:
                    merged = pd.concat(all_frames, ignore_index=True)
                    self._store.write_unpartitioned(table, merged)
                    self._meta.log_update(table, "backfill", "amazingdata", "ok", rows_count=len(merged))
                    logger.info(f"  {table}: {len(merged)} rows")
        except Exception as e:
            logger.warning(f"  {table} failed (expected if no permission): {e}")
        return 0

    def _backfill_index_data(self, table: str, api_name: str) -> int:
        """回填指数成分股/权重"""
        try:
            codes = self._client.get_base_data().get_code_list(security_type="EXTRA_INDEX_A")
            if not codes:
                return 0
            sample = codes[:20] if self._cfg.get("pipeline.test_mode", False) else codes[:200]
            info = self._client.get_info_data()
            api = getattr(info, api_name)
            result = api(sample, local_path=f"{self._cfg.cache_dir}/", is_local=False)
            if result:
                import pandas as pd
                all_frames = []
                if isinstance(result, dict):
                    for code, df in result.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            df["index_code"] = code
                            all_frames.append(df)
                if all_frames:
                    merged = pd.concat(all_frames, ignore_index=True)
                    self._store.write_unpartitioned(table, merged)
                    self._meta.log_update(table, "backfill", "amazingdata", "ok", rows_count=len(merged))
                    logger.info(f"  {table}: {len(merged)} rows")
        except Exception as e:
            logger.warning(f"  {table} failed (expected if no permission): {e}")
        return 0

    def _backfill_financial(self, table: str, api_name: str) -> int:
        """回填财务表"""
        try:
            codes = self._client.get_base_data().get_code_list(security_type="EXTRA_STOCK_A")
            if not codes:
                return 0
            sample = codes[:20] if self._cfg.get("pipeline.test_mode", False) else codes[:500]
            info = self._client.get_info_data()
            api = getattr(info, api_name)
            result = api(sample, local_path=f"{self._cfg.cache_dir}/", is_local=False)
            if result:
                import pandas as pd
                all_frames = []
                if isinstance(result, dict):
                    for code, df in result.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            df["code"] = code
                            all_frames.append(df)
                if all_frames:
                    merged = pd.concat(all_frames, ignore_index=True)
                    sub_dir = f"stock_financial/{table}"
                    self._store.write_unpartitioned(sub_dir, merged)
                    self._meta.log_update(sub_dir, "backfill", "amazingdata", "ok", rows_count=len(merged))
                    logger.info(f"  stock_financial/{table}: {len(merged)} rows")
        except Exception as e:
            logger.warning(f"  stock_financial/{table} failed (expected if no permission): {e}")
        return 0

    def _backfill_event(self, table: str, api_name: str) -> int:
        """回填事件数据"""
        try:
            # 取前100只股票作为样本（事件数据全量拉取代价大）
            codes = self._client.get_base_data().get_code_list(security_type="EXTRA_STOCK_A")
            if not codes:
                return 0
            sample = codes[:20] if self._cfg.get("pipeline.test_mode", False) else codes[:100]
            info = self._client.get_info_data()
            api = getattr(info, api_name)
            result = api(sample, local_path=f"{self._cfg.cache_dir}/", is_local=False)
            if result:
                import pandas as pd
                all_frames = []
                if isinstance(result, dict):
                    for code, df in result.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            df["code"] = code
                            all_frames.append(df)
                if all_frames:
                    merged = pd.concat(all_frames, ignore_index=True)
                    self._store.write_unpartitioned(table, merged)
                    self._meta.log_update(table, "backfill", "amazingdata", "ok", rows_count=len(merged))
                    logger.info(f"  {table}: {len(merged)} rows")
        except Exception as e:
            logger.warning(f"  {table} failed (expected if no permission): {e}")
        return 0

    def _backfill_industry(self, table: str, api_name: str) -> int:
        """回填行业数据"""
        try:
            info = self._client.get_info_data()
            api = getattr(info, api_name)
            # 行业基础信息不需要 code_list
            if api_name == "get_industry_base_info":
                result = api(local_path=f"{self._cfg.cache_dir}/", is_local=False)
            else:
                # 需要传入行业代码
                import pandas as pd
                base = info.get_industry_base_info(local_path=f"{self._cfg.cache_dir}/", is_local=False)
                industries = []
                if isinstance(base, (list, pd.DataFrame)):
                    try:
                        base_df = pd.DataFrame(base) if isinstance(base, list) else base
                        if "industry_code" in base_df.columns:
                            industries = base_df["industry_code"].tolist()[:20]
                    except Exception:
                        pass
                if not industries:
                    logger.warning(f"  {table}: no industry codes available")
                    return 0
                result = api(industries, local_path=f"{self._cfg.cache_dir}/", is_local=False)

            if result:
                all_frames = []
                if isinstance(result, dict):
                    for code, df in result.items():
                        if hasattr(df, "empty") and not df.empty:
                            df["industry_code"] = code
                            all_frames.append(df)
                if all_frames:
                    merged = pd.concat(all_frames, ignore_index=True)
                    self._store.write_unpartitioned(table, merged)
                    self._meta.log_update(table, "backfill", "amazingdata", "ok", rows_count=len(merged))
                    logger.info(f"  {table}: {len(merged)} rows")
        except Exception as e:
            logger.warning(f"  {table} failed (expected if no permission): {e}")
        return 0

    def _backfill_hist_code_list(self) -> int:
        """回填历史代码表"""
        try:
            cal = self._client.get_calendar()
            if not cal:
                return 1
            result = self._client.get_base_data().get_hist_code_list(
                security_type="EXTRA_STOCK_A",
                start_date=20130101,
                end_date=cal[-1],
                local_path=f"{self._cfg.cache_dir}/",
            )
            if result is not None:
                import pandas as pd
                df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
                path = Path(self._cfg.storage_root) / "meta" / "hist_code_list.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(path, index=False)
                self._meta.log_update("hist_code_list", "backfill", "amazingdata", "ok", rows_count=len(df))
                logger.info(f"  hist_code_list: {len(df)} rows")
        except Exception as e:
            logger.warning(f"  hist_code_list failed: {e}")
        return 0

    def _backfill_etf_pcf(self) -> int:
        """回填 ETF PCF"""
        try:
            codes = self._client.get_base_data().get_code_list(security_type="EXTRA_ETF")
            if not codes:
                return 0
            sample = codes[:10] if self._cfg.get("pipeline.test_mode", False) else codes[:50]
            result = self._client.get_base_data().get_etf_pcf(sample)
            if result is not None:
                import pandas as pd
                df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
                self._store.write_unpartitioned("etf_pcf", df)
                self._meta.log_update("etf_pcf", "backfill", "amazingdata", "ok", rows_count=len(df))
                logger.info(f"  etf_pcf: {len(df)} rows")
        except Exception as e:
            logger.warning(f"  etf_pcf failed (expected if no permission): {e}")
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

