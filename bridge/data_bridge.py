"""AmazingData 桥接层 — ETF_Rotation_Strategy 数据消费接口

提供与现有 ETF_data_bridge.py 兼容的 API，同时新增 Plan B 需要的三个衔接接口。
所有数据从本地 Parquet 读取，不依赖网络。
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# 确保能找到 src/ 包
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.amazingdata.storage import ParquetStore, MetaStore

logger = logging.getLogger(__name__)


class AmazingDataBridge:
    """AmazingData 数据桥接层

    消费端（ETF_Rotation / technical_score.py）通过此接口读取数据。
    所有读取走本地 Parquet，毫秒级响应。
    """

    def __init__(self, data_root: str = "/mnt/etf_data"):
        self._root = Path(data_root)
        self._store = ParquetStore(data_root)
        self._meta = MetaStore(data_root)
        self._init_cache()

    def _init_cache(self) -> None:
        """初始化内部缓存"""
        self._etf_list: pd.DataFrame | None = None
        self._close_cache: tuple[int, pd.DataFrame] | None = None  # (days, df)

    # ── 兼容原有 ETF_data_bridge.py 接口 ─────────────────────

    def get_etf_list(self) -> pd.DataFrame:
        """返回全部 ETF 清单

        Returns:
            DataFrame[symbol, name, type, ...]
        """
        if self._etf_list is not None:
            return self._etf_list
        path = self._root / "meta" / "etf_universe.parquet"
        if path.exists():
            self._etf_list = pd.read_parquet(path)
            # 统一列名：code → symbol
            if "code" in self._etf_list.columns and "symbol" not in self._etf_list.columns:
                self._etf_list = self._etf_list.rename(columns={"code": "symbol"})
        else:
            self._etf_list = pd.DataFrame(columns=["symbol", "name", "type"])
        return self._etf_list

    def get_etf_daily(self, symbol: str, days: int = 250) -> pd.DataFrame:
        """返回单只 ETF 最近 N 个交易日日线（兼容旧接口）

        Args:
            symbol: '510050' 或 '510050.SH'
            days: 返回最近 N 个交易日

        Returns:
            DataFrame[date, open, high, low, close, volume, amount, pct_chg]
        """
        if "." not in symbol and not symbol.endswith((".SH", ".SZ", ".BJ")):
            # 自动补后缀（兼容旧调用方式）
            symbol = f"{symbol}.SH"

        # 按需读取最近几年的数据
        now = datetime.now()
        # 从当前年份往前搜，最多搜 5 年
        for offset in range(5):
            test_year = now.year - offset
            test_path = self._root / "parquet" / "etf_daily" / f"year={test_year}" / f"{symbol}.parquet"
            if test_path.exists():
                start_year = test_year
                break
        else:
            start_year = max(now.year - 3, 2013)

        df = self._store.read_kline("etf_daily", symbol, start_year=start_year, end_year=now.year)

        if df.empty:
            return df

        # 按日期倒序取 days 条
        if "kline_time" in df.columns:
            df = df.sort_values("kline_time", ascending=False).head(days)
            df = df.sort_values("kline_time").reset_index(drop=True)
            df = df.rename(columns={"kline_time": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date

        # 计算涨跌幅
        if "close" in df.columns and "open" in df.columns:
            df["pct_chg"] = df["close"].pct_change() * 100

        return df

    def get_all_close(self, days: int = 60) -> pd.DataFrame:
        """返回全市场 ETF 收盘价矩阵（technical_score.py 核心输入）

        行 = 日期，列 = ETF 代码

        Args:
            days: 最近 N 个交易日

        Returns:
            DataFrame[date_index, symbol_columns...]
        """
        # 缓存检查
        if self._close_cache is not None and self._close_cache[0] >= days:
            return self._close_cache[1]

        etf_list = self.get_etf_list()
        if etf_list.empty:
            return pd.DataFrame()

        symbols = etf_list["symbol"].tolist() if "symbol" in etf_list.columns else []
        if not symbols:
            return pd.DataFrame()

        # 批量读取
        now = datetime.now()
        # 从当前年份往前搜
        start_year = now.year
        for offset in range(5):
            test_year = now.year - offset
            test_dir = self._root / "parquet" / "etf_daily" / f"year={test_year}"
            if test_dir.exists():
                start_year = test_year
                break
        frames = []
        for sym in symbols[:100]:  # 限制数量防止 OOM
            df = self._store.read_kline("etf_daily", sym, start_year=start_year, end_year=now.year)
            if not df.empty and "close" in df.columns and "kline_time" in df.columns:
                sub = df[["kline_time", "close"]].copy()
                sub["kline_time"] = pd.to_datetime(sub["kline_time"])
                sub = sub.rename(columns={"kline_time": "date", "close": sym})
                frames.append(sub)

        if not frames:
            return pd.DataFrame()

        # 合并为矩阵
        from functools import reduce

        merged = reduce(
            lambda left, right: pd.merge(left, right, on="date", how="outer"), frames
        )
        merged = merged.sort_values("date").tail(days).reset_index(drop=True)
        merged = merged.set_index("date")

        self._close_cache = (days, merged)
        return merged

    # ── Plan B 新增的三个衔接接口 ──────────────────────────

    def get_kline_batch(
        self, symbols: list[str], start_date: str, end_date: str
    ) -> dict[str, pd.DataFrame]:
        """批量获取多只 ETF 指定日期范围日线

        Args:
            symbols: ['510050.SH', '510300.SH', ...]
            start_date: '2024-01-01'
            end_date: '2024-12-31'

        Returns:
            { '510050.SH': DataFrame[code, kline_time, open, high, low, close, volume, amount], ... }
        """
        sy = int(start_date[:4])
        ey = int(end_date[:4])
        result = {}
        for sym in symbols:
            df = self._store.read_kline("etf_daily", sym, start_year=sy, end_year=ey)
            if not df.empty:
                # 按日期范围过滤
                mask = (df["kline_time"] >= start_date) & (df["kline_time"] <= f"{end_date} 23:59:59")
                df = df[mask]
                if not df.empty:
                    result[sym] = df
        return result

    def get_minute_data(
        self, symbol: str, date: str, period: str = "1min"
    ) -> pd.DataFrame:
        """获取单只 ETF 指定日期的分钟 K 线

        Args:
            symbol: '510050.SH'
            date: '2024-01-15'
            period: '1min' / '5min'

        Returns:
            DataFrame[kline_time, open, high, low, close, volume, amount]
        """
        table_map = {"1min": "etf_min1", "5min": "etf_min5"}
        table = table_map.get(period, "etf_min1")
        dt = pd.Timestamp(date)
        y, m = dt.year, dt.month

        store_root = self._root / "parquet"
        path = store_root / table / f"year={y}" / f"month={m:02d}" / f"{symbol}.parquet"

        if not path.exists():
            return pd.DataFrame()

        df = pd.read_parquet(path)
        if "kline_time" in df.columns:
            mask = pd.to_datetime(df["kline_time"]).dt.date == pd.Timestamp(date).date()
            df = df[mask].reset_index(drop=True)
        return df

    def get_code_info_cached(self) -> pd.DataFrame:
        """获取代码信息缓存（涨跌停价、最小变动单位）

        优先从 meta.db 缓存读取，如过期则从 AmazingData 刷新。

        Returns:
            DataFrame[code, pre_close, high_limited, low_limited, price_tick]
        """
        # 尝试从缓存读取
        cached = self._meta.cache_get("code_info")
        if cached:
            try:
                import json
                data = json.loads(cached)
                return pd.DataFrame(data)
            except Exception:
                pass

        # 从 parquet 文件读取
        path = self._root / "meta" / "code_info.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            # 缓存到 SQLite
            import json
            self._meta.cache_set("code_info", json.dumps(df.to_dict("records")))
            return df

        return pd.DataFrame(columns=["code", "pre_close", "high_limited", "low_limited", "price_tick"])
