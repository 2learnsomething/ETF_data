"""统一数据获取接口（Fetcher）

AmazingData 优先，备源自动降级。
所有方法返回 pandas DataFrame，与上游 bridge 格式一致。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .client import AmazingDataClient
from .config import Config
from .storage import ParquetStore, MetaStore

logger = logging.getLogger(__name__)


class UnifiedFetcher:
    """统一数据获取器

    按数据源优先级路由：
        1. AmazingData（主源）
        2. efinance（ETF/指数 K 线降级）
        3. baostock（A 股日线/复权降级）
        4. akshare（其他数据降级）
    """

    def __init__(self) -> None:
        self._cfg = Config()
        self._client = AmazingDataClient()
        self._store = ParquetStore(self._cfg.storage_root)
        self._meta = MetaStore(self._cfg.storage_root)
        self._amazing_available = self._client.is_available
        self._efinance_available: bool | None = None
        self._baostock_available: bool | None = None

    # ── 基础数据 ──────────────────────────────────────────────

    def fetch_code_list(self, security_type: str = "EXTRA_ETF") -> list[str]:
        """获取代码清单"""
        if self._amazing_available:
            try:
                return self._client.get_base_data().get_code_list(
                    security_type=security_type
                )
            except Exception as e:
                logger.warning(f"AmazingData code_list failed: {e}")
        # 备源：从已有 parquet 读取
        table_map = {
            "EXTRA_ETF": "etf_universe",
            "EXTRA_INDEX_A": "index_universe",
            "EXTRA_STOCK_A": "stock_universe",
        }
        fname = table_map.get(security_type, "")
        if fname:
            path = Path(self._cfg.storage_root) / "meta" / f"{fname}.parquet"
            if path.exists():
                df = pd.read_parquet(path)
                return df["code"].tolist()
        return []

    def fetch_code_info(self, symbols: list[str] | None = None) -> pd.DataFrame:
        """获取代码信息（涨跌停价、昨收）"""
        if self._amazing_available:
            try:
                info = self._client.get_base_data().get_code_info(
                    security_type="EXTRA_ETF"
                )
                if symbols and not info.empty:
                    info = info[info.index.isin(symbols)]
                return info
            except Exception as e:
                logger.warning(f"AmazingData code_info failed: {e}")
        # 备源：从 parquet 缓存读取
        path = Path(self._cfg.storage_root) / "meta" / "code_info.parquet"
        if path.exists():
            info = pd.read_parquet(path)
            if symbols:
                info = info[info["code"].isin(symbols)]
            return info
        return pd.DataFrame()

    # ── K 线数据 ──────────────────────────────────────────────

    def fetch_kline(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        period: str = "day",
    ) -> dict[str, pd.DataFrame]:
        """获取 K 线数据

        Args:
            symbols: 标的代码列表
            start_date: 起始日期 YYYY-MM-DD
            end_date: 结束日期
            period: 'day' / '1min' / '5min'

        Returns:
            { symbol: DataFrame[code, kline_time, open, high, low, close, volume, amount] }
        """
        # 优先从本地 Parquet 读取
        result = self._read_from_parquet(symbols, period)
        remaining = [s for s in symbols if s not in result]

        # 从网络拉取缺失的
        if remaining:
            fetched = self._fetch_from_network(remaining, start_date, end_date, period)
            result.update(fetched)

        return result

    def _read_from_parquet(
        self, symbols: list[str], period: str
    ) -> dict[str, pd.DataFrame]:
        """从本地 Parquet 读取"""
        table_map = {
            "day": "etf_daily",
            "1min": "etf_min1",
            "5min": "etf_min5",
        }
        table = table_map.get(period)
        if not table:
            return {}

        result = {}
        for sym in symbols:
            df = self._store.read_kline(table, sym)
            if not df.empty:
                result[sym] = df
        return result

    def _fetch_from_network(
        self, symbols: list[str], start_date: str, end_date: str, period: str
    ) -> dict[str, pd.DataFrame]:
        """从网络拉取（AmazingData → efinance）"""
        # 尝试 AmazingData
        if self._amazing_available:
            try:
                return self._fetch_amazing_kline(symbols, start_date, end_date, period)
            except Exception as e:
                logger.warning(f"AmazingData kline failed: {e}")

        # 降级到 efinance
        return self._fetch_efinance_kline(symbols, start_date, end_date, period)

    def _fetch_amazing_kline(
        self, symbols: list[str], start_date: str, end_date: str, period: str
    ) -> dict[str, pd.DataFrame]:
        """从 AmazingData 拉取 K 线"""
        ad = self._client.get_ad()
        cal = self._client.get_calendar()

        period_map = {
            "day": ad.constant.Period.day.value,
            "1min": ad.constant.Period.min1.value,
            "5min": ad.constant.Period.min5.value,
        }
        p = period_map.get(period, ad.constant.Period.day.value)

        begin = int(start_date.replace("-", ""))
        end = int(end_date.replace("-", ""))

        md = self._client.get_market_data(cal)
        raw = md.query_kline(symbols, begin_date=begin, end_date=end, period=p)

        result: dict[str, pd.DataFrame] = {}
        if isinstance(raw, dict):
            for code, df in raw.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    result[code] = df
        return result

    def _fetch_efinance_kline(
        self, symbols: list[str], start_date: str, end_date: str, period: str
    ) -> dict[str, pd.DataFrame]:
        """从 efinance 拉取 K 线（备源）"""
        try:
            import efinance as ef
        except ImportError:
            logger.error("efinance not installed, can't fetch fallback kline")
            return {}

        result = {}
        for sym in symbols:
            try:
                code = sym.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
                freq_map = {"day": "kline", "1min": "min1", "5min": "min5"}
                freq = freq_map.get(period, "kline")
                df = ef.stock.get_quote_history(code, beg=start_date, end=end_date, klt=freq)
                if df is not None and not df.empty:
                    # 重命名列以匹配 AmazingData 格式
                    df = df.rename(columns={
                        "股票代码": "code", "日期": "kline_time",
                        "开盘": "open", "收盘": "close",
                        "最高": "high", "最低": "low",
                        "成交量": "volume", "成交额": "amount",
                    })
                    df["code"] = sym
                    if "kline_time" in df.columns:
                        df["kline_time"] = pd.to_datetime(df["kline_time"])
                    df = df[["code", "kline_time", "open", "high", "low", "close", "volume", "amount"]]
                    result[sym] = df
            except Exception as e:
                logger.debug(f"efinance {sym} failed: {e}")
        return result

    # ── ETF 基金数据 ──────────────────────────────────────────

    def fetch_fund_nav(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """获取 ETF 净值"""
        if not self._amazing_available:
            return self._fetch_akshare_fund_nav(symbols)
        try:
            info = self._client.get_info_data()
            raw = info.get_fund_nav(symbols)
            return self._parse_info_dict(raw)
        except Exception as e:
            logger.warning(f"AmazingData fund_nav failed: {e}")
            return self._fetch_akshare_fund_nav(symbols)

    def _fetch_akshare_fund_nav(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """备源：从 akshare 获取净值"""
        try:
            import akshare as ak
        except ImportError:
            return {}
        result = {}
        for sym in symbols:
            try:
                code = sym.replace(".SH", "").replace(".SZ", "")
                df = ak.fund_etf_fund_info_em(code)
                if df is not None and not df.empty:
                    result[sym] = df
            except Exception:
                pass
        return result

    # ── 工具方法 ──────────────────────────────────────────────

    @staticmethod
    def _parse_info_dict(raw: Any) -> dict[str, pd.DataFrame]:
        """解析 InfoData 返回的 dict"""
        if isinstance(raw, dict):
            result = {}
            for k, v in raw.items():
                if isinstance(v, pd.DataFrame) and not v.empty:
                    result[k] = v
            return result
        return {}
