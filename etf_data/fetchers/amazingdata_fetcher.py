"""
AmazingData 数据源适配器 — pipeline 接入层

将 amazingdata.UnifiedFetcher 封装为 BaseFetcher 接口，
使其可作为 pipeline 的 source 使用。

支持的数据类型映射到 amazingdata 能力集：
  etf_daily      → fetch_kline(period='day')
  etf_min1       → fetch_kline(period='1min')
  etf_min5       → fetch_kline(period='5min')
  etf_nav        → fetch_fund_nav()
  etf_basic      → fetch_code_list() + fetch_code_info()
  trade_cal      → AmazingDataClient.get_calendar()

AmazingData 不支持的数据类型（如 etf_share、etf_adj_factor），
在 pipeline 中通过 fallback_sources 降级到 Tushare/Baostock 等。
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from ..fetchers.base import BaseFetcher, FetchRequest, FetchResult
from etf_data.amazingdata.client import AmazingDataClient
from etf_data.amazingdata.config import Config
from etf_data.amazingdata.fetcher import UnifiedFetcher

logger = logging.getLogger("etf_data.fetcher.amazingdata")


class AmazingDataFetcher(BaseFetcher):
    """AmazingData 数据源适配器"""

    def __init__(self, **kwargs):
        self._cfg = Config()
        self._client = AmazingDataClient()
        self._fetcher = UnifiedFetcher()
        self._connected = False

    @property
    def name(self) -> str:
        return "amazingdata"

    def connect(self) -> None:
        if self._connected:
            return
        ok = self._client.login()
        self._connected = ok
        if not ok:
            logger.warning("AmazingData login failed, will fall through to backup sources")

    def available_data_types(self) -> list[str]:
        return [
            "etf_daily", "etf_min1", "etf_min5",
            "etf_nav", "etf_basic",
            "trade_cal",
        ]

    def fetch(self, request: FetchRequest) -> FetchResult:
        if not self._connected:
            self.connect()

        if not self._connected or not self._client.is_available:
            return FetchResult(
                source=self.name, data_type=request.data_type,
                df=pd.DataFrame(),
                metadata={"row_count": 0, "error": "AmazingData not available"},
            )

        df = self._dispatch(request)
        return FetchResult(
            source=self.name, data_type=request.data_type, df=df,
            metadata={"row_count": len(df), "columns": list(df.columns)},
        )

    def _dispatch(self, request: FetchRequest) -> pd.DataFrame:
        """按 data_type 路由到对应方法"""
        dispatch_map = {
            "etf_daily":    self._fetch_kline,
            "etf_min1":     self._fetch_kline_min1,
            "etf_min5":     self._fetch_kline_min5,
            "etf_nav":      self._fetch_nav,
            "etf_basic":    self._fetch_basic,
            "trade_cal":    self._fetch_calendar,
        }
        fn = dispatch_map.get(request.data_type)
        if fn is None:
            logger.warning(f"AmazingData unsupported data_type: {request.data_type}")
            return pd.DataFrame()
        try:
            return fn(request)
        except Exception as e:
            logger.warning(f"AmazingData {request.data_type} failed: {e}")
            return pd.DataFrame()

    # ── K 线 ──────────────────────────────────────────────

    def _fetch_kline(self, request: FetchRequest) -> pd.DataFrame:
        """ETF 日线 — 委托 UnifiedFetcher"""
        result = self._fetcher.fetch_kline(
            symbols=request.symbols,
            start_date=request.start_date or "20150101",
            end_date=request.end_date or "20500101",
            period="day",
        )
        return self._kline_dict_to_df(result)

    def _fetch_kline_min1(self, request: FetchRequest) -> pd.DataFrame:
        result = self._fetcher.fetch_kline(
            symbols=request.symbols,
            start_date=request.start_date or "20150101",
            end_date=request.end_date or "20500101",
            period="1min",
        )
        return self._kline_dict_to_df(result)

    def _fetch_kline_min5(self, request: FetchRequest) -> pd.DataFrame:
        result = self._fetcher.fetch_kline(
            symbols=request.symbols,
            start_date=request.start_date or "20150101",
            end_date=request.end_date or "20500101",
            period="5min",
        )
        return self._kline_dict_to_df(result)

    @staticmethod
    def _kline_dict_to_df(result: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """将 {symbol: DataFrame} 合并为单表"""
        frames = []
        for symbol, df in result.items():
            if df is not None and not df.empty:
                # 统一列名：amazingdata 用 'code', pipeline 期望 'symbol'
                if "code" in df.columns:
                    df = df.rename(columns={"code": "symbol"})
                # kline_time → date
                if "kline_time" in df.columns:
                    df = df.rename(columns={"kline_time": "date"})
                # 确保 date 列为 YYYYMMDD 格式
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
                if "symbol" not in df.columns:
                    df["symbol"] = symbol
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── 净值 ──────────────────────────────────────────────

    def _fetch_nav(self, request: FetchRequest) -> pd.DataFrame:
        """ETF 净值"""
        result = self._fetcher.fetch_fund_nav(symbols=request.symbols)
        frames = []
        for symbol, df in result.items():
            if df is not None and not df.empty:
                df = df.rename(columns={"code": "symbol"} if "code" in df.columns else {})
                if "symbol" not in df.columns:
                    df["symbol"] = symbol
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── 基本信息 ──────────────────────────────────────────

    def _fetch_basic(self, request: FetchRequest) -> pd.DataFrame:
        """ETF 基本信息"""
        codes = self._fetcher.fetch_code_list(security_type="EXTRA_ETF")
        if not codes:
            return pd.DataFrame()
        df = self._fetcher.fetch_code_info(symbols=codes)
        if not df.empty:
            df = df.rename(columns={"code": "symbol"} if "code" in df.columns else {})
        return df

    # ── 交易日历 ──────────────────────────────────────────

    def _fetch_calendar(self, request: FetchRequest) -> pd.DataFrame:
        """交易日历 — 从 AmazingDataClient 获取"""
        cal = self._client.get_calendar()
        if not cal:
            return pd.DataFrame()
        return pd.DataFrame({
            "date": [str(d) for d in cal],
            "is_open": ["1"] * len(cal),
        })

    def close(self) -> None:
        pass
