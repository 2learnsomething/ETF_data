"""
Baostock 数据源适配器

免费、稳定、无需注册。底层基于 Python SDK (baostock)。
支持: ETF 日线、指数日线、复权因子、A 股日线/分钟线

注意: Baostock 的 ETF 数据可能不如东方财富齐全，
      主要用作 Tushare/AKShare 故障时的兜底盘。
"""
from __future__ import annotations

import logging
import time

import pandas as pd

from .base import BaseFetcher, FetchRequest, FetchResult

logger = logging.getLogger("etf_data.fetcher.baostock")


class BaostockFetcher(BaseFetcher):
    """Baostock 数据源适配器"""

    def __init__(self, delay: float = 0.5):
        self._bs = None
        self._delay = delay
        self._last_call: float = 0.0
        self._lg = None  # bs.login 返回的 login result

    @property
    def name(self) -> str:
        return "baostock"

    def connect(self) -> None:
        if self._bs is None:
            import baostock as bs
            self._bs = bs
            self._lg = self._bs.login()
            if self._lg.error_code != "0":
                raise ConnectionError(f"Baostock login failed: {self._lg.error_msg}")

    def available_data_types(self) -> list[str]:
        return [
            "etf_daily",        # ETF 日线
            "index_daily",      # 指数日线
            "adj_factor",       # 复权因子
            "fund_adj",         # 复权因子（标准化别名）
            "etf_adj",          # 复权因子（标准化别名）
            "etf_adj_factor",   # 复权因子（标准化别名）
            "stock_daily",      # A 股日线
            "trade_cal",        # 交易日历
        ]

    def fetch(self, request: FetchRequest) -> FetchResult:
        if self._bs is None:
            self.connect()

        method_map = {
            "etf_daily":        self._fetch_etf_daily,
            "index_daily":      self._fetch_index_daily,
            "adj_factor":       self._fetch_adj_factor,
            "fund_adj":         self._fetch_adj_factor,   # 标准化别名
            "etf_adj":          self._fetch_adj_factor,   # 标准化别名
            "etf_adj_factor":   self._fetch_adj_factor,   # 标准化别名
            "stock_daily":      self._fetch_stock_daily,
            "trade_cal":        self._fetch_trade_cal,
        }
        fn = method_map.get(request.data_type)
        if fn is None:
            raise ValueError(f"Baostock unsupported data_type: {request.data_type}")

        df = fn(request)
        return FetchResult(
            source=self.name, data_type=request.data_type, df=df,
            metadata={"row_count": len(df), "columns": list(df.columns)},
        )

    # ── ETF 日线 ──────────────────────────────────────────

    def _fetch_etf_daily(self, request: FetchRequest) -> pd.DataFrame:
        """Baostock ETF 日线 (query_history_k_data_plus)"""
        fields = "date,open,high,low,close,volume,amount,adjustflag"
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = self._to_bs_code(sym)
            start = request.start_date or "20100101"
            end = request.end_date or "20500101"

            rs = self._bs.query_history_k_data_plus(
                code, fields,
                start_date=start, end_date=end,
                frequency="d", adjustflag="3",  # 3=不复权
            )
            rows = []
            while rs.next():
                row = rs.get_row_data()
                rows.append({
                    "date": row[0].replace("-", ""),
                    "open": float(row[1]) if row[1] else None,
                    "close": float(row[2]) if row[2] else None,
                    "high": float(row[3]) if row[3] else None,
                    "low": float(row[4]) if row[4] else None,
                    "volume": float(row[5]) if row[5] else 0,
                    "amount": float(row[6]) if row[6] else 0,
                    "symbol": sym,
                })
            rs.close()
            if rows:
                frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── 指数日线 ──────────────────────────────────────────

    def _fetch_index_daily(self, request: FetchRequest) -> pd.DataFrame:
        """指数日线 (query_history_k_data_plus)"""
        fields = "date,open,high,low,close,volume,amount"
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = self._to_bs_code(sym)
            start = request.start_date or "20100101"
            end = request.end_date or "20500101"

            rs = self._bs.query_history_k_data_plus(
                code, fields,
                start_date=start, end_date=end,
                frequency="d", adjustflag="3",
            )
            rows = []
            while rs.next():
                row = rs.get_row_data()
                rows.append({
                    "date": row[0].replace("-", ""),
                    "open": float(row[1]) if row[1] else None,
                    "close": float(row[2]) if row[2] else None,
                    "high": float(row[3]) if row[3] else None,
                    "low": float(row[4]) if row[4] else None,
                    "volume": float(row[5]) if row[5] else 0,
                    "amount": float(row[6]) if row[6] else 0,
                    "symbol": sym,
                })
            rs.close()
            if rows:
                frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── 复权因子 ──────────────────────────────────────────

    def _fetch_adj_factor(self, request: FetchRequest) -> pd.DataFrame:
        """Baostock 复权因子 (query_adjust_factor)"""
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = self._to_bs_code(sym)
            start = request.start_date or "20100101"
            end = request.end_date or "20500101"

            rs = self._bs.query_adjust_factor(code, start_date=start, end_date=end)
            rows = []
            while rs.next():
                row = rs.get_row_data()
                rows.append({
                    "date": row[0].replace("-", ""),
                    "adj_factor": float(row[1]) if row[1] else 1.0,
                    "symbol": sym,
                })
            rs.close()
            if rows:
                frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── 股票日线 ──────────────────────────────────────────

    def _fetch_stock_daily(self, request: FetchRequest) -> pd.DataFrame:
        """A 股日线 — 用作备源"""
        return self._fetch_etf_daily(request)

    # ── 交易日历 ──────────────────────────────────────────

    def _fetch_trade_cal(self, request: FetchRequest) -> pd.DataFrame:
        """交易日历 (query_trade_dates)"""
        start = request.start_date or "20100101"
        end = request.end_date or "20500101"

        rs = self._bs.query_trade_dates(start_date=start, end_date=end)
        rows = []
        while rs.next():
            row = rs.get_row_data()
            rows.append({
                "date": row[1].replace("-", ""),
                "is_open": row[2],  # "1"=开市 "0"=休市
            })
        rs.close()
        return pd.DataFrame(rows)

    # ── 工具 ──────────────────────────────────────────────

    @staticmethod
    def _to_bs_code(symbol: str) -> str:
        """将标准 symbol (510050.SH) 转为 baostock 格式 (sh.510050)"""
        parts = symbol.split(".")
        if len(parts) < 2:
            # 没有交易所后缀时默认沪市
            return f"sh.{parts[0]}"
        code, exchange = parts[0], parts[1].lower()
        if exchange == "sh":
            return f"sh.{code}"
        elif exchange == "sz":
            return f"sz.{code}"
        return f"sh.{code}"

    def _rate_wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()

    def close(self) -> None:
        if self._bs and self._lg:
            self._bs.logout()
        self._bs = None
        self._lg = None
