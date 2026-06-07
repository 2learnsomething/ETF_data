"""
交易日历工具

从 SQL Server trade_calendar 表读取交易日，提供便捷查询接口。
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

import pandas as pd
import pyodbc


class TradingCalendar:
    """A股交易日历"""

    def __init__(self):
        self._trading_days: set[date] | None = None

    def _load(self) -> set[date]:
        """从 SQL Server 加载交易日集合"""
        from etf_data.utils.config_helper import get_db_conn_str
        conn_str = get_db_conn_str("tushare")
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql_query(
            "SELECT [date] FROM [dbo].[trade_calendar] WHERE is_open = 1 ORDER BY [date]",
            conn,
        )
        conn.close()
        days = {d.date() if hasattr(d, 'date') else d for d in pd.to_datetime(df["date"])}
        return days

    @property
    def trading_days(self) -> set[date]:
        if self._trading_days is None:
            self._trading_days = self._load()
        return self._trading_days

    def refresh(self) -> None:
        """强制刷新缓存"""
        self._trading_days = None

    def is_trading_day(self, d: date | None = None) -> bool:
        """判断是否为交易日"""
        d = d or date.today()
        return d in self.trading_days

    def next_trading_day(self, d: date | None = None) -> date:
        """下一个交易日（含当日）"""
        d = d or date.today()
        while d not in self.trading_days:
            d += timedelta(days=1)
        return d

    def last_trading_day(self, d: date | None = None) -> date:
        """上一个交易日"""
        d = d or date.today()
        d -= timedelta(days=1)
        while d not in self.trading_days:
            d -= timedelta(days=1)
        return d


# 单例
_calendar: TradingCalendar | None = None


def get_calendar() -> TradingCalendar:
    global _calendar
    if _calendar is None:
        _calendar = TradingCalendar()
    return _calendar
