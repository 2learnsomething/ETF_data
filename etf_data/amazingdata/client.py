"""AmazingData 客户端封装

提供登录、日历获取功能。
get_calendar() 使用 baostock 替代（AmazingData 官方接口有 Bug）。
"""

from __future__ import annotations

import logging
from typing import Any

import baostock as bs

from .config import Config

logger = logging.getLogger(__name__)


class AmazingDataClient:
    """AmazingData 客户端单例

    封装登录、会话管理、交易日历获取。
    get_calendar() 使用 baostock 替代（AmazingData get_calendar() 返回 None）。
    """

    _instance: AmazingDataClient | None = None

    def __new__(cls) -> AmazingDataClient:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._cfg = Config()
        self._amazingdata_available = False
        self._calendar: list[int] | None = None

    # ── 连接管理 ──────────────────────────────────────────────

    def login(self) -> bool:
        """登录 AmazingData 和 baostock

        Returns:
            True 表示 AmazingData 登录成功（可能有权限限制，但连接建立）
        """
        import AmazingData as ad

        try:
            ad.login(
                username=self._cfg.amazingdata_username,
                password=self._cfg.amazingdata_password,
                host=self._cfg.amazingdata_host,
                port=self._cfg.amazingdata_port,
            )
            self._amazingdata_available = True
            logger.info("AmazingData login OK")
        except Exception as e:
            self._amazingdata_available = False
            logger.warning(f"AmazingData login failed: {e}")

        # baostock 始终登录（日历替代方案）
        try:
            lg = bs.login()
            if lg.error_code != "0":
                logger.warning(f"baostock login failed: {lg.error_msg}")
            else:
                logger.info("baostock login OK")
        except Exception as e:
            logger.warning(f"baostock login failed: {e}")

        return self._amazingdata_available

    @property
    def is_available(self) -> bool:
        return self._amazingdata_available

    # ── 交易日历（baostock 替代方案） ─────────────────────────

    def get_calendar(self, start_year: int = 2013, end_year: int = 2026) -> list[int]:
        """获取交易日历

        因为 AmazingData 的 get_calendar() 返回 None（SDK Bug），
        改用 baostock 获取。

        Returns:
            交易日列表，如 [20130104, 20130107, ...]
        """
        if self._calendar is not None:
            return self._calendar

        try:
            rs = bs.query_trade_dates(
                start_date=f"{start_year}-01-01",
                end_date=f"{end_year}-12-31",
            )
            cal: list[int] = []
            while rs.next():
                row = rs.get_row_data()
                if row[1] == "1":  # is_open == 1
                    cal.append(int(row[0].replace("-", "")))
            self._calendar = sorted(cal)
            logger.info(f"Calendar loaded: {len(self._calendar)} trade days")
        except Exception as e:
            logger.error(f"Failed to load calendar from baostock: {e}")
            self._calendar = []

        return self._calendar

    # ── 底层访问 ──────────────────────────────────────────────

    def get_ad(self) -> Any:
        """获取 AmazingData 模块引用（需先 login）"""
        import AmazingData as ad

        return ad

    def get_base_data(self) -> Any:
        """获取 BaseData 实例"""
        return self.get_ad().BaseData()

    def get_market_data(self, calendar: list[int] | None = None) -> Any:
        """获取 MarketData 实例（需要日历参数）"""
        ad = self.get_ad()
        cal = calendar or self.get_calendar()
        return ad.MarketData(calendar=cal)

    def get_info_data(self) -> Any:
        """获取 InfoData 实例"""
        return self.get_ad().InfoData()
