"""
Tushare 数据源适配器

支持的数据类型:
  - stock_basic: 股票/ETF 基本信息
  - daily: 日线行情
  - adj_factor: 复权因子
  - fund_basic: 基金/ETF 基本信息
  - fund_daily: 基金日线
  - fund_nav: 基金净值
  - index_daily: 指数日线 (不支持批量，逐个拉取)
  - trade_cal: 交易日历
  - money_flow: 资金流向
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
import tushare as ts

from .base import BaseFetcher, FetchRequest, FetchResult


# ── Tushare API 方法 → data_type 映射 ──
_METHOD_MAP: dict[str, str] = {
    "stock_basic":    "stock_basic",
    "daily":          "daily",
    "adj_factor":     "adj_factor",
    # ETF专题
    "etf_basic":      "etf_basic",
    "fund_basic":     "fund_basic",
    "fund_daily":     "fund_daily",
    "fund_adj":       "fund_adj",
    "fund_share":     "fund_share",
    "fund_nav":       "fund_nav",
    "fund_mins":      "fund_mins",
    # 指数
    "index_daily":    "index_daily",
    "index_basic":    "index_basic",
    "index_weight":   "index_weight",
    # 其他
    "trade_cal":      "trade_cal",
    "money_flow":     "money_flow",
    "limit_list":     "limit_list",
    "stk_limit":      "stk_limit",
    "suspend_d":      "suspend_d",
    "namechange":     "namechange",
    # 基金专题
    "fund_portfolio": "fund_portfolio",
}

# ── 不支持批量查询的类型（需逐个 symbol 循环拉取）──
_SINGLE_SYMBOL_TYPES: set[str] = {
    "index_daily", "index_basic", "index_weight",
    "fund_daily", "fund_nav", "fund_adj", "fund_share", "etf_basic",
    "fund_portfolio",
    "adj_factor", "suspend_d", "limit_list", "stk_limit",
    "namechange", "money_flow",
}


class TushareFetcher(BaseFetcher):
    """Tushare 数据源适配器"""

    def __init__(self, token: Optional[str] = None, http_url: Optional[str] = None):
        self._token = token
        self._http_url = http_url
        self._pro: ts.pro_api | None = None
        self._last_call: float = 0.0            # 上次 API 调用时间戳
        self._rate_config: dict = {}             # {delay, single_symbol_delay, batch_delay}

    @property
    def name(self) -> str:
        return "tushare"

    def connect(self) -> None:
        if self._token is None or self._http_url is None:
            from ..utils.config_helper import get_config
            cfg = get_config("tushare_api")
            self._token = self._token or cfg["token"]
            self._http_url = self._http_url or cfg["http_url"]

        # 加载频率限制配置
        if not self._rate_config:
            from ..utils.config_helper import get_config
            api_cfg = get_config("tushare_api")
            self._rate_config = api_cfg.get("rate_limit", {})
            if not self._rate_config:
                self._rate_config = {"delay": 0.5, "single_symbol_delay": 0.3, "batch_delay": 1.0}

        self._pro = ts.pro_api(self._token)
        self._pro._DataApi__http_url = self._http_url

        # Tushare 服务器不稳定，设置 socket 超时 60s
        # 保存原值，close 时恢复
        import socket
        self._orig_socket_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(60)

        # 绕过本地代理，Tushare 服务器直连更快
        import os
        host = self._http_url.split("://")[1].split(":")[0].split("/")[0]
        no_proxy = os.environ.get("NO_PROXY", "")
        if host not in no_proxy:
            os.environ["NO_PROXY"] = f"{no_proxy},{host}" if no_proxy else host
            os.environ["no_proxy"] = os.environ["NO_PROXY"]

    def available_data_types(self) -> list[str]:
        return list(_METHOD_MAP.keys())

    def fetch(self, request: FetchRequest) -> FetchResult:
        if self._pro is None:
            self.connect()

        api_method = _METHOD_MAP.get(request.data_type)
        if api_method is None:
            raise ValueError(
                f"Unsupported data_type '{request.data_type}'. "
                f"Available: {self.available_data_types()}"
            )

        # 对不支持批量的类型，逐个 symbol 循环
        if (request.data_type in _SINGLE_SYMBOL_TYPES
                and len(request.symbols) > 1):
            return self._fetch_by_symbol(api_method, request)
        else:
            return self._fetch_batch(api_method, request)

    # ── 内部方法 ──────────────────────────────────────────

    def _fetch_by_symbol(self, api_method: str, request: FetchRequest) -> FetchResult:
        """逐个 symbol 拉取后拼接"""
        fn = getattr(self._pro, api_method)
        frames = []
        n_symbols = len(request.symbols)
        for i, sym in enumerate(request.symbols):
            kwargs = self._build_kwargs(request, single_symbol=sym)

            # 首次调用不加延迟，后续按间隔
            if i > 0:
                self._rate_limit("single")

            df = fn(**kwargs)
            if df is not None and not df.empty:
                frames.append(df)

        if not frames:
            return FetchResult(
                source=self.name, data_type=request.data_type,
                df=pd.DataFrame(), metadata={"row_count": 0},
            )

        result_df = pd.concat(frames, ignore_index=True)

        if "ts_code" in result_df.columns:
            self._normalize_codes(result_df)

        return FetchResult(
            source=self.name,
            data_type=request.data_type,
            df=result_df,
            metadata={
                "row_count": len(result_df),
                "columns": list(result_df.columns),
                "symbols": request.symbols,
            },
        )

    def _fetch_batch(self, api_method: str, request: FetchRequest) -> FetchResult:
        """标准批量拉取"""
        self._rate_limit("batch")
        fn = getattr(self._pro, api_method)
        kwargs = self._build_kwargs(request)
        df = fn(**kwargs)

        if df is None or df.empty:
            return FetchResult(
                source=self.name, data_type=request.data_type,
                df=pd.DataFrame(), metadata={"row_count": 0},
            )

        if request.fields:
            available = [f for f in request.fields if f in df.columns]
            if available:
                df = df[available]

        if "ts_code" in df.columns:
            self._normalize_codes(df)

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "symbols": request.symbols or [],
        }
        if "trade_date" in df.columns:
            metadata["date_range"] = (df["trade_date"].min(), df["trade_date"].max())

        return FetchResult(
            source=self.name,
            data_type=request.data_type,
            df=df,
            metadata=metadata,
        )

    def _build_kwargs(
        self, request: FetchRequest, single_symbol: Optional[str] = None
    ) -> dict:
        """构建 Tushare API 调用参数"""
        kwargs: dict = {}

        # ts_code
        if single_symbol is not None:
            kwargs["ts_code"] = single_symbol
        elif request.symbols:
            kwargs["ts_code"] = ",".join(request.symbols)

        # 日期范围
        if request.start_date:
            kwargs["start_date"] = request.start_date.replace("-", "")
        if request.end_date:
            kwargs["end_date"] = request.end_date.replace("-", "")

        # 字段
        if request.fields:
            kwargs["fields"] = ",".join(request.fields)

        # 透传额外参数
        kwargs.update(request.extra)
        return kwargs

    def _rate_limit(self, call_type: str = "batch") -> None:
        """
        频率限制：距上次调用不足最小间隔时 sleep。

        call_type: "batch" | "single"
        """
        import time
        delay = self._rate_config.get(
            "single_symbol_delay" if call_type == "single" else "delay",
            0.5,
        )
        elapsed = time.monotonic() - self._last_call
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_call = time.monotonic()

    @staticmethod
    def _normalize_codes(df: pd.DataFrame) -> None:
        """将 ts_code 拆分为 exchange_code 和 etf_code"""
        if "exchange_code" not in df.columns and "etf_code" not in df.columns:
            parts = df["ts_code"].str.split(".", n=1)
            df["exchange_code"] = parts.str[1]
            df["etf_code"] = parts.str[0]

    def close(self) -> None:
        # 恢复 socket 超时
        import socket
        if hasattr(self, '_orig_socket_timeout') and self._orig_socket_timeout is not None:
            socket.setdefaulttimeout(self._orig_socket_timeout)
        self._pro = None

