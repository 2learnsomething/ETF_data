"""
AKShare 扩展数据源适配器

使用 akshare 库封装新浪/同花顺/交易所等免费数据。
支持: ETF实时行情、ETF规模、ETF分类、ETF分红、ETF历史(新浪)
"""
from __future__ import annotations

import logging

import pandas as pd

from .base import BaseFetcher, FetchRequest, FetchResult

logger = logging.getLogger("etf_data.fetcher.akshare_extra")

# 数据源前缀 → akshare 函数映射
_FUNC_MAP: dict[str, str] = {
    "etf_spot_ths":       "fund_etf_spot_ths",        # 同花顺 ETF 实时行情
    "etf_scale_sse":      "fund_etf_scale_sse",       # 上证所 ETF 规模
    "etf_scale_szse":     "fund_etf_scale_szse",      # 深交所 ETF 规模
    "etf_category_sina":  "fund_etf_category_sina",   # 新浪 ETF 分类
    "etf_dividend_sina":  "fund_etf_dividend_sina",   # 新浪 ETF 分红
    "etf_hist_sina":      "fund_etf_hist_sina",       # 新浪 ETF 历史
    "etf_category_ths":   "fund_etf_category_ths",    # 同花顺 ETF 分类
}


class AKShareExtraFetcher(BaseFetcher):
    """AKShare 扩展数据源"""

    def __init__(self):
        self._ak = None

    @property
    def name(self) -> str:
        return "akshare_extra"

    def connect(self) -> None:
        if self._ak is None:
            import akshare as ak
            self._ak = ak

    def available_data_types(self) -> list[str]:
        return list(_FUNC_MAP.keys())

    def fetch(self, request: FetchRequest) -> FetchResult:
        if self._ak is None:
            self.connect()

        func_name = _FUNC_MAP.get(request.data_type)
        if func_name is None:
            raise ValueError(
                f"Unsupported data_type '{request.data_type}'. "
                f"Available: {self.available_data_types()}"
            )

        try:
            fn = getattr(self._ak, func_name)

            # 根据函数签名调用
            if request.data_type == "etf_hist_sina" and request.symbols:
                # 逐个 symbol 拉取（新浪 K 线）
                frames = []
                for sym in request.symbols:
                    code = sym.split(".")[0]
                    exchange = "sh" if sym.endswith(".SH") else "sz"
                    sina_sym = f"{exchange}{code}"
                    try:
                        df = fn(symbol=sina_sym)
                        if df is not None and not df.empty:
                            df["symbol"] = sym
                            frames.append(df)
                    except Exception as e:
                        logger.warning(f"  {sym}: {e}")
                if frames:
                    result_df = pd.concat(frames, ignore_index=True)
                else:
                    result_df = pd.DataFrame()
            else:
                result_df = fn()

            return FetchResult(
                source=self.name,
                data_type=request.data_type,
                df=result_df if result_df is not None else pd.DataFrame(),
                metadata={
                    "row_count": len(result_df) if result_df is not None else 0,
                    "columns": list(result_df.columns) if result_df is not None and not result_df.empty else [],
                },
            )
        except Exception as e:
            logger.error(f"AKShareExtra {request.data_type}: {e}")
            return FetchResult(
                source=self.name, data_type=request.data_type,
                df=pd.DataFrame(), metadata={"error": str(e)},
            )

    def close(self) -> None:
        self._ak = None
