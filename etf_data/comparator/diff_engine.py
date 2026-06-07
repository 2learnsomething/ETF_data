"""
多源数据差异检测引擎

支持三方比对: Tushare vs 新浪 vs 腾讯
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger("etf_data.comparator")

DEFAULT_TOLERANCE = {
    "open": 0.005, "high": 0.005, "low": 0.005, "close": 0.005,
    "volume": 0.02, "amount": 0.02,
}
COMPARE_FIELDS = ["open", "high", "low", "close", "volume", "amount"]


class DiffEngine:
    """多源数据差异检测"""

    def __init__(self, tolerance: dict | None = None):
        self.tolerance = tolerance or DEFAULT_TOLERANCE.copy()

    def compare_etf(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """三方比对单只 ETF"""
        from etf_data.fetchers.tushare_fetcher import TushareFetcher
        from etf_data.fetchers.akshare_fetcher import AKShareFetcher
        from etf_data.fetchers.tencent_fetcher import TencentFetcher
        from etf_data.fetchers.base import FetchRequest

        ts, ak, tc = TushareFetcher(), AKShareFetcher(), TencentFetcher()
        if ts._pro is None:
            ts.connect()

        def _fetch(src, dtype):
            r = src.fetch(FetchRequest(
                data_type=dtype, symbols=[symbol],
                start_date=start_date, end_date=end_date,
            ))
            df = r.df
            if df.empty:
                logger.warning(f"{src.name} empty for {symbol}")
            return df

        # 拉取三源
        ts_df = _fetch(ts, "fund_daily")
        ak_df = _fetch(ak, "etf_sina_daily")
        tc_df = _fetch(tc, "etf_daily")

        if ts_df.empty:
            return pd.DataFrame()

        # 标准化 Tushare 列名
        ts_df = ts.normalize_columns(ts_df, "fund_daily")

        # 三方两两比对
        pairs = [
            ("Tushare", "新浪", ts_df, ak_df),
            ("Tushare", "腾讯", ts_df, tc_df),
            ("新浪", "腾讯", ak_df, tc_df),
        ]
        all_diffs = []
        for src1, src2, df1, df2 in pairs:
            if df1.empty or df2.empty:
                continue
            diffs = self._diff_pair(df1, df2, symbol, src1, src2)
            if not diffs.empty:
                all_diffs.append(diffs)

        return pd.concat(all_diffs, ignore_index=True) if all_diffs else pd.DataFrame()

    # ── 批量比对 ──────────────────────────────────────────

    def compare_batch(self, symbols: list[str], start_date: str, end_date: str) -> dict:
        all_diffs, per_etf = [], []
        for i, sym in enumerate(symbols):
            logger.info(f"[{i+1}/{len(symbols)}] {sym}")
            try:
                diffs = self.compare_etf(sym, start_date, end_date)
                if not diffs.empty:
                    all_diffs.append(diffs)
                per_etf.append({
                    "symbol": sym,
                    "n_diffs": len(diffs),
                    "consistency": round(1 - len(diffs) / max(
                        diffs["date"].nunique() * len(COMPARE_FIELDS) * 3, 1), 4),
                    "n_dates": diffs["date"].nunique(),
                } if not diffs.empty else {
                    "symbol": sym, "n_diffs": 0, "consistency": 1.0, "n_dates": 0,
                })
            except Exception as e:
                logger.error(f"  {sym}: {e}")
                per_etf.append({"symbol": sym, "n_diffs": 0, "consistency": 0.0, "error": str(e)})

        details = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
        summary = pd.DataFrame(per_etf) if per_etf else pd.DataFrame()
        return {"details": details, "summary": summary, "overall": {
            "n_etfs": len(symbols),
            "avg_consistency": round(summary["consistency"].mean(), 4) if not summary.empty else 0,
            "total_diffs": len(details),
            "tolerance": DEFAULT_TOLERANCE,
        }}

    # ── 内部方法 ──────────────────────────────────────────

    def _diff_pair(self, df1: pd.DataFrame, df2: pd.DataFrame,
                   symbol: str, src1: str, src2: str) -> pd.DataFrame:
        df1, df2 = df1.copy(), df2.copy()
        for df in [df1, df2]:
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df.set_index("date", inplace=True)

        common = df1.index.intersection(df2.index)
        if len(common) == 0:
            return pd.DataFrame()

        diffs = []
        for dt in common:
            for f in COMPARE_FIELDS:
                if f not in df1.columns or f not in df2.columns:
                    continue
                v1, v2 = df1.loc[dt, f], df2.loc[dt, f]
                if pd.isna(v1) or pd.isna(v2) or (v1 == 0 and v2 == 0):
                    continue
                denom = max(abs(v1), abs(v2))
                if denom == 0:
                    continue
                rel = abs(v1 - v2) / denom
                th = self.tolerance.get(f, 0.01)
                if rel > th:
                    diffs.append({
                        "symbol": symbol, "date": dt, "field": f,
                        "src1": src1, "src2": src2,
                        "val1": round(v1, 6), "val2": round(v2, 6),
                        "abs_diff": round(abs(v1 - v2), 6),
                        "rel_diff": round(rel, 6), "threshold": th,
                    })
        return pd.DataFrame(diffs)
