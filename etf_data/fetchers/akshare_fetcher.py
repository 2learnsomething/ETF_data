"""
AKShare 数据源适配器（东方财富，免费稳定）

使用 curl 子进程绕过 Python 3.14 SSL 兼容问题。
支持: ETF 日线、指数日线、ETF 实时行情
"""
from __future__ import annotations

import json
import logging
import subprocess
import time

import pandas as pd

from .base import BaseFetcher, FetchRequest, FetchResult

logger = logging.getLogger("etf_data.fetcher.akshare")


class AKShareFetcher(BaseFetcher):
    """AKShare 数据源适配器 — curl 直连东方财富"""

    def __init__(self, delay: float = 0.3):
        self._delay = delay
        self._last_call: float = 0.0

    @property
    def name(self) -> str:
        return "akshare"

    def connect(self) -> None:
        pass

    def available_data_types(self) -> list[str]:
        return ["etf_daily", "etf_sina_daily", "etf_spot", "index_daily"]

    def fetch(self, request: FetchRequest) -> FetchResult:
        if request.data_type == "etf_daily":
            df = self._fetch_etf_daily(request)
        elif request.data_type == "etf_sina_daily":
            df = self._fetch_sina_daily(request)
        elif request.data_type == "etf_spot":
            df = self._fetch_etf_spot()
        elif request.data_type == "index_daily":
            df = self._fetch_index_daily(request)
        else:
            df = pd.DataFrame()

        return FetchResult(
            source=self.name, data_type=request.data_type, df=df,
            metadata={"row_count": len(df), "columns": list(df.columns)},
        )

    # ── ETF 日线 ──────────────────────────────────────────

    def _fetch_etf_daily(self, request: FetchRequest) -> pd.DataFrame:
        """
        东方财富 K 线接口: push2his.eastmoney.com
        secid: 1.510050 (上交所), 0.159919 (深交所)
        """
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = sym.split(".")[0]
            market = "1" if sym.endswith(".SH") else "0"
            secid = f"{market}.{code}"
            start = request.start_date or "20150101"
            end = request.end_date or "20500101"

            url = (
                "https://push2his.eastmoney.com/api/qt/stock/kline/get"
                f"?secid={secid}&klt=101&fqt=1"
                f"&beg={start}&end={end}"
                "&fields1=f1,f2,f3,f4,f5,f6"
                "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                "&ut=7eea3edcaed734bea9cbfc24409ed989"
            )
            data = self._curl_get(url)
            if data:
                rows = self._parse_kline(data.get("data", {}).get("klines", []), sym)
                if rows:
                    frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── ETF 日线（新浪） ──────────────────────────────────

    def _fetch_sina_daily(self, request: FetchRequest) -> pd.DataFrame:
        """
        新浪财经 K 线接口: money.finance.sina.com.cn
        symbol 格式: sh510050, sz159915
        """
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = sym.split(".")[0]
            exchange = "sh" if sym.endswith(".SH") else "sz"
            sina_sym = f"{exchange}{code}"

            # 新浪日线 API (scale=240 日线, datalen 设大一点)
            datalen = 5000  # 全量
            url = (
                "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                "CN_MarketData.getKLineData"
                f"?symbol={sina_sym}&scale=240&ma=no&datalen={datalen}"
            )
            data = self._curl_get(url)
            if data and isinstance(data, list):
                rows = []
                for item in data:
                    try:
                        rows.append({
                            "date": item.get("day", "").replace("-", ""),
                            "open": float(item.get("open", 0)),
                            "high": float(item.get("high", 0)),
                            "low": float(item.get("low", 0)),
                            "close": float(item.get("close", 0)),
                            "volume": float(item.get("volume", 0)),
                            "symbol": sym,
                        })
                    except (ValueError, TypeError):
                        continue
                if rows:
                    frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── ETF 实时行情 ──────────────────────────────────────

    def _fetch_etf_spot(self) -> pd.DataFrame:
        """ETF 实时行情列表"""
        self._rate_wait()
        url = (
            "http://80.push2.eastmoney.com/api/qt/clist/get"
            "?pn=1&pz=5000&po=1&np=1&fltt=2&invt=2"
            "&fid=f3&fs=b:MK0021,b:MK0022,b:MK0023,b:MK0024"
            "&fields=f2,f3,f4,f12,f13,f14,f15,f16,f17,f18"
        )
        data = self._curl_get(url)
        if not data:
            return pd.DataFrame()

        diffs = data.get("data", {}).get("diff", [])
        rows = []
        for item in diffs:
            rows.append({
                "symbol": f"{item.get('f12','')}.{item.get('f13','SH')}",
                "name": item.get("f14", ""),
                "close": item.get("f2"),
                "pct_chg": item.get("f3"),
                "volume": item.get("f5"),
                "amount": item.get("f6"),
            })
        return pd.DataFrame(rows)

    # ── 指数日线 ──────────────────────────────────────────

    def _fetch_index_daily(self, request: FetchRequest) -> pd.DataFrame:
        """指数日线"""
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = sym.split(".")[0]
            market = "1" if sym.endswith(".SH") else "0"
            secid = f"{market}.{code}"
            start = request.start_date or "20150101"
            end = request.end_date or "20500101"

            url = (
                "https://push2his.eastmoney.com/api/qt/stock/kline/get"
                f"?secid={secid}&klt=101&fqt=1"
                f"&beg={start}&end={end}"
                "&fields1=f1,f2,f3,f4,f5,f6"
                "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                "&ut=7eea3edcaed734bea9cbfc24409ed989"
            )
            data = self._curl_get(url)
            if data:
                rows = self._parse_kline(data.get("data", {}).get("klines", []), sym)
                if rows:
                    frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── curl 子进程 ───────────────────────────────────────

    @staticmethod
    def _curl_get(url: str, timeout: int = 30) -> dict | None:
        """用 curl 发 GET 请求，返回解析后的 JSON"""
        try:
            result = subprocess.run(
                ["curl", "-s", "--noproxy", "*", "--connect-timeout", str(timeout),
                 "--max-time", str(timeout), url],
                capture_output=True, text=True, timeout=timeout + 5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout)
            if result.stderr:
                logger.warning(f"  [curl] {url[:80]}: {result.stderr[:100]}")
        except Exception as e:
            logger.warning(f"  [curl] {url[:80]}: {e}")
        return None

    @staticmethod
    def _parse_kline(klines: list[str], symbol: str) -> list[dict]:
        """
        解析 K 线字符串: "日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率"
        """
        rows = []
        for line in (klines or []):
            parts = line.split(",")
            if len(parts) < 7:
                continue
            try:
                rows.append({
                    "date": parts[0].replace("-", ""),
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]),
                    "pct_chg": float(parts[8]) if len(parts) > 8 else None,
                    "change": float(parts[9]) if len(parts) > 9 else None,
                    "turnover_rate": float(parts[10]) if len(parts) > 10 else None,
                    "symbol": symbol,
                })
            except (ValueError, IndexError):
                continue
        return rows

    # ── 频率限制 ──────────────────────────────────────────

    def _rate_wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()

    def close(self) -> None:
        pass
