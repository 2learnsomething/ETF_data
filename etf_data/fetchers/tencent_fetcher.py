"""
腾讯财经数据源适配器

使用 HTTP API (web.ifzq.gtimg.cn)，免费无需认证。
支持: ETF 日线、分时数据
"""
from __future__ import annotations

import json
import subprocess
import time

import pandas as pd

from .base import BaseFetcher, FetchRequest, FetchResult


class TencentFetcher(BaseFetcher):
    """腾讯财经数据源适配器 — curl 直连"""

    def __init__(self, delay: float = 0.2):
        self._delay = delay
        self._last_call: float = 0.0

    @property
    def name(self) -> str:
        return "tencent"

    def connect(self) -> None:
        pass

    def available_data_types(self) -> list[str]:
        return ["etf_daily", "etf_minute"]

    def fetch(self, request: FetchRequest) -> FetchResult:
        if request.data_type == "etf_daily":
            df = self._fetch_etf_daily(request)
        elif request.data_type == "etf_minute":
            df = self._fetch_etf_minute(request)
        else:
            df = pd.DataFrame()

        return FetchResult(
            source=self.name, data_type=request.data_type, df=df,
            metadata={"row_count": len(df), "columns": list(df.columns)},
        )

    # ── ETF 日线 ──────────────────────────────────────────

    def _fetch_etf_daily(self, request: FetchRequest) -> pd.DataFrame:
        """
        腾讯财经前复权日K线

        URL: web.ifzq.gtimg.cn/appstock/app/fqkline/get
        param: sh510050,day,,,500,qfq
        """
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = sym.split(".")[0]
            exchange = "sh" if sym.endswith(".SH") else "sz"
            tencent_sym = f"{exchange}{code}"

            # datalen 上限约 2000（超出返回空列表）
            url = (
                "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                f"?param={tencent_sym},day,,,2000,qfq"
            )
            data = self._curl_get(url)
            if not data:
                continue

            symbol_data = data.get("data", {})
            # 小 datalen 返回 dict，大 datalen 返回空列表
            if isinstance(symbol_data, list):
                continue
            klines = (symbol_data.get(tencent_sym, {})
                      .get("qfqday", []))

            rows = []
            for item in klines:
                try:
                    rows.append({
                        "date": item[0],
                        "open": float(item[1]),
                        "close": float(item[2]),
                        "high": float(item[3]),
                        "low": float(item[4]),
                        "volume": float(item[5]),
                        "symbol": sym,
                    })
                except (ValueError, IndexError, TypeError):
                    continue

            if rows:
                frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── ETF 分时数据 ──────────────────────────────────────

    def _fetch_etf_minute(self, request: FetchRequest) -> pd.DataFrame:
        """
        腾讯财经分时数据（当日）

        URL: web.ifzq.gtimg.cn/appstock/app/minute/query
        code: sh510050
        """
        frames = []
        for sym in request.symbols:
            self._rate_wait()
            code = sym.split(".")[0]
            exchange = "sh" if sym.endswith(".SH") else "sz"
            tencent_sym = f"{exchange}{code}"

            url = (
                "https://web.ifzq.gtimg.cn/appstock/app/minute/query"
                f"?_var=min_data&code={tencent_sym}"
            )
            raw = self._curl_get_raw(url)
            if not raw:
                continue

            # Response: min_data={...}
            try:
                json_str = raw.split("=", 1)[1] if "=" in raw else raw
                data = json.loads(json_str)
                minute_data = (data.get("data", {})
                               .get(tencent_sym, {})
                               .get("data", {})
                               .get("data", []))
            except (json.JSONDecodeError, KeyError):
                continue

            rows = []
            for item in minute_data:
                # Format: "0930 2.970 3853 1144341.00"
                parts = item.split()
                if len(parts) >= 3:
                    try:
                        rows.append({
                            "time": parts[0],
                            "price": float(parts[1]),
                            "volume": float(parts[2]) if len(parts) > 2 else 0,
                            "amount": float(parts[3]) if len(parts) > 3 else 0,
                            "symbol": sym,
                        })
                    except (ValueError, IndexError):
                        continue

            if rows:
                frames.append(pd.DataFrame(rows))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ── curl 子进程 ───────────────────────────────────────

    @staticmethod
    def _curl_get(url: str, timeout: int = 15) -> dict | None:
        """用 curl 发 GET 请求，返回解析后的 JSON dict"""
        try:
            result = subprocess.run(
                ["curl", "-s", "--noproxy", "*",
                 "--connect-timeout", str(timeout),
                 "--max-time", str(timeout), url],
                capture_output=True, text=True, timeout=timeout + 5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout)
        except (json.JSONDecodeError, Exception):
            pass
        return None

    @staticmethod
    def _curl_get_raw(url: str, timeout: int = 15) -> str | None:
        """用 curl 发 GET 请求，返回原始文本"""
        try:
            result = subprocess.run(
                ["curl", "-s", "--noproxy", "*",
                 "--connect-timeout", str(timeout),
                 "--max-time", str(timeout), url],
                capture_output=True, text=True, timeout=timeout + 5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout
        except Exception:
            pass
        return None

    # ── 频率限制 ──────────────────────────────────────────

    def _rate_wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()

    def close(self) -> None:
        pass
