#!/usr/bin/env python3
"""
ETF 数据桥接服务 — 轻量 HTTP API

用法:
    python etf_data/api/server.py                    # 默认 localhost:8420
    python etf_data/api/server.py --port 8421        # 指定端口
    python etf_data/api/server.py --host 0.0.0.0     # 外网可访问

请求:
    GET /etfs                          → ETF 列表
    GET /daily/510050?days=250         → ETF 日线
    GET /close?days=60                 → 收盘价矩阵
    GET /portfolio/510050              → 持仓明细
    GET /calendar?year=2026            → 交易日历
    GET /quality                       → 数据质量报告
    GET /dashboard                     → 监控看板 HTML
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

# 桥接模块
sys.path.insert(0, str(_project_root.parent / "daily_news"))

from ETF_data_bridge import (
    get_etf_list, get_etf_daily, get_all_close,
    get_etf_portfolio, get_trade_calendar,
)

import pyodbc

# 提前初始化连接
_CONN = None


def _get_conn():
    global _CONN
    if _CONN is None:
        from etf_data.utils.config_helper import init, get_db_conn_str
        init()
        conn_str = get_db_conn_str("tushare")
        _CONN = pyodbc.connect(conn_str)
    return _CONN


def _df_to_json(df):
    """DataFrame → JSON 列表"""
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, default=str).encode())

    def _send_html(self, html, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def _send_md(self, md, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/markdown; charset=utf-8")
        self.end_headers()
        self.wfile.write(md.encode())

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        try:
            if path == "/etfs":
                df = get_etf_list()
                self._send_json(_df_to_json(df))

            elif path.startswith("/daily/"):
                symbol = path.split("/daily/")[1]
                days = int(params.get("days", [250])[0])
                df = get_etf_daily(symbol, days)
                self._send_json(_df_to_json(df))

            elif path == "/close":
                days = int(params.get("days", [60])[0])
                df = get_all_close(days)
                self._send_json(_df_to_json(df))

            elif path.startswith("/portfolio/"):
                symbol = path.split("/portfolio/")[1]
                df = get_etf_portfolio(symbol)
                self._send_json(_df_to_json(df))

            elif path == "/calendar":
                year = params.get("year", [date.today().year])[0]
                cal = get_trade_calendar(int(year))
                self._send_json([str(d) for d in cal])

            elif path == "/health":
                self._send_json({"status": "ok", "time": datetime.now().isoformat()})

            elif path == "/quality":
                import subprocess
                r = subprocess.run(
                    [sys.executable, "etf_data/quality/check.py"],
                    capture_output=True, text=True, timeout=30,
                    cwd=str(_project_root),
                )
                self._send_md(r.stdout, 200)

            elif path == "/dashboard":
                import subprocess
                r = subprocess.run(
                    [sys.executable, "etf_data/quality/dashboard.py", "-o", "/tmp/dash.html"],
                    capture_output=True, text=True, timeout=30,
                    cwd=str(_project_root),
                )
                html = Path("/tmp/dash.html").read_text() if Path("/tmp/dash.html").exists() else "not found"
                self._send_html(html)

            else:
                self._send_json({"error": f"path not found: {path}"}, 404)

        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]} {args[2]}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ETF 数据桥接服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8420)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), Handler)
    print(f"ETF Data API running on http://{args.host}:{args.port}")
    print(f"  GET /etfs              → ETF 列表")
    print(f"  GET /daily/510050      → 日线")
    print(f"  GET /close             → 收盘价矩阵")
    print(f"  GET /portfolio/510050  → 持仓明细")
    print(f"  GET /calendar          → 交易日历")
    print(f"  GET /health            → 健康检查")
    print(f"  GET /quality           → 质量报告")
    print(f"  GET /dashboard         → 监控看板")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.server_close()


if __name__ == "__main__":
    main()
