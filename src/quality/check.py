#!/usr/bin/env python3
"""
ETF 数据质量监控

检查各表数据完整性、新鲜度、异常值，生成报告。
支持定时运行（Hermes cron）和 CLI。

用法:
    python src/quality/check.py                          # 全部检查
    python src/quality/check.py --output reports/quality_20260602.md
    python src/quality/check.py --notify                  # 推送告警
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

logger = None


def setup_logging():
    import logging
    global logger
    logger = logging.getLogger("etf_data.quality")


# ── 表定义 ────────────────────────────────────────────────

TABLES = [
    {
        "name": "trade_calendar",
        "expected_min_rows": 4380,
        "freshness_days": 7,
        "desc": "交易日历",
    },
    {
        "name": "etf_basic",
        "expected_min_rows": 2500,
        "freshness_days": 7,
        "desc": "ETF 基本信息",
    },
    {
        "name": "etf_daily",
        "expected_min_rows": 30,
        "freshness_days": 3,
        "desc": "ETF 日线",
    },
    {
        "name": "etf_adj_factor",
        "expected_min_rows": 20000,
        "freshness_days": 7,
        "desc": "ETF 复权因子",
    },
    {
        "name": "etf_share",
        "expected_min_rows": 500,
        "freshness_days": 7,
        "desc": "ETF 份额",
    },
    {
        "name": "etf_nav",
        "expected_min_rows": 500,
        "freshness_days": 7,
        "desc": "ETF 净值",
    },
    {
        "name": "index_daily",
        "expected_min_rows": 30,
        "freshness_days": 3,
        "desc": "指数日线",
    },
    {
        "name": "fund_portfolio",
        "expected_min_rows": 1000,
        "freshness_days": 120,
        "desc": "ETF 持仓明细",
    },
    {
        "name": "stk_limit",
        "expected_min_rows": 7000,
        "freshness_days": 3,
        "desc": "涨跌停",
    },
    {
        "name": "etf_spot_ths",
        "expected_min_rows": 1500,
        "freshness_days": 7,
        "desc": "同花顺净值",
    },
    {
        "name": "etf_scale_sse",
        "expected_min_rows": 500,
        "freshness_days": 14,
        "desc": "上证所规模",
    },
    {
        "name": "etf_minute",
        "expected_min_rows": 1000,
        "freshness_days": 1,
        "desc": "分钟数据",
    },
]


def check_table(
    conn, table_name: str, expected_min: int, freshness: int
) -> dict:
    """检查单张表"""
    result = {
        "table": table_name,
        "rows": 0,
        "max_date": None,
        "min_date": None,
        "issues": [],
        "status": "ok",
    }
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table_name}]")
        result["rows"] = cursor.fetchone()[0]

        # 找日期列
        cursor.execute(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='{table_name}'"
        )
        date_cols = [
            r[0] for r in cursor.fetchall()
            if "date" in r[0].lower()
        ]

        if date_cols and result["rows"] > 0:
            dc = date_cols[0]
            cursor.execute(
                f"SELECT MIN([{dc}]), MAX([{dc}]) FROM [dbo].[{table_name}]"
            )
            row = cursor.fetchone()
            result["min_date"] = str(row[0])[:10] if row[0] else None
            result["max_date"] = str(row[1])[:10] if row[1] else None

            # 新鲜度检查
            if result["max_date"]:
                try:
                    last = datetime.strptime(result["max_date"][:10], "%Y-%m-%d").date()
                except ValueError:
                    last = datetime.strptime(result["max_date"][:8], "%Y%m%d").date()
                stale_days = (date.today() - last).days
                if stale_days > freshness:
                    result["issues"].append(
                        f"STALE: 最后数据 {stale_days} 天前 ({result['max_date']})，超过 {freshness} 天阈值"
                    )
                    result["status"] = "warn"

        # 行数检查
        if result["rows"] < expected_min:
            result["issues"].append(
                f"LOW_ROWS: {result['rows']} < {expected_min}"
            )
            result["status"] = result["status"] if result["status"] != "ok" else "warn"

        # 重复检查 — 只在有明确 date 列的表上运行
        if any(c.lower() == "date" for c in date_cols):
            try:
                dcs = [c for c in date_cols if c.lower() == "date"][:2]
                cursor.execute(f"SELECT COUNT(*) - COUNT(DISTINCT CONCAT({','.join(f'[{c}]' for c in [dcs[0]] + ['symbol'])})) FROM [dbo].[{table_name}]")
                dupes = cursor.fetchone()[0]
                if dupes > 0:
                    result["issues"].append(f"DUPLICATES: {dupes} 行重复")
                    result["status"] = "warn"
            except Exception:
                pass

    except Exception as e:
        result["issues"].append(f"ERROR: {e}")
        result["status"] = "error"

    return result


def generate_report(results: list[dict], output_path: str | None = None) -> str:
    """生成 Markdown 质量报告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok = sum(1 for r in results if r["status"] == "ok")
    warn = sum(1 for r in results if r["status"] == "warn")
    err = sum(1 for r in results if r["status"] == "error")

    lines = [
        f"# ETF 数据质量报告",
        f"",
        f"**生成时间**: {now}",
        f"**状态**: {ok} ✓ / {warn} ⚠️ / {err} ✗",
        f"",
        f"---",
        f"",
        f"## 概览",
        f"",
        f"| 表 | 行数 | 数据范围 | 状态 | 问题 |",
        f"|---|---|---|---|---|",
    ]

    for r in results:
        icon = "✓" if r["status"] == "ok" else ("⚠️" if r["status"] == "warn" else "✗")
        date_range = f"{r.get('min_date','?')} ~ {r.get('max_date','?')}" if r.get("min_date") else "N/A"
        issues = "; ".join(r["issues"]) if r["issues"] else "—"
        lines.append(
            f"| {r['table']} | {r['rows']:,} | {date_range} | {icon} | {issues} |"
        )

    lines.extend(["", "---", "*报告由 ETF_data 质量监控自动生成*"])

    report = "\n".join(lines)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved: {output_path}")
    return report


def main():
    parser = argparse.ArgumentParser(description="ETF 数据质量监控")
    parser.add_argument("--output", "-o", help="输出文件路径")
    parser.add_argument("--notify", action="store_true", help="并发告警推送")
    args = parser.parse_args()

    setup_logging()

    from src.utils.config_helper import init, get_db_conn_str
    init()
    import pyodbc
    conn = pyodbc.connect(get_db_conn_str("tushare"))

    results = []
    for tbl in TABLES:
        res = check_table(
            conn,
            tbl["name"],
            tbl["expected_min_rows"],
            tbl["freshness_days"],
        )
        results.append(res)
        md = res.get("max_date") or "?"
        print(f"  {res['status']:5s} {tbl['name']:20s}  {res['rows']:>8,} rows  {md:12s}")

    conn.close()

    output_path = args.output or f"reports/quality_{datetime.now().strftime('%Y%m%d')}.md"
    generate_report(results, output_path)

    # 有告警时推送
    if args.notify:
        warn_count = sum(1 for r in results if r["status"] != "ok")
        if warn_count > 0:
            from src.pipeline.notify import _send_serverchan
            lines = [
                f"## ETF Data Quality ⚠️",
                f"**{warn_count} 条告警** — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            ]
            for r in results:
                if r["issues"]:
                    lines.append(f"- {r['table']}: {'; '.join(r['issues'])}")
            _send_serverchan(lines)


if __name__ == "__main__":
    main()
