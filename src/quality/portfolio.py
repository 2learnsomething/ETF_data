#!/usr/bin/env python3
"""
ETF 持仓穿透分析

从 fund_portfolio 表分析 ETF 持仓重叠度、行业暴露、集中度。

用法:
    python src/quality/portfolio.py                    # 全部持仓分析
    python src/quality/portfolio.py --symbols 510050,510300  # 指定ETF
    python src/quality/portfolio.py -o reports/portfolio.md   # 输出报告
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))


def load_portfolio(conn, symbols: list[str] | None = None) -> pd.DataFrame:
    """加载最新一期持仓"""
    where = ""
    if symbols:
        quoted = [f"'{s}.SH'" if "." not in s else f"'{s}'" for s in symbols]
        where = f" WHERE fund_code IN ({', '.join(quoted)})"
    sql = f"""
        SELECT p.fund_code, p.date, p.symbol, p.stk_mkv_ratio, p.mkv,
               b.csname AS fund_name
        FROM [dbo].[fund_portfolio] p
        LEFT JOIN [dbo].[etf_basic] b ON p.fund_code = b.symbol
        {where}
    """
    return pd.read_sql_query(sql, conn)


def generate_report(conn, output_path: str | None = None) -> str:
    """生成持仓穿透报告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# ETF 持仓穿透分析",
        f"**生成时间**: {now}",
        f"",
        f"---",
        f"",
    ]

    df = load_portfolio(conn)
    if df.empty:
        lines.append("无持仓数据。")
        return "\n".join(lines)

    # 最新报告期
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date]
    lines.append(f"**最新报告期**: {latest_date}")
    lines.append(f"**有持仓的ETF数**: {latest['fund_code'].nunique()}")
    lines.append(f"**持仓股票总数**: {latest['symbol'].nunique()}")
    lines.append("")

    # 每只ETF的前5大持仓
    lines.append("---")
    lines.append("")
    lines.append("## ETF 前5大持仓")
    lines.append("")
    for fund in sorted(latest["fund_code"].unique()):
        fund_df = latest[latest["fund_code"] == fund].sort_values(
            "stk_mkv_ratio", ascending=False
        ).head(5)
        fund_name = fund_df["fund_name"].iloc[0] if pd.notna(fund_df["fund_name"].iloc[0]) else fund
        lines.append(f"### {fund_name} ({fund})")
        lines.append("")
        lines.append("| 持仓股票 | 市值占比 |")
        lines.append("|---|---|")
        for _, row in fund_df.iterrows():
            lines.append(f"| {row['symbol']} | {row['stk_mkv_ratio']:.2f}% |")
        lines.append("")

    # 跨ETF重仓股分析
    lines.append("---")
    lines.append("")
    lines.append("## 跨ETF重仓股重叠")
    lines.append("")
    lines.append("被多只ETF同时重仓的股票（Top 10 持仓）：")
    lines.append("")

    # 统计每只股票被多少ETF持仓超过3%
    top10 = latest[latest["stk_mkv_ratio"] >= 3]
    overlap = top10.groupby("symbol").agg(
        etf_count=("fund_code", "nunique"),
        avg_ratio=("stk_mkv_ratio", "mean"),
    ).sort_values("etf_count", ascending=False)

    if not overlap.empty:
        lines.append("| 股票 | 被持仓ETF数 | 平均占比 |")
        lines.append("|---|---|---|")
        for sym, row in overlap.head(20).iterrows():
            lines.append(f"| {sym} | {int(row['etf_count'])} | {row['avg_ratio']:.2f}% |")
    lines.append("")

    # 集中度
    lines.append("---")
    lines.append("")
    lines.append("## 持仓集中度")
    lines.append("")
    for fund in sorted(latest["fund_code"].unique()):
        fund_df = latest[latest["fund_code"] == fund]
        top3 = fund_df.nlargest(3, "stk_mkv_ratio")["stk_mkv_ratio"].sum()
        top5 = fund_df.nlargest(5, "stk_mkv_ratio")["stk_mkv_ratio"].sum()
        n_holdings = len(fund_df)
        lines.append(f"- {fund}: {n_holdings}只持仓, Top3={top3:.1f}%, Top5={top5:.1f}%")

    lines.append("")
    lines.append("---")
    lines.append("*报告由 ETF_data 持仓分析自动生成*")

    report = "\n".join(lines)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved: {output_path}")
    return report


def main():
    parser = argparse.ArgumentParser(description="ETF 持仓穿透分析")
    parser.add_argument("--symbols", help="逗号分隔的 ETF 代码")
    parser.add_argument("--output", "-o", help="输出文件路径")
    args = parser.parse_args()

    from src.utils.config_helper import init, get_db_conn_str
    init()
    import pyodbc
    conn = pyodbc.connect(get_db_conn_str("tushare"))

    symbols = args.symbols.split(",") if args.symbols else None
    output = args.output or f"reports/portfolio_{datetime.now().strftime('%Y%m%d')}.md"

    generate_report(conn, output)
    conn.close()


if __name__ == "__main__":
    main()
