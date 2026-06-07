"""
多源数据一致性报告

读取 DiffEngine 输出，生成 Markdown 报告。
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))


def generate_report(results: dict, output_path: str | None = None) -> str:
    summary = results["summary"]
    details = results.get("details", pd.DataFrame())
    overall = results["overall"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        f"# ETF 数据多源一致性报告",
        f"",
        f"**生成时间**: {now}",
        f"**比对 ETF 数**: {overall.get('n_etfs', 0)}",
        f"**平均一致性**: {overall.get('avg_consistency', 0):.2%}",
        f"**总差异条数**: {overall.get('total_diffs', 0)}",
        f"**差异容忍度**: O/H/L/C ≤ 0.5%, Volume ≤ 2%",
        f"",
        f"---",
        f"",
    ]

    # 差异字段分布
    if not details.empty and "field" in details.columns:
        lines.append("### 差异字段分布")
        lines.append("")
        lines.append("| 字段 | 差异数 | 占比 |")
        lines.append("|---|---|---|")
        field_counts = details["field"].value_counts()
        for field, count in field_counts.items():
            pct = count / len(details)
            lines.append(f"| {field} | {count} | {pct:.1%} |")
        lines.append("")

        # 差异源分布
        if "src1" in details.columns:
            lines.append("### 差异比对分布")
            lines.append("")
            lines.append("| 比对对 | 差异数 | 占比 |")
            lines.append("|---|---|---|")
            pair_counts = (details["src1"] + " vs " + details["src2"]).value_counts()
            for pair, count in pair_counts.items():
                pct = count / len(details)
                lines.append(f"| {pair} | {count} | {pct:.1%} |")
            lines.append("")
    else:
        lines.append("无差异记录")
        lines.append("")

    # 每 ETF 详情
    lines.append("---")
    lines.append("")
    lines.append("## ETF 一致性详情")
    lines.append("")
    lines.append("| ETF | 比对日期数 | 总对次 | 差异数 | 一致性 |")
    lines.append("|---|---|---|---|---|")

    if not summary.empty:
        for _, row in summary.iterrows():
            cons = row.get("consistency", 0)
            status = "🟢" if cons >= 0.95 else ("🟡" if cons >= 0.80 else "🔴")
            sym = row["symbol"]
            n_diffs = int(row.get("n_diffs", 0))

            # 从 details 取实际日期数
            sym_details = details[details["symbol"] == sym] if not details.empty else pd.DataFrame()
            n_dates = sym_details["date"].nunique() if not sym_details.empty else 0
            total_comps = n_dates * 6 * 3  # 6 fields × 3 pairs

            lines.append(
                f"| {status} {sym} | {n_dates} | {total_comps} | {n_diffs} | {cons:.2%} |"
            )

    lines.append("")
    lines.append("---")
    lines.append("*报告由 ETF_data comparator 自动生成*")

    report = "\n".join(lines)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved: {output_path}")
    return report


def main():
    parser = argparse.ArgumentParser(description="ETF 多源数据一致性报告")
    parser.add_argument("--symbols", default="510050.SH,510300.SH,159915.SZ",
                        help="逗号分隔的 ETF 代码")
    parser.add_argument("--start", default="20240501",
                        help="起始日期 YYYYMMDD")
    parser.add_argument("--end", help="结束日期，默认今天")
    parser.add_argument("--output", "-o", help="输出文件路径")
    args = parser.parse_args()

    symbols = args.symbols.split(",")
    end_date = args.end or datetime.now().strftime("%Y%m%d")

    from .diff_engine import DiffEngine

    engine = DiffEngine()
    print(f"Comparing {len(symbols)} ETFs: {args.start} ~ {end_date}")
    results = engine.compare_batch(symbols, args.start, end_date)

    output_path = args.output or f"reports/consistency_{datetime.now().strftime('%Y%m%d')}.md"
    generate_report(results, output_path)
    print(results.get("overall", {}).get("avg_consistency", "N/A"))


if __name__ == "__main__":
    main()

