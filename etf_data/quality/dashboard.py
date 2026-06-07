#!/usr/bin/env python3
"""
ETF 数据管道监控看板

用 pyecharts 生成 HTML 看板：行数趋势、延迟分析、数据新鲜度。

用法:
    python etf_data/quality/dashboard.py -o reports/dashboard.html
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))


def build_dashboard(output_path: str) -> str:
    """生成 HTML 看板"""
    from ..utils.config_helper import init, get_db_conn_str
    init()
    import pyodbc
    conn = pyodbc.connect(get_db_conn_str("tushare"))

    tables_data = []
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='dbo' AND TABLE_TYPE='BASE TABLE' "
        "ORDER BY TABLE_NAME"
    )
    for t in cursor.fetchall():
        name = t[0]
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{name}]")
        rows = cursor.fetchone()[0]
        # 找日期列
        cursor.execute(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='{name}'"
        )
        date_cols = [r[0] for r in cursor.fetchall() if "date" in r[0].lower()]
        max_date = None
        if date_cols and rows > 0:
            try:
                cursor.execute(f"SELECT MAX([{date_cols[0]}]) FROM [dbo].[{name}]")
                max_date = cursor.fetchone()[0]
                if max_date:
                    max_date = str(max_date)[:10]
            except Exception:
                pass
        tables_data.append({"name": name, "rows": rows, "max_date": max_date})

    conn.close()

    # 生成 HTML
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = sum(t["rows"] for t in tables_data)

    rows_html = "".join(
        f"<tr><td>{t['name']}</td>"
        f"<td style='text-align:right'>{t['rows']:,}</td>"
        f"<td>{t['max_date'] or '—'}</td>"
        f"<td><div class='bar' style='width:{min(t['rows']/max(total_rows/len(tables_data),1)*100, 100):.1f}%'></div></td></tr>"
        for t in tables_data
    )

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>ETF 数据管道监控</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 1000px; margin: 0 auto; padding: 20px;
         background: #0d1117; color: #c9d1d9; }}
  h1 {{ color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 8px; }}
  .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr));
           gap: 12px; margin: 20px 0; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
          padding: 16px; text-align: center; }}
  .card .val {{ font-size: 28px; font-weight: bold; color: #58a6ff; }}
  .card .lbl {{ font-size: 12px; color: #8b949e; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 13px; }}
  th, td {{ padding: 8px 12px; border: 1px solid #30363d; }}
  th {{ background: #161b22; color: #8b949e; position: sticky; top: 0; }}
  tr:nth-child(even) {{ background: #161b22; }}
  .bar {{ height: 16px; background: linear-gradient(90deg, #238636, #2ea043);
         border-radius: 3px; min-width: 4px; }}
  .footer {{ color: #8b949e; font-size: 12px; margin-top: 30px; }}
</style>
</head>
<body>
<h1>📊 ETF 数据管道监控</h1>
<p>{now}</p>

<div class="stats">
  <div class="card"><div class="val">{total_rows:,}</div><div class="lbl">总行数</div></div>
  <div class="card"><div class="val">{len(tables_data)}</div><div class="lbl">数据表</div></div>
  <div class="card"><div class="val">{sum(1 for t in tables_data if t['rows'] > 0)}</div><div class="lbl">有数据表</div></div>
</div>

<h2>表概览</h2>
<table>
<tr><th>表名</th><th>行数</th><th>最新数据</th><th>规模</th></tr>
{rows_html}
</table>

<div class="footer">由 ETF_data quality dashboard 自动生成</div>
</body>
</html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Dashboard saved: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="ETF 数据管道监控看板")
    parser.add_argument("--output", "-o", default="reports/dashboard.html")
    args = parser.parse_args()
    build_dashboard(args.output)


if __name__ == "__main__":
    main()

