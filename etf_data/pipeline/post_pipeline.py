"""
管道后处理步骤：数据质量 + 三方比对 + 监控看板

管道跑完后自动调用，输出到 reports/。
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("etf_data.post_pipeline")


def run_quality_check() -> str | None:
    """运行质量检查"""
    import subprocess
    output = Path("reports") / f"quality_{datetime.now().strftime('%Y%m%d')}.md"
    cmd = [
        sys.executable, "etf_data/quality/check.py",
        "-o", str(output),
    ]
    ret = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if ret.returncode == 0:
        logger.info(f"Quality report: {output}")
        return str(output)
    logger.warning(f"Quality check: {ret.stderr[:200]}")
    return None


def run_dashboard() -> str | None:
    """生成监控看板"""
    import subprocess
    output = Path("reports") / "dashboard.html"
    cmd = [
        sys.executable, "etf_data/quality/dashboard.py",
        "-o", str(output),
    ]
    ret = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if ret.returncode == 0:
        logger.info(f"Dashboard: {output}")
        return str(output)
    logger.warning(f"Dashboard: {ret.stderr[:200]}")
    return None


def run_portfolio() -> str | None:
    """持仓分析"""
    import subprocess
    output = Path("reports") / f"portfolio_{datetime.now().strftime('%Y%m%d')}.md"
    cmd = [
        sys.executable, "etf_data/quality/portfolio.py",
        "-o", str(output),
    ]
    ret = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if ret.returncode == 0:
        logger.info(f"Portfolio report: {output}")
        return str(output)
    logger.warning(f"Portfolio: {ret.stderr[:200]}")
    return None


def run_comparison() -> str | None:
    """三方比对（仅5只ETF）"""
    import subprocess
    symbols = "510050.SH,510300.SH,159915.SZ,518880.SH,513100.SH"
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    output = Path("reports") / f"consistency_{datetime.now().strftime('%Y%m%d')}.md"
    cmd = [
        sys.executable, "etf_data/comparator/report.py",
        "--symbols", symbols, "--start", start, "--end", end,
        "-o", str(output),
    ]
    ret = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if ret.returncode == 0:
        logger.info(f"Consistency report: {output}")
        return str(output)
    logger.warning(f"Comparison: {ret.stderr[:200]}")
    return None


def run_all() -> dict:
    """运行所有后处理步骤"""
    reports = {}
    for name, fn in [
        ("quality", run_quality_check),
        ("dashboard", run_dashboard),
        ("portfolio", run_portfolio),
        ("consistency", run_comparison),
    ]:
        try:
            reports[name] = fn()
        except Exception as e:
            logger.warning(f"{name}: {e}")
            reports[name] = None
    return reports
