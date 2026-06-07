#!/usr/bin/env python3
"""
每日增量更新

交易日 15:30 后运行，增量拉取最新数据。

用法:
    python etf_data/scheduler/daily_update.py          # 仅交易日执行
    python etf_data/scheduler/daily_update.py --force  # 强制执行（忽略交易日检查）
    python etf_data/scheduler/daily_update.py --notify # 完成后推送通知
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

logger = logging.getLogger("etf_data.pipeline")


def setup_logging(log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"daily_update_{datetime.now().strftime('%Y%m%d')}.log"
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root = logging.getLogger("etf_data")
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)
    logger.info(f"Daily update log: {log_file}")


def send_notification(summary: str) -> None:
    """通过 Server酱 推送通知"""
    try:
        from ..pipeline.notify import _send_serverchan
        _send_serverchan([
            f"## ETF Data Daily Update",
            f"**{summary}**",
        ])
    except Exception as e:
        logger.warning(f"Notification failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="ETF_data 每日增量更新")
    parser.add_argument("--force", action="store_true", help="强制执行（即使非交易日）")
    parser.add_argument("--notify", action="store_true", help="完成后推送通知")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    setup_logging(log_dir=args.log_dir)

    # 交易日检查
    if not args.force:
        from ..utils.calendar import get_calendar
        cal = get_calendar()
        if not cal.is_trading_day():
            logger.info(f"Today ({date.today()}) is not a trading day. Skipping.")
            return

    from ..utils.config_helper import init
    from ..pipeline import DataPipeline

    init()

    pipeline = DataPipeline(config_path="config/pipeline_tasks.yaml", dry_run=False)

    # 所有任务设为增量模式（跳过 replace 模式的任务）
    for t in pipeline._tasks:
        if t.get("target", {}).get("if_exists") != "replace":
            t["incremental"] = True

    logger.info(f"Starting daily update: {len(pipeline._tasks)} tasks")
    t0 = time.time()
    results = pipeline.run_all(verbose=True)
    elapsed = time.time() - t0

    ok = sum(1 for r in results if r["status"] == "ok")
    fail = len(results) - ok
    total_rows = sum(r.get("rows_written", 0) for r in results)

    summary = (
        f"{date.today()}  |  {ok}/{len(results)} ok  |  {total_rows:,} rows  |  {elapsed:.0f}s"
    )
    logger.info(f"Daily update complete: {summary}")

    if fail > 0:
        failed_tasks = [r["task"] for r in results if r["status"] != "ok"]
        summary += f"\nFailed: {', '.join(failed_tasks)}"

    if args.notify:
        send_notification(summary)

    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

