#!/usr/bin/env python3
"""
全量历史数据回填

遍历 pipeline_tasks.yaml 所有任务，以非增量模式拉取全量历史数据。

用法:
    python etf_data/scheduler/backfill.py                    # 全部任务
    python etf_data/scheduler/backfill.py --tasks etf_daily   # 指定任务
    python etf_data/scheduler/backfill.py --notify             # 完成后推送通知
    python etf_data/scheduler/backfill.py --dry-run            # 干跑
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))


def setup_logging(log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    logging.getLogger("etf_data.pipeline").info(f"Backfill log: {log_file}")


def send_notification(results: dict) -> None:
    """通过 Server酱 推送通知"""
    try:
        from ..pipeline.notify import send_pipeline_report
        send_pipeline_report(results.get("results", []), dry_run=False)
    except Exception as e:
        logging.getLogger("etf_data.pipeline").warning(f"Notification failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="ETF_data 全量历史回填")
    parser.add_argument("--tasks", help="逗号分隔的任务名")
    parser.add_argument("--notify", action="store_true", help="完成后推送通知")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    setup_logging(log_dir=args.log_dir)
    logger = logging.getLogger("etf_data.pipeline")

    from ..utils.config_helper import init
    from ..pipeline import DataPipeline

    init()

    pipeline = DataPipeline(config_path="config/pipeline_tasks.yaml", dry_run=args.dry_run)

    # 强制全量模式（覆盖 YAML 的 incremental=true）
    for t in pipeline._tasks:
        t["incremental"] = False

    if args.tasks:
        task_filter = args.tasks.split(",")
        pipeline._tasks = [
            t for t in pipeline._tasks
            if t.get("name", t.get("data_type")) in task_filter
        ]
        logger.info(f"Filtered to {len(pipeline._tasks)} tasks: {task_filter}")

    if not pipeline._tasks:
        logger.warning("No tasks to run.")
        return

    logger.info(f"Starting backfill: {len(pipeline._tasks)} tasks (incremental=False)")
    t0 = time.time()
    results = pipeline.run_all(verbose=True)
    elapsed = time.time() - t0

    ok = sum(1 for r in results if r["status"] == "ok")
    fail = len(results) - ok
    total_rows = sum(r.get("rows_written", 0) for r in results)

    summary = {
        "ok": ok,
        "fail": fail,
        "total_rows": total_rows,
        "elapsed": round(elapsed, 1),
        "results": results,
    }

    logger.info(f"Backfill complete: {ok} ok, {fail} failed, {total_rows:,} rows in {elapsed:.0f}s")

    if args.notify:
        send_notification(summary)

    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

