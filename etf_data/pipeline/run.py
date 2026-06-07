"""
ETF_data 数据管道 CLI 入口

用法:
    # 全量运行
    python etf_data/pipeline/run.py

    # 干跑（只拉取不写入）
    python etf_data/pipeline/run.py --dry-run

    # 指定任务
    python etf_data/pipeline/run.py --tasks stock_basic,trade_calendar

    # 增量模式（覆盖 YAML 里的 incremental 设置）
    python etf_data/pipeline/run.py --incremental

    # 静默 + 通知
    python etf_data/pipeline/run.py --quiet --notify

    # 注册为 Hermes 定时任务:
    hermes cron create \
        --name "etf-data-daily" \
        --schedule "0 18 * * 1-5" \
        --prompt "cd /home/fangyao_xu/ETF_data && PYTHONPATH=. python etf_data/pipeline/run.py --incremental --notify"
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


def setup_logging(log_dir: str = "logs", quiet: bool = False) -> None:
    """配置日志：同时输出文件和控制台"""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 文件 handler（始终写）
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING if quiet else logging.INFO)
    ch.setFormatter(fmt)

    root = logging.getLogger("etf_data")
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)

    logging.getLogger("etf_data.pipeline").info(f"Logging to {log_file}")


def run_pipeline(
    config_path: str = "config/pipeline_tasks.yaml",
    task_filter: list[str] | None = None,
    dry_run: bool = False,
    incremental: bool = False,
    verbose: bool = True,
    notify: bool = False,
) -> dict:
    """运行数据管道，返回汇总结果。"""
    from etf_data.utils.config_helper import init
    from etf_data.pipeline import DataPipeline

    init()

    pipeline = DataPipeline(
        config_path=config_path,
        dry_run=dry_run,
    )

    if incremental:
        for t in pipeline._tasks:
            if t.get("target", {}).get("if_exists") != "replace":
                t["incremental"] = True

    if task_filter:
        pipeline._tasks = [
            t for t in pipeline._tasks
            if t.get("name", t.get("data_type")) in task_filter
        ]
        if verbose:
            logging.getLogger("etf_data.pipeline").info(
                f"Filtered to {len(pipeline._tasks)} tasks: {task_filter}"
            )

    if not pipeline._tasks:
        logging.getLogger("etf_data.pipeline").warning("No tasks to run.")
        return {"ok": 0, "fail": 0, "total_rows": 0, "elapsed": 0}

    t0 = time.time()
    results = pipeline.run_all(verbose=verbose)
    elapsed = time.time() - t0

    ok = sum(1 for r in results if r["status"] == "ok")
    fail = len(results) - ok
    total_rows = sum(r.get("rows_written", 0) for r in results)

    if notify:
        try:
            from etf_data.pipeline.notify import send_pipeline_report
            send_pipeline_report(results, dry_run=dry_run)
        except Exception as e:
            logging.getLogger("etf_data.pipeline").warning(f"Notification failed: {e}")

    return {
        "ok": ok,
        "fail": fail,
        "total_rows": total_rows,
        "elapsed": round(elapsed, 1),
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description="ETF_data 数据管道")
    parser.add_argument("--config", default="config/pipeline_tasks.yaml")
    parser.add_argument("--tasks", help="逗号分隔的任务名")
    parser.add_argument("--dry-run", action="store_true", help="只拉取不写入")
    parser.add_argument("--incremental", action="store_true", help="增量模式（覆盖YAML设置）")
    parser.add_argument("--quiet", action="store_true", help="静默模式")
    parser.add_argument("--notify", action="store_true", help="完成后推送通知")
    parser.add_argument("--log-dir", default="logs", help="日志目录")
    args = parser.parse_args()

    setup_logging(log_dir=args.log_dir, quiet=args.quiet)

    task_filter = args.tasks.split(",") if args.tasks else None
    result = run_pipeline(
        config_path=args.config,
        task_filter=task_filter,
        dry_run=args.dry_run,
        incremental=args.incremental,
        verbose=not args.quiet,
        notify=args.notify,
    )

    if result["fail"] > 0:
        sys.exit(1)

    # 后处理：数据质量 + 三方比对 + 看板
    try:
        from etf_data.pipeline.post_pipeline import run_all
        reports = run_all()
        for name, path in reports.items():
            if path:
                logging.getLogger("etf_data.pipeline").info(f"  {name}: {path}")
    except Exception as e:
        logging.getLogger("etf_data.pipeline").warning(f"Post-pipeline: {e}")


if __name__ == "__main__":
    main()
