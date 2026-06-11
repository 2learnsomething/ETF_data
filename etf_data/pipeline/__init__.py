"""
数据管道编排层

根据 YAML 配置驱动数据采集流程:
  Fetcher → Transform → Validate → Storage

支持: 增量更新、断点重试、干跑模式
"""
from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ..fetchers.base import BaseFetcher, FetchRequest
from ..storage.base import BaseStorage
from .validators import DataValidator, daily_data_validator, basic_info_validator

logger = logging.getLogger("etf_data.pipeline")


# ── 预设校验器 ────────────────────────────────────────

_VALIDATOR_PRESETS = {
    "daily": daily_data_validator,
    "basic": basic_info_validator,
}


# ── Fetcher / Storage 注册表 ───────────────────────────

_FETCHER_REGISTRY: dict[str, type[BaseFetcher]] = {}
_STORAGE_REGISTRY: dict[str, type[BaseStorage]] = {}


def register_fetcher(name: str):
    def decorator(cls: type[BaseFetcher]):
        _FETCHER_REGISTRY[name] = cls
        return cls
    return decorator


def register_storage(name: str):
    def decorator(cls: type[BaseStorage]):
        _STORAGE_REGISTRY[name] = cls
        return cls
    return decorator


def _auto_register():
    try:
        from ..fetchers.tushare_fetcher import TushareFetcher
        _FETCHER_REGISTRY["tushare"] = TushareFetcher
    except ImportError:
        pass
    try:
        from ..fetchers.akshare_fetcher import AKShareFetcher
        _FETCHER_REGISTRY["akshare"] = AKShareFetcher
    except ImportError:
        pass
    try:
        from ..fetchers.tencent_fetcher import TencentFetcher
        _FETCHER_REGISTRY["tencent"] = TencentFetcher
    except ImportError:
        pass
    try:
        from ..fetchers.akshare_extra_fetcher import AKShareExtraFetcher
        _FETCHER_REGISTRY["akshare_extra"] = AKShareExtraFetcher
    except ImportError:
        pass
    try:
        from ..fetchers.baostock_fetcher import BaostockFetcher
        _FETCHER_REGISTRY["baostock"] = BaostockFetcher
    except ImportError:
        pass
    try:
        from ..fetchers.amazingdata_fetcher import AmazingDataFetcher
        _FETCHER_REGISTRY["amazingdata"] = AmazingDataFetcher
    except ImportError:
        pass
    try:
        from ..storage.sqlserver_storage import SQLServerStorage
        _STORAGE_REGISTRY["sqlserver"] = SQLServerStorage
    except ImportError:
        pass
    try:
        from ..storage.parquet_storage import ParquetStorage
        _STORAGE_REGISTRY["parquet"] = ParquetStorage
    except ImportError:
        pass


_auto_register()


# ── DataPipeline ───────────────────────────────────────

class DataPipeline:
    """数据管道编排器"""

    def __init__(
        self,
        config_path: str | None = None,
        tasks: list[dict] | None = None,
        dry_run: bool = False,
        log_dir: str | None = None,
    ):
        self.dry_run = dry_run
        if tasks is not None:
            self._tasks = tasks
        elif config_path:
            with open(config_path) as f:
                self._tasks = yaml.safe_load(f).get("tasks", [])
        else:
            self._tasks = []

        # 日志目录
        if log_dir:
            Path(log_dir).mkdir(parents=True, exist_ok=True)

    def run_all(
        self,
        fetcher: BaseFetcher | None = None,
        storage: BaseStorage | None = None,
        verbose: bool = True,
    ) -> list[dict]:
        results = []
        for i, task in enumerate(self._tasks):
            if verbose:
                logger.info(f"[{i+1}/{len(self._tasks)}] {task.get('name', task.get('data_type', '?'))}")

            # 断点重试
            max_retry = task.get("retry", 0)
            retry_delay = task.get("retry_delay", 5)
            for attempt in range(max_retry + 1):
                result = self._run_one(task, fetcher, storage, verbose)
                if result["status"] == "ok":
                    break
                if attempt < max_retry:
                    logger.warning(f"  retry {attempt+1}/{max_retry} in {retry_delay}s: {result.get('error')}")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)  # 指数退避，上限60s

            results.append(result)

        if verbose:
            self._print_summary(results)
        return results

    def _run_one(
        self,
        task: dict,
        shared_fetcher: BaseFetcher | None,
        shared_storage: BaseStorage | None,
        verbose: bool,
    ) -> dict:
        t0 = time.time()

        try:
            # 1. Fetcher
            source = task["source"]
            fetcher = shared_fetcher or self._make_fetcher(source, task.get("fetcher_config", {}))

            # 2. Storage (dry_run 时跳过)
            target = task.get("target", {})

            # 3. 计算日期范围（支持增量模式）
            start_date, end_date = self._resolve_dates(task)

            # 4. 多源回退链 — 依次尝试 source + fallback_sources
            fallback_sources = task.get("fallback_sources", [])
            source_chain = [source] + list(fallback_sources)

            fetch_result = None
            df = None
            validation_report = None
            used_source = source

            for attempt_idx, attempt_source in enumerate(source_chain):
                if attempt_idx > 0:
                    logger.warning(f"  ↓ fallback to {attempt_source} (attempt {attempt_idx}/{len(source_chain)-1})")

                try:
                    attempt_fetcher = self._make_fetcher(attempt_source, task.get("fetcher_config", {}))
                    attempt_fetcher.connect()

                    request = FetchRequest(
                        data_type=task["data_type"],
                        symbols=task.get("symbols", []),
                        start_date=start_date,
                        end_date=end_date,
                        fields=task.get("fields"),
                        extra=task.get("extra", {}),
                    )
                    fetch_result = attempt_fetcher.fetch(request)
                    attempt_fetcher.close()

                    if fetch_result is None or fetch_result.df is None or fetch_result.df.empty:
                        logger.warning(f"  {attempt_source}: 0 rows, trying next")
                        continue

                    # 5. Transform
                    attempt_df = self._maybe_transform(fetch_result.df, task.get("transform"))

                    # 5b. 列名标准化
                    attempt_df = attempt_fetcher.normalize_columns(attempt_df, task["data_type"])

                    # 6. Validate
                    validate_spec = task.get("validate")
                    if validate_spec:
                        validator = self._get_validator(validate_spec)
                        attempt_df, attempt_report = validator.validate(attempt_df)
                    else:
                        attempt_report = None

                    # 有数据 → 用当前源的结果
                    df = attempt_df
                    validation_report = attempt_report
                    used_source = attempt_source
                    if verbose:
                        logger.info(f"  fetched: {len(df)} rows from {attempt_source}")
                        if attempt_report:
                            logger.info(f"  validate: {attempt_report.summary()}")
                    break

                except Exception as e:
                    logger.warning(f"  {attempt_source} failed: {e}")
                    continue

            if df is None or df.empty:
                logger.error(f"  All sources failed for {task.get('name', task['data_type'])}")
                return {
                    "task": task.get("name", task.get("data_type")),
                    "status": "error",
                    "error": f"All {len(source_chain)} sources returned empty",
                    "elapsed": round(time.time() - t0, 2),
                }

            # 7. 数据完整性检查
            completeness_spec = task.get("completeness")
            if completeness_spec and not df.empty:
                comp_report = self._check_completeness(df, completeness_spec)
                if verbose and comp_report:
                    logger.info(f"  completeness: {comp_report}")

            # 8. Write — 支持多目标（SQL Server + Parquet 等）
            n = 0
            if not self.dry_run and target:
                target_list = target if isinstance(target, list) else [target]
                for tgt in target_list:
                    tgt_type = tgt.get("type", "sqlserver")
                    tgt_storage = self._make_storage(tgt_type, tgt.get("config", {}))
                    tgt_storage.connect()
                    try:
                        table = tgt.get("table", task["data_type"])
                        if_exists = tgt.get("if_exists", "append")
                        keys = tgt.get("keys")
                        n_tgt = tgt_storage.write(df, table, if_exists=if_exists, keys=keys)
                        n += n_tgt
                        if verbose:
                            logger.info(f"  wrote: {n_tgt} rows to {tgt_type}::{table}")
                    finally:
                        tgt_storage.close()
            elif self.dry_run:
                tgt_names = [t.get("type", "sqlserver") for t in (target if isinstance(target, list) else [target])] if target else ["parquet"]
                if verbose:
                    logger.info(f"  [dry-run] would write {len(df)} rows to {', '.join(tgt_names)}")

            elapsed = time.time() - t0
            res = {
                "task": task.get("name", task["data_type"]),
                "status": "ok",
                "rows_fetched": len(df),
                "rows_written": n if not self.dry_run else 0,
                "dry_run": self.dry_run,
                "elapsed": round(elapsed, 2),
                "source_used": used_source,
            }
            if validation_report:
                res["validation"] = validation_report.summary()
                res["validation_issues"] = validation_report.issues
            return res

        except Exception as e:
            elapsed = time.time() - t0
            logger.error(f"  FAILED: {e}")
            return {
                "task": task.get("name", task.get("data_type")),
                "status": "error",
                "error": str(e),
                "elapsed": round(elapsed, 2),
            }

    # ── 日期处理 ───────────────────────────────────────

    @staticmethod
    def _resolve_dates(task: dict) -> tuple[str | None, str | None]:
        """
        计算实际日期范围。
        增量模式: incremental=true → 只拉 lookback_days 天
        """
        incremental = task.get("incremental", False)
        if incremental:
            lookback = task.get("lookback_days", 5)
            end = datetime.now()
            start = end - timedelta(days=lookback)
            return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

        return task.get("start_date"), task.get("end_date")

    # ── 数据完整性检查 ─────────────────────────────────

    @staticmethod
    def _check_completeness(df: pd.DataFrame, spec: dict) -> str | None:
        """检查缺交易日、异常跳空等"""
        msgs = []

        # 日期列: trade_date 或 date
        date_col = None
        for col in ["trade_date", "date", "cal_date"]:
            if col in df.columns:
                date_col = col
                break
        if date_col is None:
            return None

        dates = pd.to_datetime(df[date_col]).dropna().sort_values()
        if len(dates) < 2:
            return None

        # 缺交易日检查
        if spec.get("check_gaps", True):
            all_dates = pd.date_range(dates.min(), dates.max(), freq="B")
            missing = len(all_dates) - len(dates.unique())
            if missing > 0:
                msgs.append(f"missing {missing} trading days")

        # 异常跳空检查 (日涨跌幅)
        pct_col = spec.get("pct_change_col")
        if pct_col and pct_col in df.columns:
            threshold = spec.get("gap_threshold", 0.11)  # 默认 ±11%
            extreme = (df[pct_col].abs() > threshold).sum()
            if extreme:
                msgs.append(f"{extreme} extreme gaps (>±{threshold*100:.0f}%)")

        return ", ".join(msgs) if msgs else None

    # ── 工厂方法 ───────────────────────────────────────

    @staticmethod
    def _make_fetcher(source: str, config: dict) -> BaseFetcher:
        cls = _FETCHER_REGISTRY.get(source)
        if cls is None:
            raise ValueError(f"Unknown fetcher '{source}'. Available: {list(_FETCHER_REGISTRY)}")
        return cls(**config)

    @staticmethod
    def _make_storage(storage_type: str, config: dict) -> BaseStorage:
        cls = _STORAGE_REGISTRY.get(storage_type)
        if cls is None:
            raise ValueError(f"Unknown storage '{storage_type}'. Available: {list(_STORAGE_REGISTRY)}")
        return cls(**config)

    @staticmethod
    def _get_validator(spec: str | dict | list) -> DataValidator:
        if isinstance(spec, str):
            factory = _VALIDATOR_PRESETS.get(spec)
            if factory:
                return factory()
            raise ValueError(f"Unknown validator preset '{spec}'. Available: {list(_VALIDATOR_PRESETS)}")

        if isinstance(spec, dict):
            rules = []
            if "required" in spec:
                from .validators import validate_required_columns
                _keys = spec["required"]
                rules.append(lambda df, r, keys=_keys: validate_required_columns(df, keys, r))
            if "dedup_keys" in spec:
                from .validators import deduplicate
                _keys = spec["dedup_keys"]
                rules.append(lambda df, r, keys=_keys: deduplicate(df, r, keys=keys))
            if "ranges" in spec:
                from .validators import validate_numeric_ranges
                _ranges = spec["ranges"]
                rules.append(lambda df, r, ranges=_ranges: validate_numeric_ranges(df, ranges, r))
            return DataValidator(rules)

        if isinstance(spec, list):
            validator = DataValidator()
            for item in spec:
                sub = DataPipeline._get_validator(item)
                validator._rules.extend(sub._rules)
            return validator

        return DataValidator()

    @staticmethod
    def _maybe_transform(df: pd.DataFrame, transform_spec: list[dict] | None) -> pd.DataFrame:
        if not transform_spec:
            return df
        for spec in transform_spec:
            action = spec["action"]
            if action == "rename":
                df = df.rename(columns=spec["mapping"])
            elif action == "drop":
                df = df.drop(columns=spec["columns"], errors="ignore")
            elif action == "astype":
                for col, dtype in spec["mapping"].items():
                    if col in df.columns:
                        df[col] = df[col].astype(dtype)
            elif action == "add_column":
                df[spec["name"]] = spec.get("value", "")
        return df

    @staticmethod
    def _print_summary(results: list[dict]) -> None:
        ok = sum(1 for r in results if r["status"] == "ok")
        fail = len(results) - ok
        total_rows = sum(r.get("rows_written", 0) for r in results)
        total_time = sum(r["elapsed"] for r in results)
        dry = " [DRY-RUN]" if results and results[0].get("dry_run") else ""

        logger.info(f"{'='*60}")
        logger.info(f"Pipeline{dry} done: {ok} ok, {fail} failed, "
                     f"{total_rows:,} rows in {total_time:.1f}s")
        for r in results:
            status = "✓" if r["status"] == "ok" else "✗"
            line = f"  {status} {r['task']}: {r.get('rows_written', 0):,} rows ({r['elapsed']}s)"
            if r.get("error"):
                line += f" — {r['error']}"
            if r.get("validation"):
                line += f" | {r['validation']}"
            logger.info(line)

