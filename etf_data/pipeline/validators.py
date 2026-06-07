"""
数据校验层

在 Fetcher → Storage 之间对 DataFrame 进行校验和清洗。
所有 validator 返回 (df, issues) — 清洗后的数据 + 问题报告。
"""
from __future__ import annotations

from typing import Callable

import pandas as pd


# ── 校验结果 ──────────────────────────────────────────

class ValidationReport:
    """校验报告"""

    def __init__(self):
        self.issues: list[dict] = []
        self.rows_before: int = 0
        self.rows_after: int = 0

    def warn(self, rule: str, detail: str, count: int = 0):
        self.issues.append({"level": "warn", "rule": rule, "detail": detail, "count": count})

    def error(self, rule: str, detail: str, count: int = 0):
        self.issues.append({"level": "error", "rule": rule, "detail": detail, "count": count})

    @property
    def ok(self) -> bool:
        return not any(i["level"] == "error" for i in self.issues)

    def summary(self) -> str:
        if not self.issues:
            return "validation: clean"
        parts = []
        for i in self.issues:
            tag = "WARN" if i["level"] == "warn" else "ERR"
            cnt = f" (n={i['count']})" if i["count"] else ""
            parts.append(f"[{tag}] {i['rule']}: {i['detail']}{cnt}")
        return ", ".join(parts)


# ── Validator 函数 ────────────────────────────────────

def validate_required_columns(
    df: pd.DataFrame,
    required: list[str],
    report: ValidationReport,
) -> pd.DataFrame:
    """检查必填字段：缺失则报 error"""
    missing = [c for c in required if c not in df.columns]
    if missing:
        report.error("missing_columns", f"required: {missing}")
    return df


def deduplicate(
    df: pd.DataFrame,
    report: ValidationReport,
    *,
    keys: list[str],
    keep: str = "last",
) -> pd.DataFrame:
    """
    按主键去重。

    Args:
        keys: 主键列，如 ['ts_code', 'trade_date']
        keep: 'first' 保留第一条 / 'last' 保留最后一条
    """
    available_keys = [k for k in keys if k in df.columns]
    if not available_keys:
        report.warn("dedup_skipped", f"no keys {keys} found in columns {list(df.columns)}")
        return df

    before = len(df)
    df = df.drop_duplicates(subset=available_keys, keep=keep)
    removed = before - len(df)
    if removed:
        report.warn("dedup", f"removed {removed} duplicate rows on {available_keys}", count=removed)
    return df


def strip_strings(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    """去除字符串列首尾空白"""
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
    return df


def drop_null_keys(
    df: pd.DataFrame,
    keys: list[str],
    report: ValidationReport,
) -> pd.DataFrame:
    """删除主键为 NULL 的行"""
    available = [k for k in keys if k in df.columns]
    if not available:
        return df
    mask = df[available].notna().all(axis=1)
    removed = (~mask).sum()
    if removed:
        report.warn("null_keys", f"dropped {removed} rows with null keys in {available}", count=removed)
        df = df[mask]
    return df


def validate_numeric_ranges(
    df: pd.DataFrame,
    ranges: dict[str, tuple[float | None, float | None]],
    report: ValidationReport,
) -> pd.DataFrame:
    """
    数值范围校验（越界→标记 warn，不删除行）。

    ranges: {'close': (0, None), 'volume': (0, None)}
    """
    for col, (lo, hi) in ranges.items():
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        mask = series.notna()
        if lo is not None:
            mask &= series >= lo
        if hi is not None:
            mask &= series <= hi
        bad = (~mask).sum()
        if bad and series.notna().any():
            report.warn("range", f"{col} out of range [{lo}, {hi}]: {bad} rows", count=bad)
    return df


# ── 组合校验 ───────────────────────────────────────────

class DataValidator:
    """组合多个校验规则"""

    def __init__(self, rules: list[Callable] | None = None):
        self._rules: list[Callable] = rules or []

    def add(self, rule: Callable):
        self._rules.append(rule)

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationReport]:
        report = ValidationReport()
        report.rows_before = len(df)
        for rule in self._rules:
            df = rule(df, report)
        report.rows_after = len(df)

        # 过滤 error 级别的行（如 drop_null_keys 影响的）
        dropped = report.rows_before - report.rows_after
        if dropped:
            report.warn("total_dropped", f"{dropped} rows removed by validation", count=dropped)

        return df, report


# ── 预设规则集 ─────────────────────────────────────────

def daily_data_validator() -> DataValidator:
    """日线数据标准校验（列名已标准化: symbol, date, close, volume, amount）"""
    return DataValidator([
        lambda df, r: validate_required_columns(
            df, ["symbol", "date", "close"], r),
        lambda df, r: drop_null_keys(df, ["symbol", "date"], r),
        lambda df, r: deduplicate(df, r, keys=["symbol", "date"]),
        lambda df, r: strip_strings(df, r),
        lambda df, r: validate_numeric_ranges(df, {
            "close": (0.001, None),
            "volume": (0, None),
            "amount": (0, None),
        }, r),
    ])


def basic_info_validator() -> DataValidator:
    """基本信息标准校验（列名已标准化: symbol, name）"""
    return DataValidator([
        lambda df, r: validate_required_columns(df, ["symbol", "name"], r),
        lambda df, r: drop_null_keys(df, ["symbol"], r),
        lambda df, r: deduplicate(df, r, keys=["symbol"]),
        lambda df, r: strip_strings(df, r),
    ])
