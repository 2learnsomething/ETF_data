"""
Parquet 存储后端 — pipeline 接入层

使用与 amazingdata 模块相同的分区结构：
  K 线类: {root}/parquet/{table}/year={year}/{symbol}.parquet
  小表类:  {root}/parquet/{table}/data.parquet

与 amazingdata.storage.ParquetStore 共享同一存储目录，
两个系统读写的数据完全互通。
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .base import BaseStorage

logger = logging.getLogger("etf_data.storage.parquet")

# 按年+标的 分区存储的表（日线、分钟线、复权因子等大表）
_PARTITIONED_TABLES: set[str] = {
    "etf_daily", "etf_min1", "etf_min5",
    "index_daily",
    "etf_adj_factor",
    "stk_limit",
}

# 默认日期列名（用于分区提取年份）
_DATE_COLUMNS = ["date", "trade_date", "kline_time", "cal_date"]


class ParquetStorage(BaseStorage):
    """Parquet 分区存储后端

    路径约定:
      {root}/parquet/{table}/year={year}/{symbol}.parquet   (分区表)
      {root}/parquet/{table}/data.parquet                    (非分区表)

    Args:
        root: 数据根目录（默认 /mnt/etf_data）
    """

    def __init__(self, root: str = "/mnt/etf_data"):
        self._root = Path(root) / "parquet"
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def backend_name(self) -> str:
        return "parquet"

    def connect(self) -> None:
        pass  # Parquet 无需连接

    def close(self) -> None:
        pass

    # ── 写入 ──────────────────────────────────────────────

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append",
        keys: list[str] | None = None,
        **kwargs,
    ) -> int:
        """写入 Parquet 文件

        Args:
            df: 数据
            table: 表名
            if_exists: append(追加) / replace(覆盖) / fail(抛错)
            keys: 去重主键（Parquet 不支持主键约束，用于辅助去重）

        Returns:
            int: 写入行数
        """
        if df.empty:
            return 0

        if table in _PARTITIONED_TABLES:
            return self._write_partitioned(df, table, if_exists, keys)
        else:
            return self._write_unpartitioned(df, table, if_exists, keys)

    def _write_partitioned(
        self, df: pd.DataFrame, table: str,
        if_exists: str, keys: list[str] | None,
    ) -> int:
        """分区写入：按 year + symbol 分文件"""
        # 定位日期列
        date_col = self._find_date_col(df)
        if date_col is None:
            logger.warning(f"{table}: 未找到日期列，写入非分区模式")
            return self._write_unpartitioned(df, table, if_exists, keys)

        # 确保日期列是 datetime
        df[date_col] = pd.to_datetime(df[date_col])

        symbol_col = self._find_symbol_col(df)
        total = 0

        for (year, symbol), group in df.groupby(
            [df[date_col].dt.year, symbol_col], sort=False
        ):
            sym_dir = self._root / table / f"year={year}"
            sym_dir.mkdir(parents=True, exist_ok=True)
            path = sym_dir / f"{symbol}.parquet"

            if path.exists() and if_exists == "append":
                # 读取已有数据，去重后合并
                existing = pd.read_parquet(path)
                combined = pd.concat([existing, group], ignore_index=True)
                if keys:
                    combined = combined.drop_duplicates(subset=keys, keep="last")
                combined.to_parquet(path, compression="zstd", index=False)
                total += len(group)
            elif path.exists() and if_exists == "fail":
                raise FileExistsError(f"{path} 已存在")
            else:
                # replace 或新建
                group.to_parquet(path, compression="zstd", index=False)
                total += len(group)

        return total

    def _write_unpartitioned(
        self, df: pd.DataFrame, table: str,
        if_exists: str, keys: list[str] | None,
    ) -> int:
        """非分区写入：单文件"""
        table_dir = self._root / table
        table_dir.mkdir(parents=True, exist_ok=True)
        path = table_dir / "data.parquet"

        if path.exists() and if_exists == "append":
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            if keys:
                combined = combined.drop_duplicates(subset=keys, keep="last")
            combined.to_parquet(path, compression="zstd", index=False)
        elif path.exists() and if_exists == "fail":
            raise FileExistsError(f"{path} 已存在")
        else:
            # replace 或新建
            df.to_parquet(path, compression="zstd", index=False)

        return len(df)

    # ── 读取 ──────────────────────────────────────────────

    def read(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """读取数据"""
        if table in _PARTITIONED_TABLES:
            return self._read_partitioned(table, columns, where, limit)
        else:
            return self._read_unpartitioned(table, columns, where, limit)

    def _read_partitioned(
        self, table: str,
        columns: list[str] | None, where: str | None, limit: int | None,
    ) -> pd.DataFrame:
        """读取分区表：遍历 year 目录"""
        table_dir = self._root / table
        if not table_dir.exists():
            return pd.DataFrame()

        frames = []
        for year_dir in sorted(table_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.startswith("year="):
                continue
            for parquet_file in year_dir.iterdir():
                if parquet_file.suffix == ".parquet":
                    df = pd.read_parquet(parquet_file)
                    if columns:
                        df = df[[c for c in columns if c in df.columns]]
                    frames.append(df)
                    if limit and sum(len(f) for f in frames) >= limit:
                        break
            if limit and sum(len(f) for f in frames) >= limit:
                break

        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        if limit:
            result = result.head(limit)
        return result

    def _read_unpartitioned(
        self, table: str,
        columns: list[str] | None, where: str | None, limit: int | None,
    ) -> pd.DataFrame:
        """读取非分区表"""
        path = self._root / table / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if columns:
            df = df[[c for c in columns if c in df.columns]]
        if limit:
            df = df.head(limit)
        return df

    # ── 元信息 ────────────────────────────────────────────

    def table_exists(self, table: str) -> bool:
        if table in _PARTITIONED_TABLES:
            table_dir = self._root / table
            if not table_dir.exists():
                return False
            # 任意 year 目录下有 parquet 文件即视为存在
            for year_dir in table_dir.iterdir():
                if year_dir.is_dir() and any(f.suffix == ".parquet" for f in year_dir.iterdir()):
                    return True
            return False
        else:
            return (self._root / table / "data.parquet").exists()

    def list_tables(self) -> list[str]:
        if not self._root.exists():
            return []
        return sorted(
            d.name for d in self._root.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        )

    # ── 工具 ──────────────────────────────────────────────

    @staticmethod
    def _find_date_col(df: pd.DataFrame) -> str | None:
        """从 DataFrame 中找日期列"""
        for col in _DATE_COLUMNS:
            if col in df.columns:
                return col
        # fallback: 找含 date 的列
        for col in df.columns:
            if "date" in col.lower():
                return col
        return None

    @staticmethod
    def _find_symbol_col(df: pd.DataFrame) -> str:
        """从 DataFrame 中找标的列"""
        for col in ["symbol", "etf_code", "code", "ts_code"]:
            if col in df.columns:
                return col
        return "symbol"
