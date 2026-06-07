"""
Parquet 存储后端

分区写入，自动按日期分目录:
    {base_dir}/etf_daily/2024/01/15/data.parquet

支持: 写入、读取、追加、分区查询
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .base import BaseStorage


class ParquetStorage(BaseStorage):
    """Parquet 文件存储"""

    def __init__(self, base_dir: str = "Data/parquet", partition_by: str | None = "date"):
        self._base = Path(base_dir)
        self._partition_by = partition_by

    @property
    def backend_name(self) -> str:
        return "parquet"

    def connect(self) -> None:
        self._base.mkdir(parents=True, exist_ok=True)

    def close(self) -> None:
        pass

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append",
        **kwargs,
    ) -> int:
        if df.empty:
            return 0

        table_dir = self._base / table

        if if_exists == "replace":
            import shutil
            if table_dir.exists():
                shutil.rmtree(table_dir)

        table_dir.mkdir(parents=True, exist_ok=True)

        if self._partition_by and self._partition_by in df.columns:
            return self._write_partitioned(df, table_dir)
        else:
            return self._write_single(df, table_dir)

    def read(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        table_dir = self._base / table
        if not table_dir.exists():
            return pd.DataFrame()

        if self._partition_by:
            # 递归读取所有 parquet 文件
            files = list(table_dir.rglob("*.parquet"))
        else:
            files = [table_dir / "data.parquet"]

        frames = []
        for f in files:
            if f.exists():
                df = pd.read_parquet(f, columns=columns)
                if where:
                    df = df.query(where)
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        if limit:
            result = result.head(limit)
        return result

    def table_exists(self, table: str) -> bool:
        return (self._base / table).exists()

    def list_tables(self) -> list[str]:
        if not self._base.exists():
            return []
        return [d.name for d in self._base.iterdir() if d.is_dir()]

    # ── 内部 ────────────────────────────────────────────

    def _write_partitioned(self, df: pd.DataFrame, table_dir: Path) -> int:
        """按分区列分目录写入"""
        total = 0
        col = self._partition_by
        for key, group in df.groupby(col):
            key_str = str(key).replace("-", "")
            if len(key_str) >= 8:  # YYYYMMDD
                partition_path = table_dir / key_str[:4] / key_str[4:6] / key_str[6:8]
            elif len(key_str) >= 6:  # YYYYMM
                partition_path = table_dir / key_str[:4] / key_str[4:6]
            else:
                partition_path = table_dir / key_str

            partition_path.mkdir(parents=True, exist_ok=True)
            file_path = partition_path / "data.parquet"
            group.to_parquet(file_path, index=False)
            total += len(group)

        return total

    def _write_single(self, df: pd.DataFrame, table_dir: Path) -> int:
        """单文件写入"""
        file_path = table_dir / "data.parquet"
        df.to_parquet(file_path, index=False)
        return len(df)
