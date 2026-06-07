"""
SQL Server 存储后端

使用 ODBC Driver 18，支持批量写入、类型推断、表自动创建。
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import pyodbc

from .base import BaseStorage

# pandas dtype → SQL Server type
_DTYPE_MAP: dict[str, str] = {
    "int64":     "BIGINT",
    "int32":     "INT",
    "int16":     "SMALLINT",
    "float64":   "FLOAT",
    "float32":   "REAL",
    "object":    "NVARCHAR(MAX)",
    "bool":      "BIT",
    "datetime64[ns]": "DATETIME2",
}


class SQLServerStorage(BaseStorage):
    """SQL Server 存储后端"""

    def __init__(self, conn_str: Optional[str] = None, schema: str = "dbo"):
        self._conn_str = conn_str
        self._schema = schema
        self._conn: pyodbc.Connection | None = None

    @property
    def backend_name(self) -> str:
        return "sqlserver"

    # ── 连接管理 ──────────────────────────────────────────

    def connect(self) -> None:
        if self._conn_str is None:
            from ..utils.config_helper import get_db_conn_str
            self._conn_str = get_db_conn_str("tushare")
        self._conn = pyodbc.connect(self._conn_str)
        self._conn.autocommit = False

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── CRUD ──────────────────────────────────────────────

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append",
        batch_size: int = 5000,
        keys: list[str] | None = None,     # 去重主键
        **kwargs,
    ) -> int:
        if df.empty:
            return 0

        full_table = f"[{self._schema}].[{table}]"
        exists = self.table_exists(table)

        if if_exists == "fail" and exists:
            raise ValueError(f"Table {full_table} already exists")
        if if_exists == "replace" and exists:
            self._drop_table(table)

        if not exists or if_exists == "replace":
            self._create_table(df, table, keys=keys)

        # 有主键 → MERGE（upsert）；无主键 → 直接 INSERT
        if keys and exists:
            total = self._upsert(df, table, keys, batch_size)
        else:
            total = self._batch_insert(df, table, batch_size)

        self._conn.commit()
        return total

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        keys: list[str],
        batch_size: int = 1000,
    ) -> int:
        """去重写入：按主键 MERGE，已存在则更新，不存在则插入"""
        if df.empty:
            return 0
        if not self.table_exists(table):
            self._create_table(df, table, keys=keys)
        total = self._upsert(df, table, keys, batch_size)
        self._conn.commit()
        return total

    def read(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        full_table = f"[{self._schema}].[{table}]"
        cols = ", ".join(f"[{c}]" for c in columns) if columns else "*"
        sql = f"SELECT {cols} FROM {full_table}"
        if where:
            sql += f" WHERE {where}"
        if limit:
            sql += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"

        return pd.read_sql_query(sql, self._conn)

    # ── 元信息 ────────────────────────────────────────────

    def table_exists(self, table: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            self._schema, table,
        )
        return cursor.fetchone() is not None

    def list_tables(self) -> list[str]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME",
            self._schema,
        )
        return [r[0] for r in cursor.fetchall()]

    # ── 内部方法 ──────────────────────────────────────────

    def _drop_table(self, table: str) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS [{self._schema}].[{table}]")
        self._conn.commit()

    def _create_table(self, df: pd.DataFrame, table: str, keys: list[str] | None = None) -> None:
        """根据 DataFrame 自动建表（可选主键）"""
        cols = []
        pk_cols = []
        for col in df.columns:
            sql_type = self._infer_sql_type(df[col], col)
            is_pk = keys and col in keys
            if is_pk:
                col_def = f"[{col}] {sql_type} NOT NULL"
                pk_cols.append(f"[{col}]")
            else:
                col_def = f"[{col}] {sql_type} NULL"
            cols.append(col_def)

        sql = f"CREATE TABLE [{self._schema}].[{table}] (\n  " + ",\n  ".join(cols)
        if pk_cols:
            sql += f",\n  CONSTRAINT [PK_{table}] PRIMARY KEY ({', '.join(pk_cols)})"
        sql += "\n)"
        self._conn.execute(sql)
        self._conn.commit()

    def _batch_insert(self, df: pd.DataFrame, table: str, batch_size: int) -> int:
        """批量 INSERT，内存安全"""
        full_table = f"[{self._schema}].[{table}]"
        columns = list(df.columns)
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(f"[{c}]" for c in columns)
        sql = f"INSERT INTO {full_table} ({col_list}) VALUES ({placeholders})"

        cursor = self._conn.cursor()
        cursor.fast_executemany = True
        # 逐行逐列转换：numpy NaN/None → Python None，确保 pyodbc 正确识别 NULL
        rows = [
            tuple(None if pd.isna(v) else (int(v) if isinstance(v, np.integer) else v) for v in row)
            for row in df.itertuples(index=False, name=None)
        ]
        total = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(sql, batch)
            total += len(batch)

        return total

    def _upsert(self, df: pd.DataFrame, table: str, keys: list[str], batch_size: int) -> int:
        """MERGE 去重：使用临时表 + 单次 MERGE，避免逐行 MERGE 的性能陷阱"""
        import time
        full_table = f"[{self._schema}].[{table}]"
        tmp_table = f"#_upsert_{int(time.time() * 1000)}"
        columns = list(df.columns)
        updates = [c for c in columns if c not in keys]
        key_list = " AND ".join(f"target.[{k}] = source.[{k}]" for k in keys)
        set_list = ", ".join(f"target.[{c}] = source.[{c}]" for c in updates)
        insert_cols = ", ".join(f"[{c}]" for c in columns)
        source_cols = ", ".join(f"source.[{c}]" for c in columns)
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)

        cursor = self._conn.cursor()
        cursor.fast_executemany = True

        # 1. 建临时表（需要类型信息）
        col_defs = ", ".join(
            f"[{c}] {self._infer_sql_type(df[c], c)}" for c in columns
        )
        cursor.execute(f"CREATE TABLE {tmp_table} ({col_defs})")
        self._conn.commit()

        # 2. 批量 INSERT 到临时表
        rows = [
            tuple(None if pd.isna(v) else (int(v) if isinstance(v, np.integer) else v) for v in row)
            for row in df.itertuples(index=False, name=None)
        ]
        insert_sql = f"INSERT INTO {tmp_table} ({col_list}) VALUES ({placeholders})"
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)

        # 3. 单次 MERGE（比逐行 MERGE 快 25x+）
        merge_sql = (
            f"MERGE INTO {full_table} AS target "
            f"USING {tmp_table} AS source "
            f"ON {key_list} "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols});"
        )
        cursor.execute(merge_sql)
        total = cursor.rowcount if cursor.rowcount >= 0 else len(df)

        # 4. 清理临时表
        cursor.execute(f"DROP TABLE {tmp_table}")
        self._conn.commit()

        return total

    @staticmethod
    def _infer_sql_type(series: pd.Series, col_name: str | None = None) -> str:
        """从 pandas Series 推断 SQL Server 类型"""
        dtype_str = str(series.dtype).lower()

        # 列名包含 date/date 且值为日期格式 → DATETIME2
        if col_name and "date" in col_name.lower():
            try:
                sample = series.dropna().iloc[0] if not series.dropna().empty else None
                if sample and hasattr(sample, "strftime"):
                    return "DATETIME2"
                if sample and isinstance(sample, str) and len(sample) == 8 and sample.isdigit():
                    return "DATETIME2"
            except (IndexError, AttributeError):
                pass

        # 精确匹配
        if dtype_str in _DTYPE_MAP:
            # NVARCHAR(MAX) 不能做主键，需要更精确
            if _DTYPE_MAP[dtype_str] == "NVARCHAR(MAX)" and col_name:
                try:
                    max_len = series.dropna().astype(str).str.len().max()
                    if pd.notna(max_len) and max_len > 0:
                        return f"NVARCHAR({min(int(max_len * 2), 4000)})"
                except Exception:
                    pass
                # fallback: PK-friendly 默认长度
                return "NVARCHAR(255)"
            return _DTYPE_MAP[dtype_str]

        # 近似匹配
        if dtype_str.startswith("int"):
            return "BIGINT"
        if dtype_str.startswith("float"):
            return "FLOAT"
        if dtype_str in ("object", "string", "category") or dtype_str.startswith("str"):
            # 检查实际内容的长度
            max_len = series.dropna().astype(str).str.len().max()
            if pd.isna(max_len) or max_len <= 0:
                return "NVARCHAR(255)"  # PK-friendly 默认
            if max_len <= 10:
                return f"NVARCHAR({max(max_len * 2, 20)})"
            if max_len <= 255:
                return f"NVARCHAR({max(max_len * 2, 50)})"
            if col_name and max_len <= 1000:
                return f"NVARCHAR({min(int(max_len * 2), 4000)})"
            return "NVARCHAR(MAX)"
        if "datetime" in dtype_str:
            return "DATETIME2"
        if dtype_str == "bool":
            return "BIT"

        return "NVARCHAR(MAX)"   # fallback

