"""Parquet 存储层 + SQLite 元数据

分区写入，按年+symbol 分文件：
    {root}/parquet/{table}/year={year}/{symbol}.parquet

分钟数据按年月分区：
    {root}/parquet/{table}/year={year}/month={month}/{symbol}.parquet
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd


class ParquetStore:
    """Parquet 分区存储

    写入自动按 year 和 symbol 分区。
    所有写入使用临时文件 + 重命名，保证原子性。
    """

    def __init__(self, root: str | Path = "/mnt/etf_data"):
        self._root = Path(root) / "parquet"

    def write_kline(
        self,
        table: str,
        df: pd.DataFrame,
        year: int,
        partition_by_month: bool = False,
    ) -> int:
        """写入 K 线数据，按 year + symbol 分区

        Args:
            table: 表名，如 'etf_daily'
            df: 数据，必须包含 'symbol' 和 'kline_time' 列
            year: 年份
            partition_by_month: 分钟数据按月再分区

        Returns:
            写入行数
        """
        if df.empty:
            return 0

        total = 0
        for symbol, group in df.groupby("symbol", sort=False):
            if partition_by_month and "kline_time" in group.columns:
                # 分钟数据：按年月分区
                for month, sub in group.groupby(group["kline_time"].dt.month):
                    sym_dir = (
                        self._root
                        / table
                        / f"year={year}"
                        / f"month={month:02d}"
                    )
                    sym_dir.mkdir(parents=True, exist_ok=True)
                    path = sym_dir / f"{symbol}.parquet"
                    total += self._write_single(sub, path)
            else:
                sym_dir = self._root / table / f"year={year}"
                sym_dir.mkdir(parents=True, exist_ok=True)
                path = sym_dir / f"{symbol}.parquet"
                total += self._write_single(group, path)

        return total

    def write_unpartitioned(self, table: str, df: pd.DataFrame) -> int:
        """写入不分区的小表（如 etf_nav, 财务数据等）

        Args:
            table: 表名
            df: 数据

        Returns:
            写入行数
        """
        if df.empty:
            return 0
        table_dir = self._root / table
        table_dir.mkdir(parents=True, exist_ok=True)
        path = table_dir / "data.parquet"
        return self._write_single(df, path)

    def read_kline(
        self,
        table: str,
        symbol: str,
        start_year: int = 2013,
        end_year: int = 2026,
    ) -> pd.DataFrame:
        """读取单只标的指定年份范围的 K 线

        Args:
            table: 表名
            symbol: 标的代码，如 '510050.SH'
            start_year: 起始年份
            end_year: 结束年份

        Returns:
            DataFrame，按 kline_time 升序
        """
        frames: list[pd.DataFrame] = []
        for year in range(start_year, end_year + 1):
            # 尝试日频分区
            path = self._root / table / f"year={year}" / f"{symbol}.parquet"
            if path.exists():
                frames.append(pd.read_parquet(path))
                continue
            # 尝试分钟分区（带 month 子目录）
            month_dir = self._root / table / f"year={year}"
            if month_dir.exists():
                for m_dir in sorted(month_dir.iterdir()):
                    if m_dir.is_dir() and m_dir.name.startswith("month="):
                        mp = m_dir / f"{symbol}.parquet"
                        if mp.exists():
                            frames.append(pd.read_parquet(mp))

        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        if "kline_time" in result.columns:
            result = result.sort_values("kline_time").reset_index(drop=True)
        return result

    def read_unpartitioned(self, table: str) -> pd.DataFrame:
        """读取不分区的小表"""
        path = self._root / table / "data.parquet"
        if path.exists():
            return pd.read_parquet(path)
        return pd.DataFrame()

    @staticmethod
    def _write_single(df: pd.DataFrame, path: Path) -> int:
        """原子写入：临时文件 → 重命名"""
        tmp = path.with_suffix(".tmp")
        df.to_parquet(tmp, compression="zstd", index=False)
        tmp.rename(path)
        return len(df)


class MetaStore:
    """SQLite 元数据存储

    管理：更新日志、ETF 清单缓存、代码信息缓存、交易日历。
    """

    def __init__(self, root: str | Path = "/mnt/etf_data"):
        self._root = Path(root) / "meta"
        self._root.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._root / "meta.db"))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self) -> None:
        """初始化表结构"""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS update_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name    TEXT NOT NULL,
                run_type      TEXT NOT NULL,
                run_date      TEXT NOT NULL,
                data_date     TEXT,
                symbol        TEXT,
                rows_count    INTEGER DEFAULT 0,
                source        TEXT NOT NULL,
                status        TEXT NOT NULL,
                error_msg     TEXT,
                start_time    TEXT NOT NULL,
                end_time      TEXT,
                duration_ms   INTEGER
            );

            CREATE TABLE IF NOT EXISTS cache_meta (
                key           TEXT PRIMARY KEY,
                value         TEXT,
                updated_at    TEXT NOT NULL
            );
        """)
        self._conn.commit()

    def log_update(
        self,
        table_name: str,
        run_type: str,
        source: str,
        status: str,
        rows_count: int = 0,
        data_date: str | None = None,
        symbol: str | None = None,
        error_msg: str | None = None,
    ) -> None:
        """记录一次数据更新"""
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        run_date = now[:10]
        self._conn.execute(
            """INSERT INTO update_log
               (table_name, run_type, run_date, data_date, symbol,
                rows_count, source, status, error_msg, start_time)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                table_name,
                run_type,
                run_date,
                data_date or run_date,
                symbol,
                rows_count,
                source,
                status,
                error_msg,
                now,
            ),
        )
        self._conn.commit()

    def get_last_update(self, table_name: str) -> str | None:
        """获取某张表最后成功更新的日期"""
        cur = self._conn.execute(
            """SELECT MAX(data_date) FROM update_log
               WHERE table_name=? AND status='ok'""",
            (table_name,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_missing_days(self, table_name: str, days: int = 30) -> list[str]:
        """查询最近 N 天中缺失的日期"""
        # 简版：返回所有 data_date 有值的日期中，缺失的交易日
        cur = self._conn.execute(
            """SELECT DISTINCT data_date FROM update_log
               WHERE table_name=? AND status='ok'
               AND data_date >= date('now', '-? days')""",
            (table_name, days),
        )
        existing = {row[0] for row in cur.fetchall()}
        # 这里需要用交易日历来精确判断
        # 简版实现仅返回 None，留待后续完善
        return []

    def cache_get(self, key: str) -> str | None:
        """读取缓存"""
        cur = self._conn.execute(
            "SELECT value FROM cache_meta WHERE key=?", (key,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def cache_set(self, key: str, value: str) -> None:
        """写入缓存"""
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            """INSERT OR REPLACE INTO cache_meta (key, value, updated_at)
               VALUES (?,?,?)""",
            (key, value, now),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
