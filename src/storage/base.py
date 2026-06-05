"""
存储后端抽象接口

所有存储后端 (SQL Server, MySQL, Parquet, CSV 等) 必须实现 BaseStorage。
"""
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class BaseStorage(ABC):
    """存储后端抽象基类"""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """后端名称，如 'sqlserver', 'parquet', 'mysql'"""
        ...

    @abstractmethod
    def connect(self) -> None:
        """建立连接"""
        ...

    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append",   # append / replace / fail
        **kwargs,
    ) -> int:
        """
        写入数据。

        Args:
            df: 要写入的 DataFrame
            table: 目标表名/路径
            if_exists: append(追加) / replace(替换) / fail(已存在则抛错)

        Returns:
            int: 写入行数
        """
        ...

    @abstractmethod
    def read(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取数据。

        Args:
            table: 表名/路径
            columns: 要读取的列
            where: SQL WHERE 条件或过滤表达式
            limit: 最多返回行数

        Returns:
            pd.DataFrame
        """
        ...

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """检查表/文件是否存在"""
        ...

    @abstractmethod
    def list_tables(self) -> list[str]:
        """列出所有表/数据集"""
        ...

    def close(self) -> None:
        """关闭连接"""
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
