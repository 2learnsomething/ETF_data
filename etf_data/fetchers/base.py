"""
数据源抽象接口

所有数据源 (Tushare, AKShare, EastMoney 等) 必须实现 BaseFetcher。
返回标准化的 pandas DataFrame。
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class FetchRequest:
    """标准化的数据请求"""
    data_type: str                          # daily / minute / stock_basic / adj_factor 等
    symbols: list[str] = field(default_factory=list)   # ETF/股票代码列表
    start_date: Optional[str] = None        # YYYYMMDD 或 YYYY-MM-DD
    end_date: Optional[str] = None
    fields: Optional[list[str]] = None      # 需要的字段，None=全部
    extra: dict = field(default_factory=dict)  # 数据源特有参数


@dataclass
class FetchResult:
    """标准化的返回结果"""
    source: str                             # 数据源名称
    data_type: str
    df: pd.DataFrame
    metadata: dict = field(default_factory=dict)  # {row_count, date_range, ...}


class BaseFetcher(ABC):
    """数据源抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称，如 'tushare', 'akshare'"""
        ...

    @abstractmethod
    def connect(self) -> None:
        """建立连接 / 初始化认证"""
        ...

    @abstractmethod
    def fetch(self, request: FetchRequest) -> FetchResult:
        """
        执行数据获取。

        Args:
            request: 标准化的数据请求

        Returns:
            FetchResult: 包含 DataFrame 和元信息
        """
        ...

    @abstractmethod
    def available_data_types(self) -> list[str]:
        """返回该数据源支持的数据类型列表"""
        ...

    def normalize_columns(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        列名标准化映射 — 子类可重写。

        将数据源特定列名映射为统一标准名：
          trade_date / date / cal_date  → date
          ts_code / code / symbol       → symbol
          close / close_price           → close
          vol / volume / volume_amount  → volume
        """
        # 通用映射（按 data_type 可细化）
        mapping = {
            "trade_date":  "date",
            "cal_date":    "date",
            "nav_date":    "date",     # fund_nav
            "end_date":    "date",     # fund_nav 备选
            "ann_date":    "date",     # 公告日期
            "ts_code":     "symbol",
            "code":        "symbol",
            "symbol":      "symbol",
            "vol":         "volume",
            "volume_amount": "volume",
            "amount":      "amount",
            "money_amount": "amount",
        }

        # 先 drop 多余的源列（如 nav_date 和 ann_date 都映射到 date，
        # 只保留第一个匹配的，避免 rename 后出现同名列）
        # 同时检查目标列是否已存在于 df 中
        seen_targets: set[str] = set(df.columns)  # 从已有列开始
        cols_to_drop: list[str] = []
        for old, new in mapping.items():
            if old in df.columns and old != new:
                if new in seen_targets:
                    cols_to_drop.append(old)
                else:
                    seen_targets.add(new)
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        rename = {k: v for k, v in mapping.items() if k in df.columns and k != v}
        if rename:
            df = df.rename(columns=rename)
        return df

    def close(self) -> None:
        """关闭连接，子类可选重写"""
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
