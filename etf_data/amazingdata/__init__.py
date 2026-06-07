"""AmazingData 数据管道 — 数据采集、存储、回填

基于 AmazingData SDK 的全量数据采集管道，支持：
- 多源自动降级（AmazingData → efinance → baostock → akshare）
- Parquet 分区存储（年 + symbol 分文件）
- 全量回填 / 增量更新 / 缺失补拉
- 数据一致性检查 + 报警
"""

from .config import Config
from .fetcher import UnifiedFetcher
from .storage import ParquetStore, MetaStore


def get_client(*args, **kwargs):
    """延迟导入 AmazingDataClient（避免依赖缺失时炸裂）。"""
    from .client import AmazingDataClient
    return AmazingDataClient(*args, **kwargs)


__all__ = [
    "Config",
    "UnifiedFetcher",
    "ParquetStore",
    "MetaStore",
    "get_client",
]
