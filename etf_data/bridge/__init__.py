"""AmazingData 桥接层 — 面向 ETF_Rotation_Strategy 的 Parquet 数据消费接口

提供与原有 API 兼容的读取接口，所有数据从本地 Parquet 读取。
"""

from .data_bridge import AmazingDataBridge

__all__ = ["AmazingDataBridge"]
