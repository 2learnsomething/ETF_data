# etf_data.bridge — Consumer-facing Bridge API

Provides a unified read interface for ETF_Rotation_Strategy (and other consumers). All reads go through local Parquet — no network calls, millisecond response.

## Class: `AmazingDataBridge`

```python
from etf_data.bridge import AmazingDataBridge

bridge = AmazingDataBridge(data_root="/mnt/etf_data")

# ETF list
etfs = bridge.get_etf_list()

# Single ETF daily K-line (last 250 trading days)
df = bridge.get_etf_daily("510050.SH", days=250)

# All ETFs close price matrix (for technical_score.py)
close = bridge.get_all_close(days=60)

# Batch K-line (for DataFetcherBase compatibility)
batch = bridge.get_kline_batch(
    symbols=["510050.SH", "510300.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)

# Minute data
min1 = bridge.get_minute_data("510050.SH", date="2024-01-15", period="1min")

# Cached code info (limit-up/down prices, tick size)
info = bridge.get_code_info_cached()
```

## Empty State

If Parquet files don't exist yet, all methods return empty DataFrame / dict — never raises FileNotFoundError.
