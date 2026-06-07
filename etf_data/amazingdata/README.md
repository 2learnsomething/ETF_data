# etf_data.amazingdata — AmazingData Data Collection Pipeline

Multi-source data fetcher, storage layer, and pipeline scheduler.

## Modules

| Module | Class | Description |
|--------|-------|-------------|
| `client.py` | `AmazingDataClient` | Singleton login + baostock calendar fallback |
| `config.py` | `Config` | YAML-based config (credentials, paths, fallback toggles) |
| `fetcher.py` | `UnifiedFetcher` | Multi-source routing: AmazingData → efinance → baostock → akshare |
| `storage.py` | `ParquetStore`, `MetaStore` | Partitioned Parquet writer + SQLite audit log |
| `pipeline.py` | `Pipeline` | Backfill / incremental / backfill-missing scheduler |

## Usage

```python
from etf_data.amazingdata import Config, ParquetStore, MetaStore, UnifiedFetcher

# Configure
cfg = Config()

# Fetch data
fetcher = UnifiedFetcher()
klines = fetcher.fetch_kline("510050.SH", period="day")

# Store
store = ParquetStore("/mnt/etf_data")
store.write_kline("etf_daily", klines)
```

## CLI

```bash
python -m etf_data.amazingdata.pipeline --mode backfill
python -m etf_data.amazingdata.pipeline --mode incremental
python -m etf_data.amazingdata.pipeline --mode backfill-missing --table etf_daily --days 7
```
