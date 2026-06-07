# ETF Data Pipeline

> A股 ETF 全维度数据管道 — 基于 AmazingData 的数据采集、存储和消费接口

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![PyPI version](https://img.shields.io/badge/version-0.1.0-orange)](https://github.com/2learnsomething/ETF_data)

---

## Overview

ETF Data Pipeline collects full-market ETF, index, and A-share data from **AmazingData** (primary), falling back to efinance / baostock / akshare automatically. Data is stored as **Parquet** files partitioned by year + symbol, then consumed locally by `ETF_Rotation_Strategy` via the bridge layer.

**Total coverage:** ~1,541 ETFs / 621 indices / 5,525 A-shares — ~152 GB (Parquet, zstd).

| Layer | Package | Description |
|-------|---------|-------------|
| Collection | `etf_data.amazingdata` | Multi-source fetcher with auto-fallback |
| Storage | `etf_data.amazingdata.storage` | Parquet partition writer + SQLite metadata |
| Bridge | `etf_data.bridge` | Consumer-facing API for ETF_Rotation_Strategy |
| Pipeline | `etf_data.amazingdata.pipeline` | Backfill / incremental / backfill-missing scheduler |

---

## Quick Start

```bash
# Install
pip install -e .

# Help
python -m etf_data.amazingdata.pipeline --help

# Generate synthetic test data (no AmazingData permissions needed)
python scripts/generate_test_data.py --mode core --etf-count 50 --years 2023-2025

# Point config to test data
# Edit config/etf_data_config.yaml → storage.root: ~/data/test_parquet
```

---

## Data Sources

| Source | Role | Coverage |
|--------|------|----------|
| **AmazingData** (银河证券) | Primary | ETF / Index / A-share full market |
| efinance (东方财富) | Fallback | ETF / Index daily + minute K-line |
| baostock | Fallback | A-share daily + adjustment factors |
| akshare | Fallback | Fundamentals, industry, events |

---

## Project Structure

```
ETF_data/                      ← Installable package root
├── etf_data/                  ← Python package
│   ├── __init__.py
│   ├── amazingdata/           ← New AmazingData pipeline
│   │   ├── client.py          ← Login + baostock calendar fallback
│   │   ├── config.py          ← Config management
│   │   ├── fetcher.py         ← UnifiedFetcher (multi-source routing)
│   │   ├── storage.py         ← ParquetStore + MetaStore (SQLite)
│   │   └── pipeline.py        ← Backfill / incremental scheduler
│   ├── bridge/                ← Consumer-facing API
│   │   ├── data_bridge.py     ← AmazingDataBridge class
│   │   └── __init__.py
│   ├── api/ comparator/       ← [DEPRECATED] Old Tushare pipeline
│   ├── pipeline/ quality/
│   ├── scheduler/ storage/ utils/
├── config/                    ← YAML configuration files
├── scripts/                   ← Utility scripts
│   ├── generate_test_data.py  ← Synthetic data generator
│   └── register_cron.sh       ← Cron job registration
├── pyproject.toml
└── README.md
```

---

## Storage Architecture

```
/mnt/etf_data/                  ← Production data root
└── parquet/
    ├── etf_daily/year=*/symbol=*.parquet
    ├── etf_min1/year=*/month=*/symbol=*.parquet
    ├── etf_min5/year=*/month=*/symbol=*.parquet
    ├── etf_nav/ etf_share/ etf_adj/ etf_pcf/
    ├── index_daily/year=*/index_min1/
    ├── stock_daily/year=*/stock_adj/
    ├── stock_financial/
    ├── industry_*/
    └── ...
├── meta/                      ← Metadata (universe lists, calendar)
│   ├── etf_universe.parquet
│   ├── trade_calendar.parquet
│   └── meta.db (SQLite)
└── bridge/
    └── data_bridge.py
```

Test data goes to `~/data/test_parquet/` — same format, switchable via config.

---

## Pipeline Modes

```bash
# Full backfill (all ETFs, all years)
python -m etf_data.amazingdata.pipeline --mode backfill

# Daily incremental update (run at 15:30 on trading days)
python -m etf_data.amazingdata.pipeline --mode incremental

# Backfill missing days
python -m etf_data.amazingdata.pipeline --mode backfill-missing --table etf_daily --days 7
```

---

## Packages

| Package | Import | Install |
|---------|--------|---------|
| Core pipeline | `from etf_data.amazingdata import ...` | `pip install -e .` |
| With old deps | `from etf_data.scheduler import ...` | `pip install -e .[old]` |
| All deps | — | `pip install -e .[full]` |

---

## Dependencies

- **Python 3.10+** (tested on 3.12)
- **pandas, numpy, pyarrow** — Parquet storage
- **baostock** — trade calendar fallback
- **akshare** — multi-source data fallback
- **pyyaml, python-dotenv** — config
- **AmazingData SDK** — primary data source (installed separately)

---

## Related Projects

- [ETF_Rotation_Strategy](https://github.com/2learnsomething/ETF_Rotation_Strategy) — Barra multi-factor ETF rotation backtest engine (consumes data from this pipeline)
- [daily-news](https://github.com/2learnsomething/daily-news) — Daily A-share market briefing (uses ETF data bridge)
