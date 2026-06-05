# ETF_data — A股 ETF 全维度数据管道

> 多源采集 · YAML 驱动 · SQL Server 存储 · 增量更新 · 交叉验证

---

## 快速开始

```bash
cd ETF_data
pip install -r requirements.txt

# 增量更新
python src/pipeline/run.py --incremental

# 全量回填
python src/scheduler/backfill.py

# 指定任务
python src/pipeline/run.py --tasks etf_daily,etf_nav
```

---

## 数据管道

12 个任务，4 个数据源，30 只 ETFs。每跑一次自动产出 4 份报告：

| 报告 | 命令 | 内容 |
|------|------|------|
| 质量报告 | `src/quality/check.py` | 11表健康状态、新鲜度、重复 |
| 监控看板 | `src/quality/dashboard.py` | HTML 可视化面板 |
| 持仓穿透 | `src/quality/portfolio.py` | ETF重仓股重叠分析 |
| 三方比对 | `src/comparator/report.py` | Tushare vs 新浪 vs 腾讯 |

---

## 数据源

| 源 | 接入 | 状态 |
|----|------|------|
| Tushare Pro | HTTP API | 主力，需token |
| 新浪财经 | akshare / curl | 验证源，100%价格一致 |
| 腾讯财经 | curl | 第三验证源 |
| 同花顺 | akshare | 实时净值 |
| 上证所 | akshare | ETF规模 |

---

## HTTP API 服务

```bash
python src/api/server.py --port 8420
```

| 路径 | 返回 |
|------|------|
| `GET /etfs` | ETF 列表 |
| `GET /daily/510050?days=250` | 日线 |
| `GET /close?days=60` | 收盘价矩阵 |
| `GET /portfolio/510050` | 持仓明细 |
| `GET /calendar?year=2026` | 交易日历 |
| `GET /health` | 健康检查 |
| `GET /quality` | 质量报告 |
| `GET /dashboard` | 监控看板 |

---

## daily_news 桥接

```python
from ETF_data_bridge import get_etf_daily, get_etf_list

etfs = get_etf_list()              # 全部 ETF 列表
df = get_etf_daily("510050", 250)  # 近一年日线
close = get_all_close(60)          # 收盘价矩阵
```

---

## 增量 vs 全量

| 模式 | 说明 |
|------|------|
| `run.py --incremental` | 增量（默认），只拉最近 N 天 |
| `run.py --tasks etf_daily` | 全量拉指定任务 |
| `backfill.py` | 强制全量所有任务 |

---

## 架构

```
config/pipeline_tasks.yaml  →  DataPipeline
                                ├── fetchers/ (4 sources)
                                ├── storage/  (SQL Server + Parquet)
                                ├── validators
                                ├── scheduler/
                                ├── comparator/ (三方比对)
                                └── quality/   (监控 + 持仓)
└→ reports/ (quality | dashboard | portfolio | consistency)
└→ ETF_data_bridge.py → daily_news
```

当前数据量：13 张表，~115,000 行。主键已启用，写入使用 fast_executemany 加速。
