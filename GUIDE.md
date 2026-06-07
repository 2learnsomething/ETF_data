# ETF Data Pipeline — 使用指南

> 从安装到全量回填到消费，一步步带你用起来

---

## 一、安装

```bash
# 1. 克隆
git clone https://github.com/2learnsomething/ETF_data.git
cd ETF_data

# 2. 安装依赖（推荐用 venv）
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 可选依赖

```bash
# 旧管道（Tushare / SQL Server）— 已废弃，仅保留代码参考
pip install -e .[old]

# 全部
pip install -e .[full]
```

---

## 二、配置

### 2.1 AmazingData 账号

在 `config/amazingdata_pipeline.yaml` 或 `.env` 中配置登录信息：

```yaml
# config/amazingdata_pipeline.yaml
amazingdata:
  username: "${AMAZINGDATA_USERNAME}"   # 或写死账号
  password: "${AMAZINGDATA_PASSWORD}"   # 或写死密码
  host: "120.86.124.106"
  port: 8600
```

`${VAR}` 语法会从 `.env` 或环境变量注入。优先顺序：环境变量 > `.env` > YAML 文件 > 代码默认值。

### 2.2 存储路径

```yaml
# config/amazingdata_pipeline.yaml
storage:
  root: "/mnt/etf_data"       # 生产数据
  # root: "~/data/test_parquet"  # 测试数据（切换至此即可）
```

### 2.3 备源降级开关

```yaml
# config/amazingdata_pipeline.yaml
fallback:
  enabled: true               # 启用备源降级（主源 AmazingData 断开时自动切换）
  prefer_efinance: true       # ETF K线备源（东方财富封装）
  prefer_baostock: true       # A股日线/复权备源 + 交易日历（推荐开）
  prefer_akshare: true        # 基本面/行业/龙虎榜备源
```

当 AmazingData 无权限或网络不可达时，降级顺序：efinance → baostock → akshare。

---

## 三、开发测试（无权限/无硬盘时）

在等待 AmazingData 权限和 1.5TB 硬盘期间，用合成数据跑完全链路。

### 3.1 生成合成数据

```bash
# ETF 日K线 + 代码信息（50只，2023-2025年）
python scripts/generate_test_data.py --mode core --etf-count 50 --years 2023-2025

# 分钟K线（10只，30天）
python scripts/generate_test_data.py --mode minute --etf-count 10 --days 30

# 指数 + A股日K线
python scripts/generate_test_data.py --mode index-stock --years 2023-2025
```

数据生成到 `~/data/test_parquet/`。

### 3.2 验证数据

```bash
ls ~/data/test_parquet/parquet/etf_daily/year=2024/ | head -5
ls ~/data/test_parquet/meta/
```

```python
import pandas as pd
df = pd.read_parquet("~/data/test_parquet/parquet/etf_daily/year=2024/symbol=510050.parquet")
print(df.columns.tolist())
print(len(df))
```

预期输出：
```
['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg', 'adj_close']
~250
```

### 3.3 切换配置到测试数据

```bash
# 改 config/amazingdata_pipeline.yaml 的 storage.root
sed -i 's|/mnt/etf_data|~/data/test_parquet|' config/amazingdata_pipeline.yaml
```

---

## 四、使用 Python API

### 4.1 基础使用

```python
from etf_data.amazingdata import Config, ParquetStore, MetaStore, UnifiedFetcher

# 读配置
cfg = Config()

# 拉取 ETF 日K线
fetcher = UnifiedFetcher()
df = fetcher.fetch_kline(symbol="510050.SH", period="day")
print(f"Fetched {len(df)} rows for 510050.SH")
```

### 4.2 全量回填

```python
from etf_data.amazingdata.pipeline import Pipeline

pipeline = Pipeline(mode="backfill")
pipeline.run()
```

### 4.3 增量更新

```python
from etf_data.amazingdata.pipeline import Pipeline

pipeline = Pipeline(mode="incremental")
pipeline.run()
```

### 4.4 通过桥接层消费

```python
from etf_data.bridge import AmazingDataBridge

bridge = AmazingDataBridge(data_root="~/data/test_parquet")

# 获取全市场 ETF 清单
etfs = bridge.get_etf_list()
print(f"Total ETFs: {len(etfs)}")

# 单只 ETF 近一年日线
df = bridge.get_etf_daily("510050.SH", days=250)
print(df[["trade_date", "close_price", "volume"]].tail())

# 全市场收盘价矩阵
close = bridge.get_all_close(days=60)
print(close.shape)

# 分钟数据
min1 = bridge.get_minute_data("510050.SH", date="2024-01-15", period="1min")
print(min1.head())
```

---

## 五、对接 ETF_Rotation_Strategy

### 5.1 数据流

```
ETF_data 管道
    ↓ Parquet 写入
/mnt/etf_data/parquet/etf_daily/year=2024/symbol=510050.parquet
    ↓ bridge 读取
数据格式: code, kline_time, open, high, low, close, volume, amount
    ↓ _standardize_columns() 映射
数据格式: symbol, trade_date, open_price, high_price, low_price, close_price, volume, amount
    ↓ ETF_Rotation_Strategy 因子计算 / 回测
```

### 5.2 配置对接

在 `ETF_Rotation_Strategy/etf_rotation_config.yaml` 中：

```yaml
data_source:
  type: "parquet"
  parquet:
    root: "/mnt/etf_data"
```

`ETF_data` 和 `ETF_Rotation_Strategy` 共用同一数据目录。数据由 ETF_data 的管道写入，ETF_Rotation 的 `AmazingDataFetcher` 消费。

---

## 六、数据覆盖范围

### 6.1 40 次 API 调用 → 36 个 Parquet 数据集

| Priority | 数据 | 数量 |
|:--------:|------|:----:|
| **P0** | ETF 日K线 + 1分钟K线 + 清单 + 代码信息 + 交易日历 | 核心 |
| **P1** | ETF 净值/份额/复权、指数日K线/分钟K线/成分股、A股日K线/复权、基本面 | 重要 |
| **P2** | 行业分类、ETF PCF、事件数据（大宗/龙虎榜/分红...） | 补充 |

### 6.2 资产范围

| 资产 | 数量 | 数据起点 |
|------|:----:|:--------:|
| ETF | 1,541 只 | 2013年 |
| 指数 | 621 只 | 2013年 |
| A股 | 5,525 只 | 2013年 |
| 行业 | 按 AmazingData 分类 | — |

存储估算：~152 GB（zstd 压缩 Parquet）

---

## 七、常见问题

### Q: `get_calendar()` 报错？
A: AmazingData SDK 的 `get_calendar()` 有 Bug（返回 None），用 baostock 替代。已在 `client.py` 中自动处理，无需手动干预。

### Q: 没有 AmazingData 权限怎么办？
A: 用合成数据做开发测试（见第三章），或切换到备源（efinance / baostock / akshare）。

### Q: 硬盘不够怎么办？
A: 先用 `~/data/test_parquet/` 做测试，等 1.5TB 硬盘到位后在 `/mnt/etf_data/` 跑全量回填。

### Q: 怎么迁移到新机器？
```bash
# 在旧机器上
pip install etf-data-pipeline
rsync -av /mnt/etf_data/ user@new-host:/mnt/etf_data/

# 在新机器上
pip install etf-data-pipeline
# 配置 -> 直接跑回测
```
