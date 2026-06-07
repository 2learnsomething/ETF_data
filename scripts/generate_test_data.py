#!/usr/bin/env python3
"""生成合成测试数据

按 AmazingData 手册格式生成模拟 K 线数据，用于全链路开发测试。

用法:
    python scripts/generate_test_data.py --mode core --etf-count 50 --years 2023-2025
    python scripts/generate_test_data.py --mode minute --etf-count 10 --days 30
"""

from __future__ import annotations

import argparse
import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── 真实 ETF 代码（前 50 只常用） ─────────────────────────
REAL_ETF_CODES = [
    "510050.SH", "510300.SH", "510500.SH", "510310.SH", "159919.SZ",
    "159915.SZ", "159845.SZ", "588000.SH", "588080.SH", "512880.SH",
    "512100.SH", "512010.SH", "512690.SH", "512800.SH", "512660.SH",
    "512170.SH", "159995.SZ", "516510.SH", "516160.SH", "511010.SH",
    "511260.SH", "511380.SH", "518880.SH", "159980.SZ", "513100.SH",
    "159920.SZ", "513050.SH", "159941.SZ", "511880.SH", "510880.SH",
]

REAL_INDEX_CODES = [
    "000001.SH", "000300.SH", "000905.SH", "000852.SH", "000016.SH",
    "000688.SH", "399001.SZ", "399006.SZ", "399673.SZ",
]

REAL_STOCK_CODES = [
    "600519.SH", "000858.SZ", "601318.SH", "600036.SH", "000333.SZ",
    "601166.SH", "600900.SH", "002415.SZ", "600276.SH", "000651.SZ",
]


def generate_kline(
    symbol: str,
    start_date: str,
    end_date: str,
    base_price: float | None = None,
    volatility: float = 0.015,
    seed: int = 42,
) -> pd.DataFrame:
    """生成日K线

    Args:
        symbol: 标的代码
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        base_price: 基准价，None 则随机
        volatility: 日波动率
        seed: 随机种子

    Returns:
        DataFrame[code, kline_time, open, high, low, close, volume, amount]
    """
    rng = np.random.default_rng(seed + hash(symbol) % 10000)
    dates = pd.bdate_range(start_date, end_date)
    n = len(dates)

    if base_price is None:
        price = 1.0 + rng.random() * 4  # 1~5 元
    else:
        price = float(base_price)

    # 随机游走生成 close
    returns = rng.normal(0, volatility, n)
    closes = price * np.exp(np.cumsum(returns))
    closes = np.maximum(closes, 0.01)  # 不低于 0.01

    opens = closes * np.exp(rng.normal(0, volatility * 0.3, n))
    highs = np.maximum(opens, closes) * (1 + rng.random(n) * volatility)
    lows = np.minimum(opens, closes) * (1 - rng.random(n) * volatility * 0.5)
    volumes = rng.integers(1_000_000, 100_000_000, n)
    amounts = volumes * (opens + closes) / 2

    df = pd.DataFrame({
        "code": symbol,
        "kline_time": [d.strftime("%Y-%m-%d 15:00:00") for d in dates],
        "open": opens.round(4),
        "high": highs.round(4),
        "low": np.maximum(lows.round(4), 0.001),
        "close": closes.round(4),
        "volume": volumes,
        "amount": amounts.round(2),
    })
    df["kline_time"] = pd.to_datetime(df["kline_time"])
    return df


def generate_minute_kline(
    symbol: str,
    date: str,
    daily_open: float,
    daily_close: float,
    daily_high: float,
    daily_low: float,
    daily_volume: int,
    seed: int = 42,
    period: str = "1min",
) -> pd.DataFrame:
    """根据日线拆解生成分钟K线

    Args:
        symbol: 标的代码
        date: 日期 YYYY-MM-DD
        daily_open/high/low/close/volume: 日线数据
        period: '1min' 或 '5min'

    Returns:
        DataFrame[code, kline_time, open, high, low, close, volume, amount]
    """
    rng = np.random.default_rng(seed + hash(symbol) % 10000 + hash(date) % 10000)

    if period == "1min":
        n_bars = 240
        freq = "1min"
    else:
        n_bars = 48
        freq = "5min"

    # 日内走势模拟：U型
    t = np.linspace(0, 1, n_bars)
    trend = 0.3 * np.sin(t * np.pi) + 0.3 * np.sin(t * 2 * np.pi)
    trend = (trend - trend.min()) / (trend.max() - trend.min())
    prices = daily_open + (daily_close - daily_open) * trend
    prices += rng.normal(0, (daily_high - daily_low) * 0.05, n_bars)
    prices = np.clip(prices, daily_low * 0.99, daily_high * 1.01)

    opens = prices[:-1]
    closes = prices[1:]
    highs = np.maximum(opens, closes) * (1 + rng.random(n_bars - 1) * 0.002)
    lows = np.minimum(opens, closes) * (1 - rng.random(n_bars - 1) * 0.002)
    vol = daily_volume // n_bars
    volumes = rng.integers(max(vol // 2, 1000), vol * 2, n_bars - 1)
    amounts = volumes * (opens + closes) / 2

    times = pd.date_range(f"{date} 09:30:00", periods=n_bars, freq=freq)

    df = pd.DataFrame({
        "code": symbol,
        "kline_time": times[:-1],
        "open": opens.round(4),
        "high": highs.round(4),
        "low": np.maximum(lows.round(4), 0.001),
        "close": closes.round(4),
        "volume": volumes,
        "amount": amounts.round(2),
    })
    return df


def generate_code_info(symbols: list[str]) -> pd.DataFrame:
    """生成代码信息（涨跌停价、昨收）"""
    records = []
    for s in symbols:
        pre_close = 1.0 + random.random() * 4
        price_tick = 0.001
        records.append({
            "code": s,
            "symbol": f"ETF_{s[:6]}",
            "security_status": "L",
            "pre_close": round(pre_close, 3),
            "high_limited": round(round(pre_close * 1.1 / price_tick) * price_tick, 3),
            "low_limited": round(round(pre_close * 0.9 / price_tick) * price_tick, 3),
            "price_tick": price_tick,
        })
    return pd.DataFrame(records)


# ── 主入口 ─────────────────────────────────────────────────

def cmd_core(args: argparse.Namespace) -> None:
    """生成核心数据：ETF日K线 + 代码信息"""
    output = Path(args.output)
    n_etf = args.etf_count
    years = args.years

    codes = REAL_ETF_CODES[:n_etf]
    start = f"{years.split('-')[0]}-01-01"
    end = f"{years.split('-')[1]}-12-31"

    logger.info(f"Generating {n_etf} ETFs, {years} ({start} ~ {end})")

    # ETF 清单
    universe = pd.DataFrame({
        "code": codes,
        "name": [f"ETF_{c[:6]}" for c in codes],
        "type": "ETF",
        "list_date": start,
    })
    meta_dir = output / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    universe.to_parquet(meta_dir / "etf_universe.parquet", index=False)

    # 代码信息
    code_info = generate_code_info(codes)
    code_info.to_parquet(meta_dir / "code_info.parquet", index=False)

    # 日K线
    store = output / "parquet"
    for code in codes:
        df = generate_kline(code, start, end)
        y = pd.to_datetime(df["kline_time"].iloc[0]).year
        sym_dir = store / "etf_daily" / f"year={y}"
        sym_dir.mkdir(parents=True, exist_ok=True)
        # 改为按 year 分目录，所有 symbol 写入同一目录
        df.to_parquet(sym_dir / f"{code}.parquet", compression="zstd", index=False)
        logger.debug(f"  {code}: {len(df)} rows")

    total = n_etf * pd.bdate_range(start, end).nunique()
    logger.info(f"Done: {n_etf} ETFs × ~{total // n_etf} days = ~{total} rows")


def cmd_minute(args: argparse.Namespace) -> None:
    """生成分钟K线"""
    output = Path(args.output)
    n_etf = args.etf_count
    ndays = args.days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=ndays * 2)

    codes = REAL_ETF_CODES[:n_etf]

    # 先生成日线作为分钟线的基础
    daily_data: dict[str, pd.DataFrame] = {}
    for code in codes:
        daily_data[code] = generate_kline(
            code, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

    store = output / "parquet"
    for code in codes:
        daily = daily_data[code]
        for _, row in daily.iterrows():
            date_str = pd.Timestamp(row["kline_time"]).strftime("%Y-%m-%d")
            df_min = generate_minute_kline(
                code, date_str,
                daily_open=row["open"],
                daily_close=row["close"],
                daily_high=row["high"],
                daily_low=row["low"],
                daily_volume=row["volume"],
                period="1min",
            )
            if df_min.empty:
                continue
            dt = df_min["kline_time"].iloc[0]
            y, m = dt.year, dt.month
            sym_dir = store / "etf_min1" / f"year={y}" / f"month={m:02d}"
            sym_dir.mkdir(parents=True, exist_ok=True)
            df_min.to_parquet(sym_dir / f"{code}.parquet", compression="zstd", index=False)

        logger.debug(f"  {code}: minute data done")

    logger.info(f"Minute data done: {n_etf} ETFs × {ndays} days")


def main() -> None:
    parser = argparse.ArgumentParser(description="生成合成测试数据")
    parser.add_argument("--output", default=str(Path.home() / "data" / "test_parquet"))
    sub = parser.add_subparsers(dest="mode", required=True)

    p_core = sub.add_parser("core", help="核心：日K线+代码信息")
    p_core.add_argument("--etf-count", type=int, default=30)
    p_core.add_argument("--years", default="2023-2025")

    p_min = sub.add_parser("minute", help="分钟K线")
    p_min.add_argument("--etf-count", type=int, default=10)
    p_min.add_argument("--days", type=int, default=30)

    args = parser.parse_args()

    if args.mode == "core":
        cmd_core(args)
    elif args.mode == "minute":
        cmd_minute(args)


if __name__ == "__main__":
    main()
