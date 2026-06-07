"""快速测试：检查 amazingdata 回填管道关键路径"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from etf_data.amazingdata.client import AmazingDataClient
from etf_data.amazingdata.fetcher import UnifiedFetcher
from etf_data.amazingdata.config import Config
from etf_data.amazingdata.storage import ParquetStore, MetaStore

cfg = Config()
client = AmazingDataClient()
fetcher = UnifiedFetcher()
store = ParquetStore(cfg.storage_root)
meta = MetaStore(cfg.storage_root)

# Step 1: Login
print("=== Step 1: Login ===")
ok = client.login()
print(f"Login OK: {ok}, Available: {client.is_available}")
print()

# Step 2: Get ETF code list (via AmazingData BaseData - has permission)
print("=== Step 2: ETF Code List ===")
codes = client.get_base_data().get_code_list(security_type="EXTRA_ETF")
print(f"ETF codes: {len(codes)}")
if codes:
    print(f"  Sample: {codes[:5]}")
print()

# Step 3: Test fetch_kline via efinance fallback (3 ETFs, 30 days)
print("=== Step 3: Fetch Kline (efinance fallback) ===")
test_codes = ["510050.SH", "510300.SH", "510500.SH"]
print(f"Testing with: {test_codes}")
print(f"Period: day, Range: 20260501 ~ 20260606")
try:
    result = fetcher.fetch_kline(test_codes, "20260501", "20260606", period="day")
    if result:
        for sym, df in result.items():
            print(f"  {sym}: {len(df)} rows")
            if not df.empty:
                cols = [c for c in df.columns if c != 'code']
                print(f"    columns: {cols}")
                print(f"    range: {df['kline_time'].min()} ~ {df['kline_time'].max()}")
    else:
        print("  No data - efinance may be unreachable or returned empty")
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")
print()

# Step 4: Test minute kline
print("=== Step 4: Fetch 1min Kline (efinance fallback) ===")
try:
    result = fetcher.fetch_kline(["510050.SH"], "20260605", "20260606", period="1min")
    if result:
        for sym, df in result.items():
            print(f"  {sym}: {len(df)} rows")
    else:
        print("  No minute data returned")
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")
print()

# Step 5: Test fund_nav fallback (akshare)
print("=== Step 5: Fetch Fund NAV (akshare fallback) ===")
try:
    nav = fetcher.fetch_fund_nav(["510050.SH"])
    if nav:
        for sym, df in nav.items():
            print(f"  {sym}: {len(df)} rows, cols={list(df.columns) if hasattr(df,'columns') else 'N/A'}")
    else:
        print("  No NAV data returned")
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")
print()

# Step 6: Check Parquet storage path
print("=== Step 6: Storage Path ===")
print(f"Storage root: {cfg.storage_root}")
print(f"Root exists: {Path(cfg.storage_root).exists()}")
print()

print("=== Test Complete ===")
