"""
pipeline/promote.py
Simulates staging → production promotion workflow.
Only promotes if UAT passes all SLA-critical test cases.

UBS equivalent: production release gating process.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.uat.test_engine import UATEngine
from reports.uat_report    import generate_uat_report
from config.test_cases     import EXPECTED_TICKERS


def generate_staging_data(scenario: str = "clean") -> pd.DataFrame:
    """
    Simulate staging dataset with configurable data quality scenarios.
    In real UBS environment this would be loaded from MySQL staging schema.

    Scenarios:
      clean         — perfect data, should pass all tests
      missing_ticker — one ticker missing
      null_close     — some null close prices
      bad_return     — extreme daily return values
      duplicate_dates — duplicate entries
      stale_data      — frozen prices for one ticker
    """
    np.random.seed(42)
    all_dfs = []

    for i, ticker in enumerate(EXPECTED_TICKERS):
        n = 500
        dates  = pd.bdate_range(end=datetime.today(), periods=n)
        base   = 1000 + i * 500
        prices = base + np.cumsum(np.random.randn(n) * base * 0.01)
        prices = np.maximum(prices, 1.0)

        df = pd.DataFrame({
            "ticker":     ticker,
            "price_date": dates,
            "open":       (prices * np.random.uniform(0.99, 1.0, n)).round(4),
            "high":       (prices * np.random.uniform(1.0, 1.02, n)).round(4),
            "low":        (prices * np.random.uniform(0.98, 1.0, n)).round(4),
            "close":      prices.round(4),
            "volume":     np.random.randint(500_000, 10_000_000, n),
        })

        # Compute analytics
        close = pd.Series(prices)
        df["daily_return_pct"]  = close.pct_change().mul(100).round(4)
        df["sma_20"]            = close.rolling(20).mean().round(4)
        df["sma_50"]            = close.rolling(50).mean().round(4)
        df["sma_200"]           = close.rolling(200).mean().round(4)
        df["volatility_30d"]    = (close.pct_change().rolling(30).std()
                                   .mul(np.sqrt(252)*100).round(4))
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        df["rsi_14"]            = (100 - 100 / (1 + rs)).round(2)
        df["drawdown_pct"]      = ((close / close.cummax() - 1) * 100).round(4)
        df["cumulative_return_pct"] = ((close / close.iloc[0] - 1) * 100).round(4)
        df["indexed_price"]     = (close / close.iloc[0] * 100).round(4)

        all_dfs.append(df)

    staging = pd.concat(all_dfs, ignore_index=True)

    # ── Inject defects per scenario ───────────────────────────
    if scenario == "missing_ticker":
        staging = staging[staging["ticker"] != "^FTSE"]
        print(f"  [SCENARIO] missing_ticker — removed ^FTSE from staging")

    elif scenario == "null_close":
        mask = (staging["ticker"] == "^GSPC") & (np.random.random(len(staging)) < 0.05)
        staging.loc[mask, "close"] = np.nan
        print(f"  [SCENARIO] null_close — injected {mask.sum()} null close prices")

    elif scenario == "bad_return":
        mask = staging["ticker"] == "GC=F"
        idx  = staging[mask].sample(3, random_state=42).index
        staging.loc[idx, "daily_return_pct"] = 75.0
        print(f"  [SCENARIO] bad_return — injected extreme returns for GC=F")

    elif scenario == "duplicate_dates":
        dupes = staging[staging["ticker"] == "^N225"].head(5).copy()
        staging = pd.concat([staging, dupes], ignore_index=True)
        print(f"  [SCENARIO] duplicate_dates — injected 5 duplicate rows for ^N225")

    elif scenario == "stale_data":
        mask   = staging["ticker"] == "BTC-USD"
        idx    = staging[mask].tail(10).index
        frozen = staging.loc[idx[0], "close"]
        staging.loc[idx, "close"] = frozen
        print(f"  [SCENARIO] stale_data — frozen BTC-USD price for last 10 days")

    elif scenario == "clean":
        print(f"  [SCENARIO] clean — no defects injected")

    return staging


def generate_dim_asset() -> pd.DataFrame:
    """Generate dim_asset lookup table."""
    asset_map = {
        "^GSPC":"Equity","^IXIC":"Equity","^FTSE":"Equity","^N225":"Equity","^GDAXI":"Equity",
        "^TNX":"Bond","^TYX":"Bond",
        "GC=F":"Commodity","SI=F":"Commodity","CL=F":"Commodity",
        "BTC-USD":"Crypto","ETH-USD":"Crypto",
        "EURUSD=X":"FX","GBPUSD=X":"FX","JPY=X":"FX",
    }
    return pd.DataFrame([
        {"ticker": t, "asset_class": c, "is_active": 1}
        for t, c in asset_map.items()
    ])


def run_promotion(scenario: str = "clean") -> bool:
    """
    Full UAT + promotion workflow.
    Returns True if promoted to production, False if blocked.
    """
    print(f"\n{'#'*60}")
    print(f"  PRODUCTION PROMOTION WORKFLOW")
    print(f"  Scenario: {scenario.upper()}")
    print(f"  Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    # ── 1. Load staging ───────────────────────────────────────
    print(f"\n  [STEP 1] Loading staging data...")
    staging    = generate_staging_data(scenario)
    dim_asset  = generate_dim_asset()
    production = generate_staging_data("clean")   # simulate existing production
    print(f"  Staging rows: {len(staging)} across {staging['ticker'].nunique()} tickers")

    # ── 2. Run UAT ────────────────────────────────────────────
    print(f"\n  [STEP 2] Running UAT test suite...")
    engine  = UATEngine(staging, dim_asset, production)
    results = engine.run_all()

    # ── 3. Generate sign-off report ───────────────────────────
    print(f"\n  [STEP 3] Generating UAT sign-off report...")
    report_path = generate_uat_report(results, scenario)

    # ── 4. Promotion decision ─────────────────────────────────
    critical_fails = [r for r in results if r.status == "FAIL" and r.sla_critical]
    promoted       = len(critical_fails) == 0

    print(f"\n{'─'*60}")
    if promoted:
        print(f"  ✅  UAT PASSED — Data promoted to PRODUCTION")
        print(f"  Production updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print(f"  ❌  UAT FAILED — Promotion BLOCKED")
        print(f"  Critical failures:")
        for f in critical_fails:
            print(f"    • [{f.test_id}] {f.name}: {f.detail}")
    print(f"  Report: {report_path}")
    print(f"{'─'*60}\n")

    return promoted


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="clean",
        choices=["clean","missing_ticker","null_close","bad_return","duplicate_dates","stale_data"],
        help="Data quality scenario to test")
    args = parser.parse_args()
    run_promotion(args.scenario)
