"""
tests/unit/test_uat_engine.py
Unit tests for the UAT engine test cases.
Tests the tester — ensures each test case correctly
identifies good and bad data.

Run with: pytest tests/unit/ -v
"""

import pytest
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.uat.test_engine import UATEngine
from config.test_cases import EXPECTED_TICKERS


# ── Fixtures ──────────────────────────────────────────────────

def clean_staging(n_tickers=15, rows=300):
    """Generate a perfectly clean staging dataset."""
    np.random.seed(42)
    dfs = []
    for i, ticker in enumerate(EXPECTED_TICKERS[:n_tickers]):
        dates  = pd.bdate_range(end="2025-01-01", periods=rows)
        base   = 1000 + i * 200
        prices = base + np.cumsum(np.random.randn(rows) * base * 0.005)
        prices = np.maximum(prices, 1.0)
        close  = pd.Series(prices)
        dfs.append(pd.DataFrame({
            "ticker":             ticker,
            "price_date":         dates,
            "open":               (prices * 0.995).round(4),
            "high":               (prices * 1.01).round(4),
            "low":                (prices * 0.99).round(4),
            "close":              prices.round(4),
            "volume":             np.random.randint(1_000_000, 5_000_000, rows),
            "daily_return_pct":   close.pct_change().mul(100).round(4),
            "sma_20":             close.rolling(20).mean().round(4),
            "sma_50":             close.rolling(50).mean().round(4),
            "volatility_30d":     (close.pct_change().rolling(30).std().mul(np.sqrt(252)*100)).round(4),
            "rsi_14":             pd.Series(np.random.uniform(30, 70, rows)).round(2),
            "drawdown_pct":       pd.Series(np.random.uniform(-10, 0, rows)).round(4),
            "cumulative_return_pct": ((close / close.iloc[0] - 1) * 100).round(4),
            "indexed_price":      (close / close.iloc[0] * 100).round(4),
        }))
    return pd.concat(dfs, ignore_index=True)


def clean_dim_asset():
    asset_map = {
        "^GSPC":"Equity","^IXIC":"Equity","^FTSE":"Equity","^N225":"Equity","^GDAXI":"Equity",
        "^TNX":"Bond","^TYX":"Bond",
        "GC=F":"Commodity","SI=F":"Commodity","CL=F":"Commodity",
        "BTC-USD":"Crypto","ETH-USD":"Crypto",
        "EURUSD=X":"FX","GBPUSD=X":"FX","JPY=X":"FX",
    }
    return pd.DataFrame([{"ticker":t,"asset_class":c,"is_active":1} for t,c in asset_map.items()])


# ── TC-001: All tickers present ───────────────────────────────
def test_tc001_passes_with_all_tickers():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-001")
    assert r.status == "PASS"

def test_tc001_fails_with_missing_ticker():
    df  = clean_staging()
    df  = df[df["ticker"] != "^FTSE"]
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-001")
    assert r.status == "FAIL"
    assert "^FTSE" in r.affected


# ── TC-003: Min row count ─────────────────────────────────────
def test_tc003_passes_with_enough_rows():
    df  = clean_staging(rows=300)
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-003")
    assert r.status == "PASS"

def test_tc003_fails_with_few_rows():
    df  = clean_staging(rows=100)
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-003")
    assert r.status == "FAIL"


# ── TC-004: No null close ─────────────────────────────────────
def test_tc004_passes_with_no_nulls():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-004")
    assert r.status == "PASS"

def test_tc004_fails_with_null_close():
    df  = clean_staging()
    df.loc[df["ticker"] == "^GSPC", "close"] = np.nan
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-004")
    assert r.status == "FAIL"
    assert "^GSPC" in r.affected


# ── TC-006: Price positive ────────────────────────────────────
def test_tc006_passes_with_positive_prices():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-006")
    assert r.status == "PASS"

def test_tc006_fails_with_zero_price():
    df  = clean_staging()
    df.loc[df.index[10], "close"] = 0
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-006")
    assert r.status == "FAIL"


# ── TC-007: Daily return bounds ───────────────────────────────
def test_tc007_passes_with_normal_returns():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-007")
    assert r.status == "PASS"

def test_tc007_fails_with_extreme_return():
    df  = clean_staging()
    df.loc[df.index[5], "daily_return_pct"] = 99.0
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-007")
    assert r.status == "FAIL"


# ── TC-009: High >= Low ───────────────────────────────────────
def test_tc009_passes_with_valid_hloc():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-009")
    assert r.status == "PASS"

def test_tc009_fails_when_high_less_than_low():
    df  = clean_staging()
    df.loc[df.index[0], "high"] = 100
    df.loc[df.index[0], "low"]  = 200  # low > high
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-009")
    assert r.status == "FAIL"


# ── TC-011: No duplicates ─────────────────────────────────────
def test_tc011_passes_with_no_dupes():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-011")
    assert r.status == "PASS"

def test_tc011_fails_with_duplicate_rows():
    df   = clean_staging()
    dupe = df[df["ticker"] == "^GSPC"].head(3).copy()
    df   = pd.concat([df, dupe], ignore_index=True)
    eng  = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-011")
    assert r.status == "FAIL"


# ── TC-017: Referential integrity ────────────────────────────
def test_tc017_passes_with_matching_dim():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-017")
    assert r.status == "PASS"

def test_tc017_fails_with_orphan_ticker():
    df  = clean_staging()
    orphan = df[df["ticker"] == "^GSPC"].head(5).copy()
    orphan["ticker"] = "FAKE_TICKER"
    df  = pd.concat([df, orphan], ignore_index=True)
    eng = UATEngine(df, clean_dim_asset())
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-017")
    assert r.status == "FAIL"
    assert "FAKE_TICKER" in r.affected


# ── TC-020: Regression row count ─────────────────────────────
def test_tc020_passes_when_staging_has_enough_rows():
    staging = clean_staging(rows=300)
    prod    = clean_staging(rows=300)
    eng     = UATEngine(staging, clean_dim_asset(), prod)
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-020")
    assert r.status == "PASS"

def test_tc020_fails_when_staging_loses_rows():
    staging = clean_staging(rows=100)   # much fewer than prod
    prod    = clean_staging(rows=300)
    eng     = UATEngine(staging, clean_dim_asset(), prod)
    eng.run_all()
    r = next(x for x in eng.results if x.test_id == "TC-020")
    assert r.status == "FAIL"


# ── Full run: clean data should pass ─────────────────────────
def test_full_run_clean_data_no_critical_failures():
    df  = clean_staging()
    eng = UATEngine(df, clean_dim_asset(), clean_staging())
    eng.run_all()
    critical_fails = [r for r in eng.results if r.status == "FAIL" and r.sla_critical]
    assert len(critical_fails) == 0, \
        f"Clean data should have zero critical failures. Got: {[r.test_id for r in critical_fails]}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
