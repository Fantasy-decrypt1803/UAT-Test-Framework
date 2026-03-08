"""
tests/uat/test_engine.py
Core UAT test engine — runs all 22 test cases against staging data.
Produces a result object per test case with PASS/FAIL/WARN status.

UBS equivalent: formal UAT sign-off before every production release.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Optional
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from config.test_cases import (
    UAT_TEST_CASES, EXPECTED_TICKERS, VALID_ASSET_CLASSES,
    MIN_ROWS_PER_TICKER, MAX_DAILY_RETURN_PCT, DATA_FRESHNESS_DAYS,
    STALE_STREAK_DAYS, REGRESSION_ROW_THRESHOLD, PRICE_MATCH_THRESHOLD,
    SMA_ACCURACY_THRESHOLD
)


class TestResult:
    """Represents the outcome of a single UAT test case."""

    def __init__(self, test_id: str, name: str, category: str,
                 severity: str, sla_critical: bool):
        self.test_id      = test_id
        self.name         = name
        self.category     = category
        self.severity     = severity
        self.sla_critical = sla_critical
        self.status       = "NOT RUN"   # PASS / FAIL / WARN / NOT RUN
        self.detail       = ""
        self.affected     = []          # list of affected tickers/rows
        self.ran_at       = None

    def passed(self, detail="All checks passed."):
        self.status  = "PASS"
        self.detail  = detail
        self.ran_at  = datetime.now()
        return self

    def failed(self, detail, affected=None):
        self.status   = "FAIL"
        self.detail   = detail
        self.affected = affected or []
        self.ran_at   = datetime.now()
        return self

    def warned(self, detail, affected=None):
        self.status   = "WARN"
        self.detail   = detail
        self.affected = affected or []
        self.ran_at   = datetime.now()
        return self

    def to_dict(self):
        return {
            "test_id":      self.test_id,
            "name":         self.name,
            "category":     self.category,
            "severity":     self.severity,
            "sla_critical": self.sla_critical,
            "status":       self.status,
            "detail":       self.detail,
            "affected":     ", ".join(str(a) for a in self.affected[:10]),
            "ran_at":       str(self.ran_at),
        }


class UATEngine:
    """
    Runs all UAT test cases against a staging DataFrame.
    Optionally compares against production DataFrame for regression tests.
    """

    def __init__(self,
                 staging_df:    pd.DataFrame,
                 dim_asset_df:  Optional[pd.DataFrame] = None,
                 production_df: Optional[pd.DataFrame] = None):
        self.staging    = staging_df.copy()
        self.dim_asset  = dim_asset_df
        self.production = production_df
        self.results    = []

        # Normalise date column
        if "price_date" in self.staging.columns:
            self.staging["price_date"] = pd.to_datetime(self.staging["price_date"])
        if self.production is not None and "price_date" in self.production.columns:
            self.production["price_date"] = pd.to_datetime(self.production["price_date"])


    # ── Individual test methods ───────────────────────────────

    def _tc001_all_tickers_present(self) -> TestResult:
        r = TestResult("TC-001", "All expected tickers present",
                       "COMPLETENESS", "HIGH", True)
        found   = set(self.staging["ticker"].unique())
        missing = [t for t in EXPECTED_TICKERS if t not in found]
        if missing:
            return r.failed(f"{len(missing)} tickers missing from staging.", missing)
        return r.passed(f"All {len(EXPECTED_TICKERS)} tickers present.")


    def _tc002_no_missing_trading_days(self) -> TestResult:
        r = TestResult("TC-002", "No missing trading days",
                       "COMPLETENESS", "HIGH", True)
        issues = []
        for ticker, grp in self.staging.groupby("ticker"):
            dates     = pd.to_datetime(grp["price_date"]).sort_values()
            biz_range = pd.bdate_range(dates.min(), dates.max())
            coverage  = len(dates) / len(biz_range) * 100
            if coverage < 90:
                issues.append(f"{ticker}: {coverage:.1f}% coverage")
        if issues:
            return r.warned(f"{len(issues)} tickers below 90% trading day coverage.", issues)
        return r.passed("All tickers have >= 90% trading day coverage.")


    def _tc003_min_row_count(self) -> TestResult:
        r = TestResult("TC-003", "Minimum row count per ticker",
                       "COMPLETENESS", "HIGH", True)
        low = []
        for ticker, grp in self.staging.groupby("ticker"):
            if len(grp) < MIN_ROWS_PER_TICKER:
                low.append(f"{ticker}: {len(grp)} rows")
        if low:
            return r.failed(f"{len(low)} tickers below {MIN_ROWS_PER_TICKER} row minimum.", low)
        return r.passed(f"All tickers meet minimum {MIN_ROWS_PER_TICKER} rows.")


    def _tc004_no_null_close(self) -> TestResult:
        r = TestResult("TC-004", "No null close prices",
                       "COMPLETENESS", "HIGH", True)
        col = "close" if "close" in self.staging.columns else "close_price"
        if col not in self.staging.columns:
            return r.failed("Close price column not found in staging data.")
        nulls = self.staging[self.staging[col].isnull()]
        if len(nulls) > 0:
            affected = nulls["ticker"].unique().tolist()
            return r.failed(f"{len(nulls)} null close prices found.", affected)
        return r.passed("Zero null close prices.")


    def _tc005_analytics_populated(self) -> TestResult:
        r = TestResult("TC-005", "All analytics columns populated",
                       "COMPLETENESS", "MEDIUM", False)
        analytics_cols = ["sma_20", "sma_50", "volatility_30d", "rsi_14", "daily_return_pct"]
        missing_cols   = [c for c in analytics_cols if c not in self.staging.columns]
        if missing_cols:
            return r.failed(f"Analytics columns missing entirely: {missing_cols}")

        issues = []
        for col in analytics_cols:
            # Skip first N rows where rolling window is expected to be null
            skip = 30 if "volatility" in col else 20 if "sma_20" in col else 14
            check_df = self.staging.groupby("ticker").apply(lambda g: g.iloc[skip:])
            null_pct  = check_df[col].isnull().mean() * 100
            if null_pct > 10:
                issues.append(f"{col}: {null_pct:.1f}% null after warm-up period")
        if issues:
            return r.warned(f"High null % in analytics columns.", issues)
        return r.passed("All analytics columns sufficiently populated.")


    def _tc006_price_positive(self) -> TestResult:
        r = TestResult("TC-006", "Close price within realistic range",
                       "ACCURACY", "HIGH", True)
        col   = "close" if "close" in self.staging.columns else "close_price"
        bad   = self.staging[self.staging[col] <= 0]
        if len(bad) > 0:
            return r.failed(f"{len(bad)} rows with zero or negative close price.",
                            bad["ticker"].unique().tolist())
        return r.passed("All close prices are positive.")


    def _tc007_daily_return_bounds(self) -> TestResult:
        r = TestResult("TC-007", "Daily return within bounds",
                       "ACCURACY", "HIGH", True)
        if "daily_return_pct" not in self.staging.columns:
            return r.warned("daily_return_pct column not present — skipping check.")
        extreme = self.staging[self.staging["daily_return_pct"].abs() > MAX_DAILY_RETURN_PCT]
        if len(extreme) > 0:
            affected = extreme[["ticker", "price_date", "daily_return_pct"]].head(5).to_dict("records")
            return r.failed(
                f"{len(extreme)} rows with daily return > ±{MAX_DAILY_RETURN_PCT}%.",
                [f"{r['ticker']} {r['price_date']} {r['daily_return_pct']:.2f}%" for r in affected])
        return r.passed(f"All daily returns within ±{MAX_DAILY_RETURN_PCT}% bounds.")


    def _tc008_rsi_range(self) -> TestResult:
        r = TestResult("TC-008", "RSI within valid range",
                       "ACCURACY", "MEDIUM", False)
        if "rsi_14" not in self.staging.columns:
            return r.warned("rsi_14 column not present — skipping.")
        rsi_data = self.staging["rsi_14"].dropna()
        bad      = rsi_data[(rsi_data < 0) | (rsi_data > 100)]
        if len(bad) > 0:
            return r.failed(f"{len(bad)} RSI values outside 0–100 range.")
        return r.passed("All RSI values within valid 0–100 range.")


    def _tc009_high_gte_low(self) -> TestResult:
        r = TestResult("TC-009", "High >= Low for all rows",
                       "ACCURACY", "HIGH", True)
        if "high" not in self.staging.columns or "low" not in self.staging.columns:
            return r.warned("high/low columns not present — skipping.")
        bad = self.staging[self.staging["high"] < self.staging["low"]]
        if len(bad) > 0:
            return r.failed(f"{len(bad)} rows where high < low — data corruption.",
                            bad["ticker"].unique().tolist())
        return r.passed("All rows have high >= low.")


    def _tc010_sma_accuracy(self) -> TestResult:
        r = TestResult("TC-010", "SMA-20 accuracy check",
                       "ACCURACY", "MEDIUM", False)
        if "sma_20" not in self.staging.columns:
            return r.warned("sma_20 column not present — skipping.")
        col   = "close" if "close" in self.staging.columns else "close_price"
        issues = []
        for ticker, grp in self.staging.groupby("ticker"):
            grp = grp.sort_values("price_date").reset_index(drop=True)
            manual_sma = grp[col].rolling(20).mean()
            stored_sma = grp["sma_20"]
            diff = (manual_sma - stored_sma).abs() / manual_sma.replace(0, np.nan)
            bad  = diff[diff > SMA_ACCURACY_THRESHOLD].dropna()
            if len(bad) > 3:
                issues.append(f"{ticker}: {len(bad)} rows with >5% SMA deviation")
        if issues:
            return r.failed("SMA-20 values deviate significantly from manual calculation.", issues)
        return r.passed("SMA-20 values match manual calculation within 5% tolerance.")


    def _tc011_no_duplicates(self) -> TestResult:
        r = TestResult("TC-011", "No duplicate date entries",
                       "ACCURACY", "HIGH", True)
        dupes = self.staging[self.staging.duplicated(subset=["ticker", "price_date"], keep=False)]
        if len(dupes) > 0:
            affected = dupes["ticker"].unique().tolist()
            return r.failed(f"{len(dupes)} duplicate ticker+date rows found.", affected)
        return r.passed("No duplicate ticker+date combinations.")


    def _tc012_data_freshness(self) -> TestResult:
        r = TestResult("TC-012", "Data freshness check",
                       "TIMELINESS", "HIGH", True)
        cutoff  = pd.Timestamp(datetime.today() - timedelta(days=DATA_FRESHNESS_DAYS))
        stale   = []
        for ticker, grp in self.staging.groupby("ticker"):
            latest = grp["price_date"].max()
            if latest < cutoff:
                stale.append(f"{ticker}: latest={latest.date()}")
        if stale:
            return r.failed(
                f"{len(stale)} tickers have data older than {DATA_FRESHNESS_DAYS} business days.", stale)
        return r.passed(f"All tickers have data within last {DATA_FRESHNESS_DAYS} business days.")


    def _tc013_no_stale_streak(self) -> TestResult:
        r = TestResult("TC-013", "No stale data streaks",
                       "TIMELINESS", "MEDIUM", False)
        col    = "close" if "close" in self.staging.columns else "close_price"
        issues = []
        for ticker, grp in self.staging.groupby("ticker"):
            grp    = grp.sort_values("price_date")
            prices = grp[col].tolist()
            streak = 1
            for i in range(1, len(prices)):
                if prices[i] == prices[i-1] and pd.notna(prices[i]):
                    streak += 1
                    if streak >= STALE_STREAK_DAYS:
                        issues.append(f"{ticker}: {streak}-day identical price streak")
                        break
                else:
                    streak = 1
        if issues:
            return r.warned(f"{len(issues)} tickers with stale price streaks.", issues)
        return r.passed("No stale price streaks detected.")


    def _tc014_date_format(self) -> TestResult:
        r = TestResult("TC-014", "Date column is valid date type",
                       "FORMAT", "HIGH", True)
        try:
            pd.to_datetime(self.staging["price_date"])
            return r.passed("price_date column parses correctly as dates.")
        except Exception as e:
            return r.failed(f"price_date column contains invalid date values: {e}")


    def _tc015_numeric_columns(self) -> TestResult:
        r = TestResult("TC-015", "Numeric columns are numeric",
                       "FORMAT", "HIGH", True)
        num_cols = ["open","high","low","close","volume"]
        issues   = []
        for col in num_cols:
            if col in self.staging.columns:
                converted = pd.to_numeric(self.staging[col], errors="coerce")
                bad_count = converted.isnull().sum() - self.staging[col].isnull().sum()
                if bad_count > 0:
                    issues.append(f"{col}: {bad_count} non-numeric values")
        if issues:
            return r.failed("Non-numeric values found in numeric columns.", issues)
        return r.passed("All numeric columns contain valid numeric data.")


    def _tc016_ticker_format(self) -> TestResult:
        r = TestResult("TC-016", "Ticker format is correct",
                       "FORMAT", "MEDIUM", False)
        found    = set(self.staging["ticker"].unique())
        unknown  = [t for t in found if t not in EXPECTED_TICKERS]
        if unknown:
            return r.warned(f"{len(unknown)} unrecognised tickers found.", unknown)
        return r.passed("All tickers match the configured asset universe.")


    def _tc017_referential_integrity(self) -> TestResult:
        r = TestResult("TC-017", "Referential integrity — ticker in dim_asset",
                       "INTEGRITY", "HIGH", True)
        if self.dim_asset is None:
            return r.warned("dim_asset not provided — skipping referential integrity check.")
        fact_tickers  = set(self.staging["ticker"].unique())
        dim_tickers   = set(self.dim_asset["ticker"].unique())
        orphans       = fact_tickers - dim_tickers
        if orphans:
            return r.failed(
                f"{len(orphans)} tickers in fact_prices have no match in dim_asset.", list(orphans))
        return r.passed("All fact_prices tickers exist in dim_asset.")


    def _tc018_volume_non_negative(self) -> TestResult:
        r = TestResult("TC-018", "Volume non-negative",
                       "INTEGRITY", "MEDIUM", False)
        if "volume" not in self.staging.columns:
            return r.warned("volume column not present — skipping.")
        bad = self.staging[self.staging["volume"] < 0]
        if len(bad) > 0:
            return r.failed(f"{len(bad)} rows with negative volume.",
                            bad["ticker"].unique().tolist())
        return r.passed("All volume values are zero or positive.")


    def _tc019_asset_class_valid(self) -> TestResult:
        r = TestResult("TC-019", "Asset class values valid",
                       "INTEGRITY", "MEDIUM", False)
        if self.dim_asset is None:
            return r.warned("dim_asset not provided — skipping.")
        invalid = self.dim_asset[~self.dim_asset["asset_class"].isin(VALID_ASSET_CLASSES)]
        if len(invalid) > 0:
            return r.failed(
                f"{len(invalid)} rows with invalid asset_class values.",
                invalid["asset_class"].unique().tolist())
        return r.passed(f"All asset_class values are valid: {VALID_ASSET_CLASSES}")


    def _tc020_row_count_regression(self) -> TestResult:
        r = TestResult("TC-020", "Row count not decreased vs production",
                       "REGRESSION", "HIGH", True)
        if self.production is None:
            return r.warned("Production data not provided — skipping regression check.")
        issues = []
        for ticker in self.production["ticker"].unique():
            prod_count    = len(self.production[self.production["ticker"] == ticker])
            staging_count = len(self.staging[self.staging["ticker"] == ticker])
            if staging_count == 0:
                issues.append(f"{ticker}: MISSING in staging (prod has {prod_count} rows)")
            elif staging_count < prod_count * REGRESSION_ROW_THRESHOLD:
                ratio = staging_count / prod_count * 100
                issues.append(f"{ticker}: {ratio:.1f}% of prod rows ({staging_count} vs {prod_count})")
        if issues:
            return r.failed(f"{len(issues)} tickers lost rows vs production.", issues)
        return r.passed("All tickers maintain >= 95% of production row count.")


    def _tc021_historical_prices_unchanged(self) -> TestResult:
        r = TestResult("TC-021", "Historical prices unchanged",
                       "REGRESSION", "HIGH", True)
        if self.production is None:
            return r.warned("Production data not provided — skipping regression check.")
        close_col = "close" if "close" in self.staging.columns else "close_price"
        issues    = []
        for ticker in self.production["ticker"].unique():
            prod_t    = self.production[self.production["ticker"] == ticker][["price_date", close_col]]
            staging_t = self.staging[self.staging["ticker"] == ticker][["price_date", close_col]]
            merged    = prod_t.merge(staging_t, on="price_date", suffixes=("_prod","_stg"))
            if merged.empty:
                continue
            col_prod = f"{close_col}_prod"
            col_stg  = f"{close_col}_stg"
            diff_pct = ((merged[col_prod] - merged[col_stg]).abs() /
                        merged[col_prod].replace(0, np.nan))
            bad      = diff_pct[diff_pct > PRICE_MATCH_THRESHOLD]
            if len(bad) > 0:
                issues.append(f"{ticker}: {len(bad)} historical prices changed")
        if issues:
            return r.failed(f"Historical price changes detected in {len(issues)} tickers.", issues)
        return r.passed("All historical prices match production within 0.01% tolerance.")


    def _tc022_no_tickers_lost(self) -> TestResult:
        r = TestResult("TC-022", "No new tickers lost",
                       "REGRESSION", "HIGH", True)
        if self.production is None:
            return r.warned("Production data not provided — skipping regression check.")
        prod_tickers    = set(self.production["ticker"].unique())
        staging_tickers = set(self.staging["ticker"].unique())
        lost            = prod_tickers - staging_tickers
        if lost:
            return r.failed(f"{len(lost)} production tickers missing in staging.", list(lost))
        return r.passed("All production tickers present in staging.")


    # ── Main runner ───────────────────────────────────────────

    def run_all(self) -> list:
        """Run all 22 UAT test cases and return list of TestResult objects."""
        test_methods = [
            self._tc001_all_tickers_present,
            self._tc002_no_missing_trading_days,
            self._tc003_min_row_count,
            self._tc004_no_null_close,
            self._tc005_analytics_populated,
            self._tc006_price_positive,
            self._tc007_daily_return_bounds,
            self._tc008_rsi_range,
            self._tc009_high_gte_low,
            self._tc010_sma_accuracy,
            self._tc011_no_duplicates,
            self._tc012_data_freshness,
            self._tc013_no_stale_streak,
            self._tc014_date_format,
            self._tc015_numeric_columns,
            self._tc016_ticker_format,
            self._tc017_referential_integrity,
            self._tc018_volume_non_negative,
            self._tc019_asset_class_valid,
            self._tc020_row_count_regression,
            self._tc021_historical_prices_unchanged,
            self._tc022_no_tickers_lost,
        ]

        print(f"\n{'='*60}")
        print(f"  UAT ENGINE — Running {len(test_methods)} test cases")
        print(f"{'='*60}\n")

        self.results = []
        for method in test_methods:
            result = method()
            self.results.append(result)
            icon = "✅" if result.status == "PASS" else "❌" if result.status == "FAIL" else "⚠️ "
            print(f"  {icon}  [{result.test_id}] {result.name[:45]:<45} {result.status}")

        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        warned = sum(1 for r in self.results if r.status == "WARN")
        critical_fails = [r for r in self.results if r.status == "FAIL" and r.sla_critical]

        print(f"\n  {'─'*58}")
        print(f"  PASS: {passed}  |  FAIL: {failed}  |  WARN: {warned}")
        print(f"  SLA-Critical failures: {len(critical_fails)}")
        uat_decision = "✅ APPROVED FOR PRODUCTION" if len(critical_fails) == 0 else "❌ BLOCKED — DO NOT PROMOTE"
        print(f"  UAT Decision: {uat_decision}")
        print(f"  {'─'*58}\n")

        return self.results
