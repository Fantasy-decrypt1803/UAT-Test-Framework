"""
config/test_cases.py
Central registry of all UAT test cases.
Each test case maps to a UBS Data Services acceptance criterion.

Test categories:
  COMPLETENESS  — All expected records are present
  ACCURACY      — Values are correct and within thresholds
  TIMELINESS    — Data arrived within SLA window
  FORMAT        — Schema, types, and ranges are correct
  INTEGRITY     — Referential integrity and business rules
  REGRESSION    — Production vs staging comparison
"""

UAT_TEST_CASES = [

    # ── COMPLETENESS ─────────────────────────────────────────
    {
        "id":          "TC-001",
        "category":    "COMPLETENESS",
        "name":        "All expected tickers present",
        "description": "Staging must contain all 15 configured asset tickers",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-002",
        "category":    "COMPLETENESS",
        "name":        "No missing trading days",
        "description": "Each ticker must have data for all expected business days in the period",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-003",
        "category":    "COMPLETENESS",
        "name":        "Minimum row count per ticker",
        "description": "Each ticker must have at least 200 rows for a 1-year load",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-004",
        "category":    "COMPLETENESS",
        "name":        "No null close prices",
        "description": "The close_price / close column must have zero null values",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-005",
        "category":    "COMPLETENESS",
        "name":        "All analytics columns populated",
        "description": "SMA, RSI, volatility, and return columns must be populated for rows where calculable",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },

    # ── ACCURACY ─────────────────────────────────────────────
    {
        "id":          "TC-006",
        "category":    "ACCURACY",
        "name":        "Close price within realistic range",
        "description": "No close price should be zero or negative",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-007",
        "category":    "ACCURACY",
        "name":        "Daily return within bounds",
        "description": "Daily return % must be between -50% and +50% (extreme moves flag bad data)",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-008",
        "category":    "ACCURACY",
        "name":        "RSI within valid range",
        "description": "RSI must be between 0 and 100 — values outside this indicate calculation error",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },
    {
        "id":          "TC-009",
        "category":    "ACCURACY",
        "name":        "High >= Low for all rows",
        "description": "Daily high price must always be greater than or equal to daily low price",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-010",
        "category":    "ACCURACY",
        "name":        "SMA-20 close to average",
        "description": "SMA-20 should be within 5% of manually calculated 20-day average",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },
    {
        "id":          "TC-011",
        "category":    "ACCURACY",
        "name":        "No duplicate date entries",
        "description": "Each ticker must have exactly one record per trading date",
        "severity":    "HIGH",
        "sla_critical": True,
    },

    # ── TIMELINESS ───────────────────────────────────────────
    {
        "id":          "TC-012",
        "category":    "TIMELINESS",
        "name":        "Data freshness check",
        "description": "Most recent record must be within last 5 business days",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-013",
        "category":    "TIMELINESS",
        "name":        "No stale data streaks",
        "description": "No ticker should have the same close price for 5+ consecutive days",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },

    # ── FORMAT ───────────────────────────────────────────────
    {
        "id":          "TC-014",
        "category":    "FORMAT",
        "name":        "Date column is valid date type",
        "description": "price_date must be parseable as a date and not contain strings",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-015",
        "category":    "FORMAT",
        "name":        "Numeric columns are numeric",
        "description": "open, high, low, close, volume must all be numeric — no text values",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-016",
        "category":    "FORMAT",
        "name":        "Ticker format is correct",
        "description": "All ticker values must match the configured asset universe list",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },

    # ── INTEGRITY ────────────────────────────────────────────
    {
        "id":          "TC-017",
        "category":    "INTEGRITY",
        "name":        "Referential integrity — ticker in dim_asset",
        "description": "Every ticker in fact_prices must have a matching entry in dim_asset",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-018",
        "category":    "INTEGRITY",
        "name":        "Volume non-negative",
        "description": "Volume must be zero or positive — no negative volume values allowed",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },
    {
        "id":          "TC-019",
        "category":    "INTEGRITY",
        "name":        "Asset class values valid",
        "description": "asset_class in dim_asset must be one of: Equity, Bond, Commodity, Crypto, FX",
        "severity":    "MEDIUM",
        "sla_critical": False,
    },

    # ── REGRESSION ───────────────────────────────────────────
    {
        "id":          "TC-020",
        "category":    "REGRESSION",
        "name":        "Row count not decreased vs production",
        "description": "Staging must have >= 95% of production row count per ticker",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-021",
        "category":    "REGRESSION",
        "name":        "Historical prices unchanged",
        "description": "Close prices for dates existing in both staging and production must match within 0.01%",
        "severity":    "HIGH",
        "sla_critical": True,
    },
    {
        "id":          "TC-022",
        "category":    "REGRESSION",
        "name":        "No new tickers lost",
        "description": "All tickers present in production must still be present in staging",
        "severity":    "HIGH",
        "sla_critical": True,
    },
]

# Expected asset universe
EXPECTED_TICKERS = [
    "^GSPC", "^IXIC", "^FTSE", "^N225", "^GDAXI",
    "^TNX",  "^TYX",
    "GC=F",  "SI=F",  "CL=F",
    "BTC-USD", "ETH-USD",
    "EURUSD=X", "GBPUSD=X", "JPY=X"
]

VALID_ASSET_CLASSES = ["Equity", "Bond", "Commodity", "Crypto", "FX"]

# Thresholds
MIN_ROWS_PER_TICKER     = 200
MAX_DAILY_RETURN_PCT    = 50.0
DATA_FRESHNESS_DAYS     = 5
STALE_STREAK_DAYS       = 5
REGRESSION_ROW_THRESHOLD = 0.95   # staging must have >= 95% of prod rows
PRICE_MATCH_THRESHOLD   = 0.0001  # 0.01% tolerance for regression price check
SMA_ACCURACY_THRESHOLD  = 0.05    # SMA must be within 5% of manual calc
