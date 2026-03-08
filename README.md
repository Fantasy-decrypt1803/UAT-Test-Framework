# UAT Test Framework for Financial Data Pipelines
### Python · Pandas · Pytest · openpyxl

> **UBS Data Services context:** Formal UAT (User Acceptance Testing) framework that gates staging → production promotion for financial data pipelines. Every data release at an asset manager must pass a defined set of acceptance criteria before going live. This framework automates that process.

---

## What It Does

Runs **22 automated test cases** across 6 quality dimensions against a staging dataset. Generates a formal Excel sign-off report. Blocks production promotion if any SLA-critical test fails.

```
Staging Data
    │
    ▼
UAT Engine (22 test cases)
    │
    ├── PASS (21/22) ──► ✅ APPROVED FOR PRODUCTION
    │
    └── FAIL (critical) ► ❌ BLOCKED — DO NOT PROMOTE
```

---

## 22 Test Cases

| ID | Category | Test | Severity |
|---|---|---|---|
| TC-001 | Completeness | All 15 expected tickers present | HIGH |
| TC-002 | Completeness | No missing trading days | HIGH |
| TC-003 | Completeness | Min 200 rows per ticker | HIGH |
| TC-004 | Completeness | No null close prices | HIGH |
| TC-005 | Completeness | Analytics columns populated | MEDIUM |
| TC-006 | Accuracy | Close prices positive | HIGH |
| TC-007 | Accuracy | Daily return within ±50% bounds | HIGH |
| TC-008 | Accuracy | RSI within 0–100 range | MEDIUM |
| TC-009 | Accuracy | High >= Low for all rows | HIGH |
| TC-010 | Accuracy | SMA-20 within 5% of manual calc | MEDIUM |
| TC-011 | Accuracy | No duplicate date entries | HIGH |
| TC-012 | Timeliness | Data within last 5 business days | HIGH |
| TC-013 | Timeliness | No stale price streaks | MEDIUM |
| TC-014 | Format | Date column is valid date type | HIGH |
| TC-015 | Format | Numeric columns are numeric | HIGH |
| TC-016 | Format | Tickers match expected universe | MEDIUM |
| TC-017 | Integrity | All tickers exist in dim_asset | HIGH |
| TC-018 | Integrity | Volume non-negative | MEDIUM |
| TC-019 | Integrity | Asset class values valid | MEDIUM |
| TC-020 | Regression | Staging >= 95% of production rows | HIGH |
| TC-021 | Regression | Historical prices unchanged | HIGH |
| TC-022 | Regression | No production tickers lost | HIGH |

---

## Excel Sign-Off Report (4 Sheets)

| Sheet | Contents |
|---|---|
| **UAT Sign-Off** | KPI banner, decision badge (APPROVED/BLOCKED), full results table |
| **Failures & Warnings** | Detailed breakdown of every failed/warned test |
| **By Category** | Results grouped by quality dimension |
| **Sign-Off Checklist** | Formal release checklist with analyst name, timestamp, decision |

---

## Test Scenarios (6 defect types)

| Scenario | Defect Injected | Expected Decision |
|---|---|---|
| `clean` | None | ✅ APPROVED |
| `missing_ticker` | ^FTSE removed | ❌ BLOCKED |
| `null_close` | Null close prices in ^GSPC | ❌ BLOCKED |
| `bad_return` | 99% daily return in GC=F | ❌ BLOCKED |
| `duplicate_dates` | Duplicate rows in ^N225 | ❌ BLOCKED |
| `stale_data` | Frozen BTC-USD price 10 days | ⚠️ WARN |

---

## Usage

```bash
# Install
pip install -r requirements.txt

# Run clean scenario (APPROVED)
python run_uat.py

# Run defective scenario (BLOCKED)
python run_uat.py --scenario missing_ticker
python run_uat.py --scenario null_close
python run_uat.py --scenario bad_return
python run_uat.py --scenario duplicate_dates
python run_uat.py --scenario stale_data

# Run unit tests
pytest tests/unit/ -v
```

---

## Project Structure

```
uat_framework/
├── config/
│   └── test_cases.py          # 22 test case definitions + thresholds
├── tests/
│   ├── uat/
│   │   └── test_engine.py     # Core UAT engine (22 test methods)
│   └── unit/
│       └── test_uat_engine.py # Unit tests for the engine (pytest)
├── pipeline/
│   └── promote.py             # Staging → production promotion workflow
├── reports/
│   └── uat_report.py          # Excel sign-off report generator
├── run_uat.py                 # Main runner
└── requirements.txt
```

---

## Skills Demonstrated

- UAT framework design for financial data pipelines
- 6 quality dimensions: Completeness, Accuracy, Timeliness, Format, Integrity, Regression
- Production gating logic — SLA-critical vs non-critical test separation
- Defect injection for 6 realistic data quality scenarios
- Formal sign-off report generation (4-sheet Excel)
- Unit testing: tests that test the tester
- Pipeline orchestration with configurable scenarios

---
*Built as a portfolio project for Data Analyst roles in Asset Management.*
