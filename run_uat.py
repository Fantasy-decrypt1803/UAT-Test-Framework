"""
run_uat.py — UAT Framework Main Runner

Usage:
    python run_uat.py                        # clean scenario
    python run_uat.py --scenario missing_ticker
    python run_uat.py --scenario null_close
    python run_uat.py --scenario bad_return
    python run_uat.py --scenario duplicate_dates
    python run_uat.py --scenario stale_data

All scenarios:
    clean           — perfect data, APPROVED
    missing_ticker  — one ticker absent, BLOCKED
    null_close      — null close prices, BLOCKED
    bad_return      — extreme daily return, BLOCKED
    duplicate_dates — duplicate rows, BLOCKED
    stale_data      — frozen price streak, WARN
"""

import argparse
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from pipeline.promote import run_promotion

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UAT Framework for Financial Data Pipelines")
    parser.add_argument(
        "--scenario", default="clean",
        choices=["clean","missing_ticker","null_close","bad_return","duplicate_dates","stale_data"],
        help="Data quality scenario to simulate"
    )
    args = parser.parse_args()
    promoted = run_promotion(args.scenario)
    sys.exit(0 if promoted else 1)
