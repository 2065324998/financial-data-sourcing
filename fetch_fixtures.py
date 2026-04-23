#!/usr/bin/env python3
"""
Fetch real data from external sources and save as JSON fixtures.

Target: ~10,000 data points across all sources.

Usage:
  python3 fetch_fixtures.py              # Fetch all sources
  python3 fetch_fixtures.py --source fedinvest
  python3 fetch_fixtures.py --source wikipedia
  python3 fetch_fixtures.py --source yahoo
  python3 fetch_fixtures.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def fetch_fedinvest(dry_run: bool = False):
    """Fetch ~25 trading days of FedInvest prices (~10,000 records)."""
    from bond.sources import LiveFedInvestSource

    out_dir = os.path.join(FIXTURES_DIR, "bond", "fedinvest")
    os.makedirs(out_dir, exist_ok=True)

    source = LiveFedInvestSource()
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=40)  # ~25 trading days in 40 calendar days
    current = start
    total_records = 0
    days_fetched = 0

    print(f"FedInvest: fetching prices from {start} to {end}")

    while current <= end and total_records < 12000:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue

        out_path = os.path.join(out_dir, f"{current.isoformat()}.json")
        if os.path.exists(out_path):
            with open(out_path) as f:
                existing = json.load(f)
            total_records += len(existing)
            days_fetched += 1
            print(f"  {current}: {len(existing)} records (cached)")
            current += timedelta(days=1)
            continue

        if dry_run:
            print(f"  {current}: [DRY RUN] would fetch")
            current += timedelta(days=1)
            continue

        try:
            prices = source.fetch_prices_for_date(current)
        except Exception as e:
            print(f"  {current}: FETCH ERROR {e}")
            time.sleep(1.5)
            current += timedelta(days=1)
            continue

        if not prices:
            print(f"  {current}: no data (holiday)")
            time.sleep(1.5)
            current += timedelta(days=1)
            continue

        try:
            with open(out_path, "w") as f:
                json.dump(prices, f)
            total_records += len(prices)
            days_fetched += 1
            print(f"  {current}: {len(prices)} records")
        except OSError as e:
            print(f"  {current}: WRITE ERROR {e}")

        time.sleep(1.5)
        current += timedelta(days=1)

    print(f"  Total: {total_records} price records across {days_fetched} days\n")


def fetch_wikipedia(dry_run: bool = False):
    """Fetch S&P 500, Nasdaq-100, Dow Jones constituents (~630 records)."""
    from equity.sources import LiveWikipediaSource

    out_dir = os.path.join(FIXTURES_DIR, "equity", "wikipedia")
    os.makedirs(out_dir, exist_ok=True)

    source = LiveWikipediaSource()
    total = 0

    for name, method in [("sp500", source.fetch_sp500),
                          ("nasdaq100", source.fetch_nasdaq100),
                          ("dow", source.fetch_dow)]:
        out_path = os.path.join(out_dir, f"{name}.json")
        if dry_run:
            print(f"  {name}: [DRY RUN] would fetch")
            continue
        try:
            data = method()
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2)
            total += len(data)
            print(f"  {name}: {len(data)} constituents")
        except Exception as e:
            print(f"  {name}: ERROR {e}")

    print(f"  Total: {total} equity constituents\n")


def fetch_yahoo(dry_run: bool = False):
    """Fetch 60 days of prices for top 20 equities (~1,200 records)."""
    from equity.sources import LiveYahooFinanceSource

    out_dir = os.path.join(FIXTURES_DIR, "equity", "yahoo")
    os.makedirs(out_dir, exist_ok=True)

    source = LiveYahooFinanceSource()
    tickers = [
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "BRK-B",
        "JPM", "V", "UNH", "XOM", "MA", "JNJ", "PG", "HD", "COST", "MRK",
        "ABBV", "CRM",
    ]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=90)

    print(f"Yahoo Finance: fetching {len(tickers)} tickers from {start} to {end}")

    if dry_run:
        print(f"  [DRY RUN] would fetch {len(tickers)} tickers")
        return

    try:
        prices = source.download_prices(tickers, start, end)
        total = 0
        for ticker, ticker_prices in prices.items():
            if ticker_prices:
                out_path = os.path.join(out_dir, f"{ticker}.json")
                with open(out_path, "w") as f:
                    json.dump(ticker_prices, f, indent=2)
                total += len(ticker_prices)
                print(f"  {ticker}: {len(ticker_prices)} days")
        print(f"  Total: {total} price records\n")
    except Exception as e:
        print(f"  ERROR: {e}\n")


def main():
    parser = argparse.ArgumentParser(description="Fetch real data fixtures (~10K data points)")
    parser.add_argument("--source", choices=["fedinvest", "wikipedia", "yahoo", "all"],
                        default="all", help="Which source to fetch (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sources = {
        "fedinvest": ("FedInvest Treasury Prices", fetch_fedinvest),
        "wikipedia": ("Wikipedia Index Constituents", fetch_wikipedia),
        "yahoo": ("Yahoo Finance Equity Prices", fetch_yahoo),
    }

    to_run = sources if args.source == "all" else {args.source: sources[args.source]}

    print("=" * 60)
    print("Fetching fixtures for financial-data-sourcing")
    print("=" * 60)

    for key, (label, func) in to_run.items():
        print(f"\n--- {label} ---")
        func(dry_run=args.dry_run)

    # Summary
    total = 0
    for root, _, files in os.walk(FIXTURES_DIR):
        for f in files:
            if f.endswith(".json"):
                path = os.path.join(root, f)
                try:
                    with open(path) as fh:
                        data = json.load(fh)
                        total += len(data) if isinstance(data, list) else len(data)
                except Exception:
                    pass

    print("=" * 60)
    print(f"Total data points in fixtures: {total}")
    print("=" * 60)


if __name__ == "__main__":
    main()
