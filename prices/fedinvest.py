"""
FedInvest Treasury Price Scraper
================================

Downloads daily per-CUSIP Treasury security prices from TreasuryDirect's FedInvest
system and loads them into the FinTekkers price table.

Data Source:
  FedInvest (Federal Investments Program)
  Today:      https://www.treasurydirect.gov/GA-FI/FedInvest/todaySecurityPriceDetail
  Historical: https://www.treasurydirect.gov/GA-FI/FedInvest/selectSecurityPriceDate
              (POST form: priceDate.month, priceDate.day, priceDate.year)

Fields scraped per CUSIP:
  - CUSIP, Security Type, Coupon Rate, Maturity Date, Call Date
  - Buy Price, Sell Price, End-of-Day Price (all as % of par)

Usage:
  pip install requests beautifulsoup4 psycopg2-binary

  # Fetch today's prices (dry run)
  python3 prices/fedinvest.py --dry-run

  # Fetch a specific date
  python3 prices/fedinvest.py --date 2025-01-15

  # Fetch a date range
  python3 prices/fedinvest.py --from 2025-01-01 --to 2025-01-31

  # Fetch and load into database
  python3 prices/fedinvest.py --date 2025-01-15

The script is idempotent — safe to re-run. Uses INSERT ... ON CONFLICT DO UPDATE.
"""

import argparse
import calendar
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

# Add ledger-models Python to path for proto imports
sys.path.insert(0, os.path.expanduser("~/projects/ledger-models/ledger-models-python"))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.treasurydirect.gov/GA-FI/FedInvest"
TODAY_URL = f"{BASE_URL}/todaySecurityPriceDetail"
HISTORICAL_FORM_URL = f"{BASE_URL}/selectSecurityPriceDate"

# Delay between historical requests (seconds) — be respectful to the server
REQUEST_DELAY = 1.5

# DB config (same as cpi_index.py)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "cejmot-gabze7-qaJdej",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
class SecurityPrice:
    """A single CUSIP price observation for one date."""

    def __init__(
        self,
        cusip: str,
        security_type: str,
        rate: str,
        maturity_date: str,
        call_date: str,
        buy_price: float,
        sell_price: float,
        eod_price: float,
        price_date: date,
    ):
        self.cusip = cusip
        self.security_type = security_type
        self.rate = rate
        self.maturity_date = maturity_date
        self.call_date = call_date
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.eod_price = eod_price
        self.price_date = price_date

    def __repr__(self):
        return (
            f"SecurityPrice({self.cusip}, {self.security_type}, "
            f"buy={self.buy_price}, sell={self.sell_price}, eod={self.eod_price}, "
            f"date={self.price_date})"
        )

    def to_dict(self) -> dict:
        return {
            "cusip": self.cusip,
            "security_type": self.security_type,
            "rate": self.rate,
            "maturity_date": self.maturity_date,
            "call_date": self.call_date,
            "buy_price": self.buy_price,
            "sell_price": self.sell_price,
            "eod_price": self.eod_price,
            "price_date": self.price_date.isoformat(),
        }


# ---------------------------------------------------------------------------
# HTML Parser
# ---------------------------------------------------------------------------
def _parse_price(text: str) -> float:
    """Parse a price string, returning 0.0 for empty/invalid values."""
    text = text.strip()
    if not text or text == "-":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_date_from_header(soup: BeautifulSoup) -> Optional[date]:
    """Extract the price date from the 'Prices For: ...' header."""
    for h2 in soup.find_all("h2"):
        text = h2.get_text()
        match = re.search(r"Prices For:\s*(.+)", text)
        if match:
            date_str = match.group(1).strip()
            # Remove any trailing HTML artifacts
            date_str = re.sub(r"\s*\(.*", "", date_str)
            try:
                return datetime.strptime(date_str, "%b %d, %Y").date()
            except ValueError:
                pass
    return None


def parse_prices_html(html: str, fallback_date: Optional[date] = None) -> list[SecurityPrice]:
    """Parse the FedInvest HTML table into a list of SecurityPrice objects.

    The HTML has a single <table class="data1"> with columns:
      CUSIP | SECURITY TYPE | RATE | MATURITY DATE | CALL DATE | BUY | SELL | END OF DAY
    """
    soup = BeautifulSoup(html, "html.parser")

    # Extract the date from the header
    price_date = _parse_date_from_header(soup) or fallback_date
    if price_date is None:
        raise ValueError("Could not determine price date from HTML")

    table = soup.find("table", class_="data1")
    if table is None:
        raise ValueError("No price table found in HTML (missing <table class='data1'>)")

    prices = []
    rows = table.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        cusip = cells[0].get_text(strip=True)
        # Skip rows that don't look like a CUSIP (9 alphanumeric chars)
        if not re.match(r"^[A-Z0-9]{9}$", cusip):
            continue

        security_type = cells[1].get_text(strip=True)
        rate = cells[2].get_text(strip=True)
        maturity_date = cells[3].get_text(strip=True)
        call_date = cells[4].get_text(strip=True)
        buy_price = _parse_price(cells[5].get_text())
        sell_price = _parse_price(cells[6].get_text())
        eod_price = _parse_price(cells[7].get_text())

        prices.append(SecurityPrice(
            cusip=cusip,
            security_type=security_type,
            rate=rate,
            maturity_date=maturity_date,
            call_date=call_date,
            buy_price=buy_price,
            sell_price=sell_price,
            eod_price=eod_price,
            price_date=price_date,
        ))

    return prices


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------
def fetch_today_prices() -> list[SecurityPrice]:
    """Fetch today's prices from FedInvest."""
    print(f"Fetching today's prices from {TODAY_URL}")
    resp = requests.get(TODAY_URL, timeout=30)
    resp.raise_for_status()
    prices = parse_prices_html(resp.text)
    print(f"  Parsed {len(prices)} securities for {prices[0].price_date if prices else '?'}")
    return prices


def fetch_prices_for_date(target_date: date) -> list[SecurityPrice]:
    """Fetch historical prices for a specific date from FedInvest.

    Uses a requests.Session to maintain the JSESSIONID cookie required
    by the historical price form.
    """
    print(f"Fetching prices for {target_date.isoformat()}")

    session = requests.Session()

    # Step 1: GET the form page to establish session
    session.get(HISTORICAL_FORM_URL, timeout=30)

    # Step 2: POST the date to the form
    form_data = {
        "priceDate.month": str(target_date.month),
        "priceDate.day": str(target_date.day),
        "priceDate.year": str(target_date.year),
        "submit": "Show Prices",
    }
    resp = session.post(HISTORICAL_FORM_URL, data=form_data, allow_redirects=True, timeout=30)
    resp.raise_for_status()

    # Check for error pages
    if "Error Message" in resp.text and '<table class="data1">' not in resp.text:
        print(f"  No data available for {target_date} (possibly a weekend/holiday)")
        return []

    prices = parse_prices_html(resp.text, fallback_date=target_date)
    print(f"  Parsed {len(prices)} securities")
    return prices


def fetch_prices_date_range(
    start_date: date, end_date: date
) -> list[SecurityPrice]:
    """Fetch prices for all business days in a date range.

    Skips weekends and handles holidays gracefully (empty response = skip).
    Adds a delay between requests to be respectful to the server.
    """
    all_prices = []
    current = start_date
    day_count = 0

    while current <= end_date:
        # Skip weekends
        if current.weekday() >= 5:  # Saturday=5, Sunday=6
            current += timedelta(days=1)
            continue

        if day_count > 0:
            time.sleep(REQUEST_DELAY)

        try:
            prices = fetch_prices_for_date(current)
            if prices:
                all_prices.extend(prices)
                day_count += 1
        except Exception as e:
            print(f"  ERROR fetching {current}: {e}")

        current += timedelta(days=1)

    print(f"\nTotal: {len(all_prices)} price records across {day_count} trading days")
    return all_prices


# ---------------------------------------------------------------------------
# CUSIP → Security UUID lookup
# ---------------------------------------------------------------------------
def _build_cusip_lookup() -> dict:
    """Build CUSIP → security UUID lookup from the security table in Postgres."""
    import psycopg2

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT primarykey, binarydata FROM security")
    security_rows = cur.fetchall()
    cur.close()
    conn.close()

    cusip_to_uuid = {}
    for pk, binary in security_rows:
        if binary:
            text = bytes(binary).decode("latin-1", errors="ignore")
            cusip_match = re.search(r"[A-Z0-9]{9}", text)
            if cusip_match:
                cusip_to_uuid[cusip_match.group()] = pk

    return cusip_to_uuid


# ---------------------------------------------------------------------------
# gRPC Price Upload
# ---------------------------------------------------------------------------
PRICE_SERVICE_HOST = "localhost:8083"


def _make_timestamp(d: date):
    """Build a LocalTimestampProto for a given date."""
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto

    dt = datetime(d.year, d.month, d.day)
    seconds = calendar.timegm(dt.timetuple())

    ts = PbTimestamp()
    ts.seconds = seconds
    ts.nanos = 0

    return LocalTimestampProto(timestamp=ts, time_zone="UTC")


def load_prices_to_db(prices: list[SecurityPrice], dry_run: bool = False):
    """Upload FedInvest prices via PriceService.CreateOrUpdate gRPC on port 8083.

    Matches CUSIPs to security UUIDs in the security table, then builds
    a CreatePriceRequestProto for each price and sends it to the price service.
    """
    if dry_run:
        by_type = {}
        for p in prices:
            by_type.setdefault(p.security_type, []).append(p)

        print(f"\n[DRY RUN] Would upsert {len(prices)} price records:")
        for sec_type, type_prices in sorted(by_type.items()):
            print(f"  {sec_type}: {len(type_prices)} securities")

        print(f"\nSample prices (first 10):")
        for p in prices[:10]:
            print(
                f"  {p.cusip}  {p.security_type:<25s}  "
                f"buy={p.buy_price:>10.6f}  sell={p.sell_price:>10.6f}  "
                f"eod={p.eod_price:>10.6f}  date={p.price_date}"
            )
        return

    import grpc
    from fintekkers.models.price.price_pb2 import PriceProto
    from fintekkers.models.price.price_type_pb2 import PERCENTAGE
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto
    from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub

    # Build CUSIP → security UUID lookup
    cusip_to_uuid = _build_cusip_lookup()
    print(f"Found {len(cusip_to_uuid)} securities in database")

    # Connect to price service
    channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
    stub = PriceStub(channel)

    upserted = 0
    skipped_no_security = 0
    errors = 0

    for p in prices:
        security_uuid_str = cusip_to_uuid.get(p.cusip)
        if not security_uuid_str:
            skipped_no_security += 1
            continue
        security_uuid = uuid.UUID(str(security_uuid_str))

        # Use EOD price as the primary price, falling back to sell then buy
        price_value = p.eod_price if p.eod_price > 0 else p.sell_price
        if price_value <= 0:
            price_value = p.buy_price
        if price_value <= 0:
            continue

        # Deterministic UUID for idempotency
        price_uuid = uuid.uuid5(
            uuid.UUID("00000000-0000-0000-0000-000000000000"),
            f"fedinvest-{p.cusip}-{p.price_date.isoformat()}"
        )

        as_of = _make_timestamp(p.price_date)

        request = CreatePriceRequestProto(
            object_class="CreatePriceRequestProto",
            version="0.0.1",
            create_price_input=PriceProto(
                object_class="PriceProto",
                version="0.0.1",
                uuid=UUIDProto(raw_uuid=price_uuid.bytes),
                as_of=as_of,
                valid_from=as_of,
                price=DecimalValueProto(arbitrary_precision_value=str(price_value)),
                security=SecurityProto(
                    object_class="Security",
                    version="0.0.1",
                    uuid=UUIDProto(raw_uuid=security_uuid.bytes),
                    is_link=True,
                ),
                price_type=PERCENTAGE,
            ),
        )

        try:
            stub.CreateOrUpdate(request)
            upserted += 1
        except grpc.RpcError as e:
            errors += 1
            if errors <= 5:
                print(f"  ERROR: {p.cusip} {p.price_date}: {e.details()}")

    channel.close()

    print(f"\nDone. Upserted: {upserted}, Skipped (no security): {skipped_no_security}, "
          f"Errors: {errors}, Total input: {len(prices)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="FedInvest Treasury Price Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prices/fedinvest.py --dry-run                    # Today's prices (no DB write)
  python3 prices/fedinvest.py --date 2025-01-15            # Specific date
  python3 prices/fedinvest.py --from 2025-01-01 --to 2025-01-31  # Date range
  python3 prices/fedinvest.py --date 2025-01-15 --dry-run  # Preview without DB write
        """,
    )
    parser.add_argument("--date", type=str, help="Fetch prices for a specific date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="from_date", type=str, help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=str, help="End date for range (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Preview prices without writing to database")
    args = parser.parse_args()

    # Determine what to fetch
    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        prices = fetch_prices_for_date(target)
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.to_date, "%Y-%m-%d").date()
        if start > end:
            print("ERROR: --from date must be before --to date")
            sys.exit(1)
        days = (end - start).days
        est_requests = sum(1 for d in range(days + 1) if (start + timedelta(days=d)).weekday() < 5)
        est_time = est_requests * REQUEST_DELAY
        print(f"Date range: {start} to {end} ({est_requests} estimated business days)")
        print(f"Estimated time: {est_time:.0f}s ({est_time/60:.1f} min) at {REQUEST_DELAY}s/request")
        prices = fetch_prices_date_range(start, end)
    else:
        # Default: fetch today
        prices = fetch_today_prices()

    if not prices:
        print("No prices fetched. Exiting.")
        sys.exit(0)

    # Summary by security type
    by_type = {}
    for p in prices:
        by_type.setdefault(p.security_type, []).append(p)
    print(f"\nSummary:")
    for sec_type, type_prices in sorted(by_type.items()):
        print(f"  {sec_type}: {len(type_prices)} securities")

    # Load to DB (or dry run)
    load_prices_to_db(prices, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
