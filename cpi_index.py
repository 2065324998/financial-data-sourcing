"""
CPI-U Index Price Sourcing Script
==================================

Downloads monthly CPI-U (Consumer Price Index for All Urban Consumers,
Not Seasonally Adjusted) data and loads it into the FinTekkers price table.

Data Sources:
  Primary:   BLS API v2 — series CUUR0000SA0 (NSA)
             https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0
  Secondary: FRED CPIAUCNS (for reconciliation)
             https://fred.stlouisfed.org/series/CPIAUCNS

CPI-U Security UUID: c7c719a1-7bbc-5890-992d-7f6f3a4b3dca

Usage:
  pip install requests psycopg2-binary
  python3 cpi_index.py                  # Fetch & upsert all available data (2020-current)
  python3 cpi_index.py --reconcile      # Also reconcile BLS vs FRED values
  python3 cpi_index.py --dry-run        # Show what would be inserted without writing

The script is idempotent — safe to re-run via PriceService.CreateOrUpdate gRPC.
"""

import argparse
import calendar
import json
import os
import sys
import uuid
from datetime import datetime

import requests

# Add ledger-models Python to path for proto imports
sys.path.insert(0, os.path.expanduser("~/projects/ledger-models/ledger-models-python"))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CPI_U_SECURITY_UUID = "c7c719a1-7bbc-5890-992d-7f6f3a4b3dca"
CPI_U_SECURITY_UUID_BYTES = uuid.UUID(CPI_U_SECURITY_UUID).bytes

BLS_SERIES_ID = "CUUR0000SA0"  # CPI-U All Items, Not Seasonally Adjusted
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

FRED_SERIES_URL = (
    "https://api.stlouisfed.org/fred/series/observations"
    "?series_id=CPIAUCNS&file_type=json&api_key=DEMO_KEY"
)

START_YEAR = 2020

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "cejmot-gabze7-qaJdej",
}

# ---------------------------------------------------------------------------
# BLS API Fetcher
# ---------------------------------------------------------------------------

def fetch_from_bls(start_year: int, end_year: int) -> list[dict]:
    """Fetch CPI-U NSA monthly data from BLS API v2 (public, no key needed).

    BLS limits requests to 20 years and 2 series per query on the public tier.
    """
    # BLS allows max 20 year span per request
    observations = []
    chunk_start = start_year
    while chunk_start <= end_year:
        chunk_end = min(chunk_start + 19, end_year)
        payload = {
            "seriesid": [BLS_SERIES_ID],
            "startyear": str(chunk_start),
            "endyear": str(chunk_end),
        }
        resp = requests.post(BLS_API_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = data.get("message", ["Unknown error"])
            raise RuntimeError(f"BLS API error: {msg}")

        for series in data.get("Results", {}).get("series", []):
            for item in series.get("data", []):
                period = item.get("period", "")
                if not period.startswith("M"):
                    continue  # skip annual averages (M13)
                month = int(period[1:])
                if month < 1 or month > 12:
                    continue
                year = int(item["year"])
                value = item["value"].strip()
                if value == "-":
                    continue  # data unavailable
                observations.append({
                    "year": year,
                    "month": month,
                    "value": value,
                })

        chunk_start = chunk_end + 1

    # BLS returns newest-first; sort chronologically
    observations.sort(key=lambda o: (o["year"], o["month"]))
    return observations


# ---------------------------------------------------------------------------
# FRED API Fetcher (fallback / reconciliation)
# ---------------------------------------------------------------------------

def fetch_from_fred(start_year: int, end_year: int) -> list[dict]:
    """Fetch CPI-U NSA from FRED as a secondary source.

    Uses the DEMO_KEY which has limited rate (max ~30 requests/day).
    Falls back gracefully if unavailable.
    """
    url = (
        f"{FRED_SERIES_URL}"
        f"&observation_start={start_year}-01-01"
        f"&observation_end={end_year}-12-31"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    observations = []
    for obs in data.get("observations", []):
        if obs["value"] == ".":
            continue
        dt = datetime.strptime(obs["date"], "%Y-%m-%d")
        observations.append({
            "year": dt.year,
            "month": dt.month,
            "value": obs["value"],
        })

    observations.sort(key=lambda o: (o["year"], o["month"]))
    return observations


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile(bls_data: list[dict], fred_data: list[dict], tolerance: float = 0.01):
    """Compare BLS and FRED data. Print mismatches exceeding tolerance."""
    fred_lookup = {(o["year"], o["month"]): float(o["value"]) for o in fred_data}
    mismatches = []

    for obs in bls_data:
        key = (obs["year"], obs["month"])
        bls_val = float(obs["value"])
        fred_val = fred_lookup.get(key)
        if fred_val is None:
            continue
        diff = abs(bls_val - fred_val)
        if diff > tolerance:
            mismatches.append({
                "date": f"{key[0]}-{key[1]:02d}",
                "bls": bls_val,
                "fred": fred_val,
                "diff": round(diff, 3),
            })

    if mismatches:
        print(f"\nRECONCILIATION: {len(mismatches)} mismatches (tolerance={tolerance}):")
        for m in mismatches:
            print(f"  {m['date']}: BLS={m['bls']}, FRED={m['fred']}, diff={m['diff']}")
    else:
        matched = sum(1 for o in bls_data if (o["year"], o["month"]) in fred_lookup)
        print(f"\nRECONCILIATION: All {matched} overlapping data points match (tolerance={tolerance})")

    return mismatches


# ---------------------------------------------------------------------------
# gRPC Price Upload
# ---------------------------------------------------------------------------
PRICE_SERVICE_HOST = "localhost:8083"


def _make_timestamp(year: int, month: int, day: int = 1):
    """Build a LocalTimestampProto for a given date."""
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto

    dt = datetime(year, month, day)
    seconds = calendar.timegm(dt.timetuple())

    ts = PbTimestamp()
    ts.seconds = seconds
    ts.nanos = 0

    return LocalTimestampProto(timestamp=ts, time_zone="UTC")


def upsert_prices(observations: list[dict], dry_run: bool = False):
    """Upload CPI-U price records via PriceService.CreateOrUpdate gRPC on port 8083.

    Generates a deterministic UUID per (security, year, month) so re-runs
    produce the same price UUID, making upserts idempotent.
    """
    if dry_run:
        print(f"\n[DRY RUN] Would upsert {len(observations)} price records:")
        for obs in observations:
            print(f"  {obs['year']}-{obs['month']:02d}: {obs['value']}")
        return

    import grpc
    from fintekkers.models.price.price_pb2 import PriceProto
    from fintekkers.models.price.price_type_pb2 import INDEX_LEVEL
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.security.security_type_pb2 import INDEX_SECURITY
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto
    from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub

    channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
    stub = PriceStub(channel)

    upserted = 0
    errors = 0

    for obs in observations:
        year, month, value = obs["year"], obs["month"], obs["value"]
        label = f"{year}-{month:02d}"

        # Deterministic UUID: namespace UUID5 from security UUID + date
        price_uuid = uuid.uuid5(
            uuid.UUID(CPI_U_SECURITY_UUID), f"CPI-U-{year}-{month:02d}-01"
        )

        as_of = _make_timestamp(year, month)

        request = CreatePriceRequestProto(
            object_class="CreatePriceRequestProto",
            version="0.0.1",
            create_price_input=PriceProto(
                object_class="PriceProto",
                version="0.0.1",
                uuid=UUIDProto(raw_uuid=price_uuid.bytes),
                as_of=as_of,
                valid_from=as_of,
                price=DecimalValueProto(arbitrary_precision_value=value),
                security=SecurityProto(
                    object_class="Security",
                    version="0.0.1",
                    uuid=UUIDProto(raw_uuid=CPI_U_SECURITY_UUID_BYTES),
                    is_link=True,
                    security_type=INDEX_SECURITY,
                ),
                price_type=INDEX_LEVEL,
            ),
        )

        try:
            stub.CreateOrUpdate(request)
            upserted += 1
            print(f"  OK: {label} = {value}")
        except grpc.RpcError as e:
            errors += 1
            print(f"  FAILED: {label} = {value} — {e.details()}")

    channel.close()
    print(f"\nDone. Upserted: {upserted}, Errors: {errors}, Total: {len(observations)}")


def verify_count():
    """Print the total number of CPI-U price records in the database."""
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM price WHERE securityid = %s",
        (CPI_U_SECURITY_UUID,),
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"Verification: {count} CPI-U price records in database")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CPI-U Index Price Sourcing")
    parser.add_argument("--reconcile", action="store_true", help="Reconcile BLS vs FRED data")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be inserted")
    parser.add_argument("--start-year", type=int, default=START_YEAR, help=f"Start year (default: {START_YEAR})")
    parser.add_argument("--end-year", type=int, default=datetime.now().year, help="End year (default: current)")
    args = parser.parse_args()

    print(f"Fetching CPI-U (NSA) from BLS API: {BLS_SERIES_ID}")
    print(f"Date range: {args.start_year}-{args.end_year}")

    # 1. Fetch from primary source (BLS)
    try:
        bls_data = fetch_from_bls(args.start_year, args.end_year)
        print(f"Fetched {len(bls_data)} observations from BLS")
        source = "BLS"
    except Exception as e:
        print(f"BLS API failed: {e}")
        print("Falling back to FRED...")
        try:
            bls_data = fetch_from_fred(args.start_year, args.end_year)
            print(f"Fetched {len(bls_data)} observations from FRED")
            source = "FRED"
        except Exception as e2:
            print(f"FRED API also failed: {e2}")
            sys.exit(1)

    if not bls_data:
        print("No data fetched. Exiting.")
        sys.exit(1)

    # 2. Reconcile with FRED if requested
    if args.reconcile and source == "BLS":
        print("\nFetching FRED data for reconciliation...")
        try:
            fred_data = fetch_from_fred(args.start_year, args.end_year)
            print(f"Fetched {len(fred_data)} observations from FRED")
            reconcile(bls_data, fred_data)
        except Exception as e:
            print(f"FRED reconciliation failed: {e}")

    # 3. Upsert into Postgres
    print(f"\nUpserting {len(bls_data)} price records (source: {source})...")
    upsert_prices(bls_data, dry_run=args.dry_run)

    # 4. Verify
    if not args.dry_run:
        verify_count()


if __name__ == "__main__":
    main()
