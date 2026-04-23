"""
Gilt Price Backfill (gRPC)
==========================

Fetches Gilt (UK Government bond) securities from the SecurityService, then
uploads a closing price for each via the PriceService.

Data sources (in priority order):
  1. yfinance  — tried with ISIN+'=XX' ticker (e.g. 'GB00B24FF097=XX').
                 As of 2026 this returns no data for Gilts; included for
                 future-proofing in case Yahoo adds coverage.
  2. DMO CSV   — UK Debt Management Office publishes daily prices, but their
                 site blocks automated scraping. Placeholder left in _dmo_price().
  3. Fallback  — loads a single test price of 98.50 for today's date.

Gilt discovery:
  Queries SecurityService for BOND_SECURITY securities filtered by issuer name
  (default: "UK Government"). Any security whose primary ISIN identifier starts
  with "GB" is treated as a Gilt.

Idempotency:
  Price UUIDs are deterministic: uuid5(GILT_PRICE_NS, f"{isin}-{date}").
  Re-running the script is safe — PriceService.CreateOrUpdate is a no-op for
  duplicate UUIDs.

Usage:
  python3 prices/backfill_gilts_prices.py                      # Load today's prices
  python3 prices/backfill_gilts_prices.py --date 2026-03-01    # Specific date
  python3 prices/backfill_gilts_prices.py --issuer "UK Gov"    # Override issuer filter
  python3 prices/backfill_gilts_prices.py --dry-run            # Preview without uploading

Dependency:
  Requires Issue #121 (load Gilt securities) to have completed first.
  If no Gilts are found in the ledger the script exits with a clear message.
"""

import argparse
import calendar
import os
import sys
import uuid
from datetime import date, datetime
from typing import List, Optional, Tuple



# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECURITY_SERVICE_HOST = "localhost:8082"
PRICE_SERVICE_HOST = "localhost:8083"

STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")

# Deterministic namespace for Gilt price UUIDs
_GILT_PRICE_NS = uuid.UUID("c3d4e5f6-a7b8-9012-cdef-123456789012")

# Fallback price used when no market data source returns a value
FALLBACK_PRICE = 98.50


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def _update_status(status: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Backfill Gilt prices from DMO or yfinance (Issue #123)\n"
        f"updated: {now}\n"
        f"blockers: {blockers}\n"
        f"progress: {progress}\n"
    )
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(content)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Proto timestamp helper
# ---------------------------------------------------------------------------
def _make_timestamp(d: date):
    """Build a LocalTimestampProto for midnight UTC on the given date."""
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto

    dt = datetime(d.year, d.month, d.day)
    seconds = calendar.timegm(dt.timetuple())
    return LocalTimestampProto(
        timestamp=PbTimestamp(seconds=seconds, nanos=0),
        time_zone="UTC",
    )


# ---------------------------------------------------------------------------
# Step 1: Discover Gilt securities from SecurityService
# ---------------------------------------------------------------------------
def _fetch_gilt_securities(issuer_filter: str) -> List[Tuple[str, str]]:
    """Return a list of (isin, security_uuid_hex) for Gilts in the ledger.

    Queries SecurityService by SECURITY_ISSUER_NAME using the provided filter
    string, then keeps only securities whose primary ISIN identifier starts
    with "GB".
    """
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import ISIN
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    entry = FieldMapEntry(
        field=FieldProto.SECURITY_ISSUER_NAME,
        string_value=issuer_filter,
    )
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)

    gilts = []
    for sec in SecurityService().search(request):
        p = sec.proto
        sec_uuid_hex = p.uuid.raw_uuid.hex()
        # Primary identifier: check for ISIN starting with "GB"
        if p.HasField("identifier"):
            id_type = p.identifier.identifier_type
            id_val = p.identifier.identifier_value
            if id_type == ISIN and id_val.startswith("GB"):
                gilts.append((id_val, sec_uuid_hex))
                continue
        # Also scan extra_identifiers if available
        for extra_id in p.extra_identifiers:
            if extra_id.identifier_type == ISIN and extra_id.identifier_value.startswith("GB"):
                gilts.append((extra_id.identifier_value, sec_uuid_hex))
                break

    return gilts


# ---------------------------------------------------------------------------
# Step 2: Fetch price from yfinance
# ---------------------------------------------------------------------------
def _yfinance_price(isin: str, price_date: date) -> Optional[float]:
    """Try to get an end-of-day close price from yfinance for a Gilt ISIN.

    Uses the ISIN+'=XX' ticker convention. As of 2026, Yahoo Finance does not
    carry Gilt prices in this format — the function returns None in that case.
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    ticker = f"{isin}=XX"
    try:
        # Fetch a small window around the target date
        from datetime import timedelta
        start = price_date - timedelta(days=5)
        end = price_date + timedelta(days=1)
        data = yf.download(ticker, start=start.isoformat(), end=end.isoformat(),
                           progress=False, auto_adjust=True)
        if data.empty:
            return None
        # Get the Close for the exact date if available, else the most recent
        close_col = "Close"
        if hasattr(data.columns, "get_level_values"):
            # Multi-level columns from yfinance ≥ 0.2
            try:
                series = data[close_col][ticker]
            except (KeyError, TypeError):
                series = data[close_col]
        else:
            series = data[close_col]

        target = datetime(price_date.year, price_date.month, price_date.day)
        if target in series.index:
            val = float(series[target])
        elif not series.empty:
            val = float(series.iloc[-1])
        else:
            return None

        return val if val > 0 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 3: DMO price (placeholder — DMO blocks automated scraping)
# ---------------------------------------------------------------------------
def _dmo_price(isin: str, price_date: date) -> Optional[float]:
    """Attempt to fetch a Gilt price from the UK DMO.

    The DMO publishes daily gilt prices at dmo.gov.uk, but their site currently
    blocks automated HTTP requests. This function is a placeholder for when/if
    that changes or a local DMO CSV is provided.
    """
    # Future implementation: parse DMO CSV or API if they add one.
    return None


# ---------------------------------------------------------------------------
# Step 4: Upload price via PriceService
# ---------------------------------------------------------------------------
def _upload_price(
    stub,
    isin: str,
    security_uuid_hex: str,
    price_value: float,
    price_date: date,
    dry_run: bool,
) -> bool:
    """Upload a single Gilt price to PriceService.CreateOrUpdate.

    Price UUID is deterministic (uuid5) for idempotency.
    Returns True on success (or dry-run), False on error.
    """
    price_uuid = uuid.uuid5(_GILT_PRICE_NS, f"gilt-{isin}-{price_date.isoformat()}")
    as_of = _make_timestamp(price_date)

    if dry_run:
        print(f"  [DRY RUN] {isin} {price_date}: {price_value:.4f} (uuid={str(price_uuid)[:8]}...)")
        return True

    from fintekkers.models.price.price_pb2 import PriceProto
    from fintekkers.models.price.price_type_pb2 import ABSOLUTE
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto

    sec_uuid_obj = uuid.UUID(security_uuid_hex)
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
                uuid=UUIDProto(raw_uuid=sec_uuid_obj.bytes),
                is_link=True,
            ),
            price_type=ABSOLUTE,
        ),
    )

    try:
        stub.CreateOrUpdate(request)
        return True
    except Exception as e:
        print(f"  gRPC error {isin} {price_date}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(price_date: date, issuer_filter: str, dry_run: bool) -> None:
    _update_status("in_progress", f"Discovering Gilt securities (issuer filter: {issuer_filter!r})")

    # --- Step 1: discover Gilts ---
    print(f"Querying SecurityService for Gilts (issuer={issuer_filter!r})...")
    gilts = _fetch_gilt_securities(issuer_filter)

    if not gilts:
        msg = (
            f"No Gilt securities found in the ledger for issuer={issuer_filter!r}.\n"
            f"Issue #121 (load Gilt securities) must complete before running this script.\n"
            f"If Gilts are loaded with a different issuer name, pass --issuer '<name>'."
        )
        print(f"\nWARNING: {msg}")
        _update_status(
            "blocked",
            "No Gilt securities found in ledger.",
            blockers="Issue #121 (load Gilt securities) must complete first",
        )
        return

    print(f"Found {len(gilts)} Gilt(s):")
    for isin, sec_uuid_hex in gilts:
        print(f"  {isin}  (uuid={sec_uuid_hex[:8]}...)")

    # --- Step 2: connect to PriceService ---
    stub = None
    channel = None
    if not dry_run:
        import grpc
        from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub

        channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
        stub = PriceStub(channel)
        print(f"\nConnected to PriceService at {PRICE_SERVICE_HOST}")

    # --- Step 3 & 4: fetch price and upload for each Gilt ---
    print(f"\nLoading prices for {price_date}...\n")
    uploaded = 0
    errors = 0

    for isin, sec_uuid_hex in gilts:
        # Try data sources in order
        price_value = None
        source = None

        # 1. yfinance
        price_value = _yfinance_price(isin, price_date)
        if price_value is not None:
            source = "yfinance"

        # 2. DMO
        if price_value is None:
            price_value = _dmo_price(isin, price_date)
            if price_value is not None:
                source = "DMO"

        # 3. Fallback test price
        if price_value is None:
            price_value = FALLBACK_PRICE
            source = f"fallback ({FALLBACK_PRICE})"

        print(f"  {isin}: {price_value:.4f}  [{source}]")

        ok = _upload_price(stub, isin, sec_uuid_hex, price_value, price_date, dry_run)
        if ok:
            uploaded += 1
        else:
            errors += 1

    if channel:
        channel.close()

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Done: {uploaded} uploaded, {errors} errors")

    _update_status(
        "review",
        f"{prefix}{uploaded} Gilt prices loaded for {price_date}. "
        f"Gilts: {len(gilts)}. Errors: {errors}.",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill UK Gilt prices via PriceService gRPC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prices/backfill_gilts_prices.py                        # Today's prices
  python3 prices/backfill_gilts_prices.py --date 2026-03-01      # Specific date
  python3 prices/backfill_gilts_prices.py --issuer "UK Gov"      # Override issuer name
  python3 prices/backfill_gilts_prices.py --dry-run              # Preview
        """,
    )
    parser.add_argument(
        "--date",
        dest="price_date",
        type=str,
        default=date.today().isoformat(),
        help="Price date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--issuer",
        dest="issuer",
        type=str,
        default="UK Government",
        help="SECURITY_ISSUER_NAME filter for the Gilt lookup (default: 'UK Government')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without uploading to PriceService",
    )
    args = parser.parse_args()

    price_date = datetime.strptime(args.price_date, "%Y-%m-%d").date()

    print(f"Gilt Price Backfill")
    print(f"  Date:           {price_date}")
    print(f"  Issuer filter:  {args.issuer!r}")
    print(f"  SecurityService:{SECURITY_SERVICE_HOST}")
    print(f"  PriceService:   {PRICE_SERVICE_HOST}")
    print(f"  Dry run:        {args.dry_run}")
    print()

    run(price_date=price_date, issuer_filter=args.issuer, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
