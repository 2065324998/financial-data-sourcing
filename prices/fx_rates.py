"""
FRED Historical FX Rate Loader
==============================

Downloads daily GBP/USD (DEXUSUK) and EUR/USD (DEXUSEU) spot rates from FRED
and loads them into the FinTekkers platform via SecurityService + PriceService.

Data Source:
  FRED (Federal Reserve Economic Data), St. Louis Fed
  GBP/USD: https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSUK
  EUR/USD: https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU

  Both series represent "units of USD per 1 foreign currency unit":
    DEXUSUK = USD per 1 GBP (e.g. 1.25 means 1 GBP = 1.25 USD)
    DEXUSEU = USD per 1 EUR (e.g. 1.08 means 1 EUR = 1.08 USD)

  Update frequency: US business days. Missing values (weekends, holidays) appear
  as "." in the CSV — these rows are skipped.

FX Security Convention:
  SecurityType: FX_SPOT
  Identifier:   EXCH_TICKER, e.g. "GBP/USD"
  FxSpotDetails: base_currency=GBP, quote_currency=USD,
                 convention="UNITS_OF_QUOTE_PER_BASE"

Idempotency:
  - Security UUIDs are deterministic (uuid5) based on the pair name; the script
    queries the ledger first and only creates if absent.
  - Price UUIDs are deterministic based on (series_id, date), so re-runs
    produce the same UUID and PriceService.CreateOrUpdate is a no-op.
  - Checkpoint file ~/second-brain/status/fx_backfill_checkpoint.txt stores
    the last fully-uploaded date; re-running resumes from there.

Usage:
  python3 prices/fx_rates.py                      # Full backfill (resumes from checkpoint)
  python3 prices/fx_rates.py --from 2015-01-01    # Override start date
  python3 prices/fx_rates.py --dry-run            # Preview without uploading

  # Force full re-run from the beginning (ignores checkpoint):
  python3 prices/fx_rates.py --from 2009-01-01
"""

import argparse
import calendar
import io
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

import requests

sys.path.insert(0, os.path.expanduser("~/projects/ledger-models/ledger-models-python"))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

# FRED series ID -> (pair label, base ISO, quote ISO)
FX_PAIRS = [
    ("DEXUSUK", "GBP/USD", "GBP", "USD"),
    ("DEXUSEU", "EUR/USD", "EUR", "USD"),
]

BACKFILL_START = date(2009, 1, 1)

PRICE_SERVICE_HOST = "localhost:8083"
SECURITY_SERVICE_HOST = "localhost:8082"

STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")
CHECKPOINT_FILE = os.path.expanduser("~/second-brain/status/fx_backfill_checkpoint.txt")
STATUS_UPDATE_INTERVAL = 300  # seconds

# Deterministic UUID namespace for FX securities
_FX_SECURITY_NS = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
# Deterministic UUID namespace for FX prices
_FX_PRICE_NS = uuid.UUID("b2c3d4e5-f6a7-8901-bcde-f12345678901")


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
def _load_checkpoint() -> Optional[date]:
    """Return the last successfully processed date, or None."""
    try:
        with open(CHECKPOINT_FILE) as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d").date()
    except (FileNotFoundError, ValueError):
        return None


def _save_checkpoint(d: date) -> None:
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(d.isoformat())
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def _update_status(status: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Backfill historical FX rates from FRED (Issue #55)\n"
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
# FRED data download
# ---------------------------------------------------------------------------
def download_fred_series(series_id: str) -> dict[date, float]:
    """Download a FRED CSV series and return a {date: rate} dict.

    Missing values (FRED uses "." for non-trading days) are excluded.
    """
    url = FRED_CSV_URL.format(series_id=series_id)
    print(f"Downloading {series_id} from FRED...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    data: dict[date, float] = {}
    reader = io.StringIO(resp.text)
    # Skip header line "DATE,{SERIES_ID}"
    next(reader)
    for line in reader:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) != 2:
            continue
        date_str, value_str = parts[0].strip(), parts[1].strip()
        if value_str == ".":
            continue  # missing value (weekend/holiday/not yet published)
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            data[d] = float(value_str)
        except (ValueError, IndexError):
            continue

    print(f"  {len(data)} trading days loaded for {series_id}")
    return data


# ---------------------------------------------------------------------------
# Proto helpers
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


def _get_cash_security(iso_code: str):
    """Fetch a CASH security proto from the ledger by ISO currency code."""
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import CASH
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    id_proto = IdentifierProto(identifier_type=CASH, identifier_value=iso_code)
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)
    for sec in SecurityService().search(request):
        return sec.proto
    raise RuntimeError(
        f"{iso_code} cash security not found in ledger — "
        "ensure the currency is loaded before running this script."
    )


# ---------------------------------------------------------------------------
# FX security create-or-find
# ---------------------------------------------------------------------------
def _find_fx_security(pair_label: str):
    """Look up an existing FX_SPOT security by its EXCH_TICKER identifier.

    Returns the SecurityProto if found, else None.
    """
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    id_proto = IdentifierProto(identifier_type=EXCH_TICKER, identifier_value=pair_label)
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)
    for sec in SecurityService().search(request):
        return sec.proto
    return None


def _create_fx_security(
    pair_label: str,
    base_iso: str,
    quote_iso: str,
    base_cash,
    quote_cash,
):
    """Create an FX_SPOT security in the ledger and return its SecurityProto."""
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.security.security_pb2 import SecurityProto, FxSpotDetailsProto
    from fintekkers.models.security.security_type_pb2 import FX_SPOT
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.wrappers.models.security.security import Security
    from fintekkers.wrappers.requests.security import CreateSecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    sec_uuid = uuid.uuid5(_FX_SECURITY_NS, f"fx-spot-{pair_label}")
    ts_seconds = int(time.time())

    proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="UTC",
            timestamp=PbTimestamp(seconds=ts_seconds, nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
        security_type=FX_SPOT,
        description=f"{pair_label} FX Spot Rate",
        identifier=IdentifierProto(
            identifier_type=EXCH_TICKER,
            identifier_value=pair_label,
        ),
        # Settlement currency = the quote currency (USD for both pairs)
        settlement_currency=quote_cash,
        fx_spot_details=FxSpotDetailsProto(
            base_currency=base_cash,
            quote_currency=quote_cash,
            convention="UNITS_OF_QUOTE_PER_BASE",
        ),
    )

    security = Security(proto)
    request = CreateSecurityRequest.create_or_update_request(security)
    SecurityService().create_or_update(request)
    print(f"  Created FX_SPOT security: {pair_label} (UUID: {sec_uuid})")
    return proto


def ensure_fx_security(pair_label: str, base_iso: str, quote_iso: str, dry_run: bool):
    """Return (SecurityProto, security_uuid) for the given pair.

    Creates the security if it doesn't already exist.
    In dry-run mode, returns a synthetic proto with a deterministic UUID.
    """
    sec_uuid = uuid.uuid5(_FX_SECURITY_NS, f"fx-spot-{pair_label}")

    if dry_run:
        from fintekkers.models.security.security_pb2 import SecurityProto
        from fintekkers.models.util.uuid_pb2 import UUIDProto
        return SecurityProto(
            object_class="Security",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
            is_link=True,
        ), sec_uuid

    existing = _find_fx_security(pair_label)
    if existing is not None:
        print(f"  {pair_label}: security already exists (skipping create)")
        return existing, uuid.UUID(bytes=existing.uuid.raw_uuid)

    print(f"  {pair_label}: creating FX_SPOT security...")
    base_cash = _get_cash_security(base_iso)
    quote_cash = _get_cash_security(quote_iso)
    proto = _create_fx_security(pair_label, base_iso, quote_iso, base_cash, quote_cash)
    return proto, sec_uuid


# ---------------------------------------------------------------------------
# Price upload
# ---------------------------------------------------------------------------
def _upload_price(
    stub,
    series_id: str,
    security_uuid: uuid.UUID,
    price_date: date,
    rate: float,
) -> bool:
    """Upload a single FX rate as a PriceProto via PriceService.CreateOrUpdate.

    Uses a deterministic UUID so repeated calls are idempotent.
    Returns True on success, False on error.
    """
    from fintekkers.models.price.price_pb2 import PriceProto
    from fintekkers.models.price.price_type_pb2 import ABSOLUTE
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto

    price_uuid = uuid.uuid5(_FX_PRICE_NS, f"fred-{series_id}-{price_date.isoformat()}")
    as_of = _make_timestamp(price_date)

    request = CreatePriceRequestProto(
        object_class="CreatePriceRequestProto",
        version="0.0.1",
        create_price_input=PriceProto(
            object_class="PriceProto",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=price_uuid.bytes),
            as_of=as_of,
            valid_from=as_of,
            price=DecimalValueProto(arbitrary_precision_value=str(rate)),
            security=SecurityProto(
                object_class="Security",
                version="0.0.1",
                uuid=UUIDProto(raw_uuid=security_uuid.bytes),
                is_link=True,
            ),
            price_type=ABSOLUTE,
        ),
    )

    try:
        stub.CreateOrUpdate(request)
        return True
    except Exception as e:
        print(f"  gRPC error {series_id} {price_date}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main backfill loop
# ---------------------------------------------------------------------------
def run_backfill(
    start: date,
    end: date,
    dry_run: bool,
) -> None:
    """Download FX rates and upload prices for all dates in [start, end]."""

    # Download all FRED data upfront (two small CSV files)
    fred_data: dict[str, dict[date, float]] = {}
    for series_id, pair_label, _, _ in FX_PAIRS:
        fred_data[series_id] = download_fred_series(series_id)

    # Resolve FX securities (create if missing)
    print("\nResolving FX_SPOT securities...")
    securities: dict[str, tuple] = {}  # series_id -> (SecurityProto, uuid)
    for series_id, pair_label, base_iso, quote_iso in FX_PAIRS:
        proto, sec_uuid = ensure_fx_security(pair_label, base_iso, quote_iso, dry_run)
        securities[series_id] = (proto, sec_uuid)

    # Connect to PriceService
    stub = None
    channel = None
    if not dry_run:
        import grpc
        from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub
        channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
        stub = PriceStub(channel)
        print(f"\nConnected to PriceService at {PRICE_SERVICE_HOST}")

    # Loop over date range
    print(f"\nUploading prices from {start} to {end}...\n")
    total_uploaded = 0
    total_skipped = 0
    total_errors = 0
    days_processed = 0
    last_status_update = time.time()

    current = start
    while current <= end:
        day_uploaded = 0
        day_skipped = 0
        day_errors = 0

        for series_id, pair_label, _, _ in FX_PAIRS:
            rate = fred_data[series_id].get(current)
            if rate is None:
                day_skipped += 1
                continue  # no data for this date (weekend/holiday)

            if dry_run:
                day_uploaded += 1
                continue

            _, sec_uuid = securities[series_id]
            ok = _upload_price(stub, series_id, sec_uuid, current, rate)
            if ok:
                day_uploaded += 1
            else:
                day_errors += 1

        total_uploaded += day_uploaded
        total_skipped += day_skipped
        total_errors += day_errors

        if day_uploaded > 0 or day_skipped > 0:
            days_processed += 1
            _save_checkpoint(current)

        if days_processed % 100 == 0 and days_processed > 0:
            print(
                f"  Progress: {days_processed} days processed, "
                f"{total_uploaded} uploaded, {total_errors} errors, at {current}"
            )

        now = time.time()
        if now - last_status_update >= STATUS_UPDATE_INTERVAL:
            _update_status(
                status="in_progress",
                progress=(
                    f"{total_uploaded} prices uploaded, {days_processed} days, "
                    f"{total_errors} errors, currently at {current.isoformat()}"
                ),
            )
            last_status_update = now

        current += timedelta(days=1)

    if channel:
        channel.close()

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}FX rate backfill complete:")
    print(f"  Date range:      {start} to {end}")
    print(f"  Days processed:  {days_processed}")
    print(f"  Prices uploaded: {total_uploaded}")
    print(f"  Dates skipped:   {total_skipped} (no FRED data)")
    print(f"  Errors:          {total_errors}")

    _update_status(
        status="review",
        progress=(
            f"{prefix}Complete. {total_uploaded} prices uploaded across "
            f"{days_processed} days. Errors: {total_errors}."
        ),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    checkpoint = _load_checkpoint()
    default_start = (checkpoint + timedelta(days=1)) if checkpoint else BACKFILL_START
    default_end = date.today()

    parser = argparse.ArgumentParser(
        description="Load historical GBP/USD and EUR/USD FX rates from FRED into FinTekkers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prices/fx_rates.py                       # Full backfill (2009-01-01 to today)
  python3 prices/fx_rates.py --from 2015-01-01     # Override start date
  python3 prices/fx_rates.py --dry-run             # Preview without uploading
  python3 prices/fx_rates.py --from 2009-01-01     # Force full re-run (ignore checkpoint)
        """,
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=default_start.isoformat(),
        help=(
            f"Start date YYYY-MM-DD "
            f"(default: {default_start} — from checkpoint)"
            if checkpoint
            else f"Start date YYYY-MM-DD (default: {BACKFILL_START})"
        ),
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=default_end.isoformat(),
        help=f"End date YYYY-MM-DD (default: today, {default_end})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    args = parser.parse_args()

    start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.to_date, "%Y-%m-%d").date()

    if start > end:
        print(f"ERROR: --from ({start}) must be on or before --to ({end})")
        sys.exit(1)

    if checkpoint:
        print(f"Checkpoint: last completed date = {checkpoint}")
    print(f"Date range: {start} to {end}")
    print(f"Pairs: {', '.join(p for _, p, _, _ in FX_PAIRS)}")
    print(f"Checkpoint file: {CHECKPOINT_FILE}")
    print(f"PriceService: {PRICE_SERVICE_HOST}")
    print()

    _update_status(
        status="in_progress",
        progress=f"Starting backfill from {start} to {end}",
    )

    run_backfill(start=start, end=end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
