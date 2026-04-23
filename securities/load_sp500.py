"""
S&P 500 Index Composition Loader
==================================

Creates the S&P 500 index security (ticker: SPX, type: EQUITY_INDEX_SECURITY)
in ledger-service via SecurityService, then loads the index composition (top-30
constituents by market cap) via IndexCompositionService.CreateOrUpdate.

Constituents use a hardcoded top-30 list (Q1 2026 approximate weights). If a
constituent security is not already in the ledger it is created automatically,
following the same UUID namespace as equity_securities.py.

Index security:
  type:        EQUITY_INDEX_SECURITY
  ticker:      SPX
  description: S&P 500 Index

Composition:
  effective_date: today (or --date override)
  constituents:   top-30 S&P 500 names with approximate weights

Idempotency:
  - SPX security UUID is deterministic (uuid5 of "equity-SPX").
  - Constituent UUIDs are deterministic (uuid5 of "equity-{ticker}").
  - CreateOrUpdate on the index composition uses a deterministic composition UUID.
  - Safe to re-run; the in-memory store upserts on matching UUID.

Note:
  IndexCompositionService stores data IN MEMORY only. Compositions are lost
  when ledger-service restarts. Re-run this script after each restart.

Usage:
  python3 securities/load_sp500.py              # Load today's composition
  python3 securities/load_sp500.py --dry-run    # Preview without uploading
  python3 securities/load_sp500.py --date 2026-01-01  # Specific effective date
"""

import argparse
import calendar
import os
import sys
import time
import uuid
from datetime import date, datetime
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.expanduser("~/projects/ledger-models/ledger-models-python"))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LEDGER_SERVICE_HOST = "localhost:8082"
STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")

# Shared with equity_securities.py — must stay in sync
_EQUITY_NS = uuid.UUID("5e6f7a8b-9c0d-1e2f-3a4b-5c6d7e8f9a0b")

# Deterministic namespace for index composition records
_COMPOSITION_NS = uuid.UUID("d4e5f6a7-b8c9-0123-defa-bcdef0123456")

# Top-30 S&P 500 constituents by approximate market cap (Q1 2026)
# (ticker, company_name, weight_pct)
SP500_TOP30: List[Tuple[str, str, float]] = [
    ("AAPL",  "Apple Inc.",                        7.20),
    ("MSFT",  "Microsoft Corporation",             6.50),
    ("NVDA",  "NVIDIA Corporation",                6.10),
    ("AMZN",  "Amazon.com Inc.",                   3.80),
    ("GOOG",  "Alphabet Inc. Class C",             2.00),
    ("GOOGL", "Alphabet Inc. Class A",             1.90),
    ("META",  "Meta Platforms Inc.",               2.40),
    ("BRK-B", "Berkshire Hathaway Inc. Class B",   1.70),
    ("TSLA",  "Tesla Inc.",                        1.80),
    ("LLY",   "Eli Lilly and Company",             1.40),
    ("JPM",   "JPMorgan Chase & Co.",              1.35),
    ("V",     "Visa Inc.",                         1.10),
    ("UNH",   "UnitedHealth Group Incorporated",   1.05),
    ("XOM",   "Exxon Mobil Corporation",           1.00),
    ("AVGO",  "Broadcom Inc.",                     0.95),
    ("MA",    "Mastercard Incorporated",           0.90),
    ("JNJ",   "Johnson & Johnson",                 0.85),
    ("PG",    "Procter & Gamble Co.",              0.80),
    ("HD",    "The Home Depot Inc.",               0.78),
    ("COST",  "Costco Wholesale Corporation",      0.75),
    ("MRK",   "Merck & Co. Inc.",                  0.72),
    ("ABBV",  "AbbVie Inc.",                       0.70),
    ("CRM",   "Salesforce Inc.",                   0.65),
    ("BAC",   "Bank of America Corporation",       0.63),
    ("AMD",   "Advanced Micro Devices Inc.",       0.60),
    ("PEP",   "PepsiCo Inc.",                      0.58),
    ("KO",    "The Coca-Cola Company",             0.56),
    ("WMT",   "Walmart Inc.",                      0.55),
    ("NFLX",  "Netflix Inc.",                      0.54),
    ("ORCL",  "Oracle Corporation",                0.52),
]


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def _update_status(status: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Issue #135 — Load S&P 500 index composition into ledger-service\n"
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
# Proto helpers
# ---------------------------------------------------------------------------
def _make_local_date(d: date):
    from fintekkers.models.util.local_date_pb2 import LocalDateProto
    return LocalDateProto(year=d.year, month=d.month, day=d.day)


def _make_timestamp(d: date):
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
    dt = datetime(d.year, d.month, d.day)
    seconds = calendar.timegm(dt.timetuple())
    return LocalTimestampProto(
        timestamp=PbTimestamp(seconds=seconds, nanos=0),
        time_zone="UTC",
    )


# ---------------------------------------------------------------------------
# Security helpers
# ---------------------------------------------------------------------------
def _get_usd_cash():
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import CASH
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    id_proto = IdentifierProto(identifier_type=CASH, identifier_value="USD")
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    req_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService
    for sec in SecurityService().search(QuerySecurityRequest(proto=req_proto)):
        return sec.proto
    raise RuntimeError("USD cash security not found in ledger")


def _find_security_by_ticker(ticker: str) -> Optional[uuid.UUID]:
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    id_proto = IdentifierProto(identifier_type=EXCH_TICKER, identifier_value=ticker)
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    req_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    for sec in SecurityService().search(QuerySecurityRequest(proto=req_proto)):
        return uuid.UUID(bytes=bytes(sec.proto.uuid.raw_uuid))
    return None


def _create_equity_security(ticker: str, name: str, usd_cash) -> uuid.UUID:
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.security.security_type_pb2 import EQUITY_SECURITY
    from fintekkers.models.security.security_quantity_type_pb2 import UNITS
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.wrappers.models.security.security import Security
    from fintekkers.wrappers.requests.security import CreateSecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    sec_uuid = uuid.uuid5(_EQUITY_NS, f"equity-{ticker}")
    proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="America/New_York",
            timestamp=PbTimestamp(seconds=int(time.time()), nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
        security_type=EQUITY_SECURITY,
        asset_class="Equity",
        issuer_name=name,
        description=f"{name} Common Stock",
        quantity_type=UNITS,
        identifier=IdentifierProto(identifier_type=EXCH_TICKER, identifier_value=ticker),
        settlement_currency=usd_cash,
    )
    SecurityService().create_or_update(
        CreateSecurityRequest.create_or_update_request(Security(proto))
    )
    return sec_uuid


def ensure_spx_security(dry_run: bool):
    """Create or find the SPX index security. Returns (SecurityProto, uuid)."""
    from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.security.security_type_pb2 import EQUITY_INDEX_SECURITY
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.wrappers.models.security.security import Security
    from fintekkers.wrappers.requests.security import CreateSecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    spx_uuid = uuid.uuid5(_EQUITY_NS, "equity-SPX")

    if dry_run:
        proto = SecurityProto(
            object_class="Security",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=spx_uuid.bytes),
            is_link=True,
        )
        return proto, spx_uuid

    # Check if already in ledger
    existing = _find_security_by_ticker("SPX")
    if existing is not None:
        print(f"  SPX: already in ledger (uuid={str(existing)[:8]}...)")
        proto = SecurityProto(
            object_class="Security",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=existing.bytes),
            is_link=True,
        )
        return proto, existing

    # Create it — settlement_currency required by validator for all non-CASH types
    usd_cash = _get_usd_cash()
    proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="America/New_York",
            timestamp=PbTimestamp(seconds=int(time.time()), nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=spx_uuid.bytes),
        security_type=EQUITY_INDEX_SECURITY,
        asset_class="Equity",
        issuer_name="S&P Dow Jones Indices",
        description="S&P 500 Index",
        identifier=IdentifierProto(identifier_type=EXCH_TICKER, identifier_value="SPX"),
        settlement_currency=usd_cash,
    )
    SecurityService().create_or_update(
        CreateSecurityRequest.create_or_update_request(Security(proto))
    )
    print(f"  SPX: created EQUITY_INDEX_SECURITY (uuid={str(spx_uuid)[:8]}...)")
    link = SecurityProto(
        object_class="Security",
        version="0.0.1",
        uuid=UUIDProto(raw_uuid=spx_uuid.bytes),
        is_link=True,
    )
    return link, spx_uuid


# ---------------------------------------------------------------------------
# Resolve constituents
# ---------------------------------------------------------------------------
def resolve_constituents(
    dry_run: bool,
) -> List[Tuple[str, uuid.UUID, float]]:
    """Return (ticker, security_uuid, weight_pct) for all SP500_TOP30 members.

    Creates any missing equity securities in the ledger.
    In dry-run uses deterministic UUIDs without hitting the ledger.
    """
    result = []
    usd_cash = None

    for ticker, name, weight in SP500_TOP30:
        det_uuid = uuid.uuid5(_EQUITY_NS, f"equity-{ticker}")

        if dry_run:
            result.append((ticker, det_uuid, weight))
            continue

        existing = _find_security_by_ticker(ticker)
        if existing is not None:
            result.append((ticker, existing, weight))
        else:
            print(f"    {ticker}: not found — creating...")
            if usd_cash is None:
                usd_cash = _get_usd_cash()
            created = _create_equity_security(ticker, name, usd_cash)
            result.append((ticker, created, weight))
            print(f"    {ticker}: created (uuid={str(created)[:8]}...)")

    return result


# ---------------------------------------------------------------------------
# Build and submit composition
# ---------------------------------------------------------------------------
def create_composition(
    stub,
    spx_proto,
    spx_uuid: uuid.UUID,
    constituents: List[Tuple[str, uuid.UUID, float]],
    effective_date: date,
    dry_run: bool,
) -> Optional[uuid.UUID]:
    """Build IndexCompositionProto and call CreateOrUpdate.

    Returns the composition UUID on success, None on error.
    """
    from fintekkers.models.security.index_composition_pb2 import (
        IndexCompositionProto,
        IndexConstituentProto,
    )
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.index_composition.create_index_composition_request_pb2 import (
        CreateIndexCompositionRequestProto,
    )

    # Deterministic UUID: one composition per (index, effective_date)
    comp_uuid = uuid.uuid5(_COMPOSITION_NS, f"spx-composition-{effective_date.isoformat()}")

    constituent_protos = []
    for ticker, sec_uuid, weight_pct in constituents:
        sec_link = SecurityProto(
            object_class="Security",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
            is_link=True,
        )
        constituent_protos.append(
            IndexConstituentProto(
                security=sec_link,
                weight=DecimalValueProto(
                    arbitrary_precision_value=str(round(weight_pct / 100.0, 6))
                ),
            )
        )

    composition = IndexCompositionProto(
        object_class="IndexCompositionProto",
        version="0.0.1",
        uuid=UUIDProto(raw_uuid=comp_uuid.bytes),
        as_of=_make_timestamp(effective_date),
        index_security=spx_proto,
        effective_date=_make_local_date(effective_date),
        constituents=constituent_protos,
        notes=f"S&P 500 top-30 composition loaded on {datetime.utcnow().date().isoformat()}",
    )

    if dry_run:
        print(f"\n  [DRY RUN] IndexCompositionProto:")
        print(f"    uuid:           {str(comp_uuid)[:8]}...")
        print(f"    index_security: SPX (uuid={str(spx_uuid)[:8]}...)")
        print(f"    effective_date: {effective_date}")
        print(f"    constituents:   {len(constituent_protos)}")
        total_w = sum(w for _, _, w in constituents)
        print(f"    total weight:   {total_w:.2f}%")
        return comp_uuid

    request = CreateIndexCompositionRequestProto(
        object_class="CreateIndexCompositionRequestProto",
        version="0.0.1",
        create_index_composition_input=composition,
    )

    try:
        response = stub.CreateOrUpdate(request)
        stored = response.index_composition_response
        stored_uuid = uuid.UUID(bytes=bytes(stored.uuid.raw_uuid))
        return stored_uuid
    except Exception as e:
        print(f"  ERROR CreateOrUpdate: {e}")
        return None


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
def verify_composition(
    stub,
    spx_uuid: uuid.UUID,
    effective_date: date,
    expected_count: int,
) -> bool:
    """Call GetIndexComposition and print a summary."""
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.index_composition.get_index_composition_request_pb2 import (
        GetIndexCompositionRequestProto,
    )

    request = GetIndexCompositionRequestProto(
        object_class="GetIndexCompositionRequestProto",
        version="0.0.1",
        index_uuid=UUIDProto(raw_uuid=spx_uuid.bytes),
        as_of_date=_make_local_date(effective_date),
    )

    try:
        response = stub.GetIndexComposition(request)
        comp = response.composition
        n = len(comp.constituents)
        eff = comp.effective_date
        print(f"\n  Verification OK:")
        print(f"    effective_date: {eff.year}-{eff.month:02d}-{eff.day:02d}")
        print(f"    constituents:   {n}")
        # Print first 5
        for c in comp.constituents[:5]:
            w = float(c.weight.arbitrary_precision_value)
            sec_uuid_hex = c.security.uuid.raw_uuid.hex()
            print(f"      uuid={sec_uuid_hex[:8]}...  weight={w:.4f}")
        if n > 5:
            print(f"      ... ({n - 5} more)")
        return n == expected_count
    except Exception as e:
        print(f"  Verification FAILED: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(effective_date: date, dry_run: bool) -> None:
    _update_status("in_progress", "Resolving SPX index security...")

    import grpc
    from fintekkers.services.index_composition_service.index_composition_service_pb2_grpc import (
        IndexCompositionStub,
    )

    # Step 1: ensure SPX security exists
    print("Step 1: Resolving SPX index security...")
    spx_proto, spx_uuid = ensure_spx_security(dry_run)
    if dry_run:
        print(f"  [DRY RUN] SPX uuid={str(spx_uuid)[:8]}...")

    # Step 2: resolve/create constituent securities
    print(f"\nStep 2: Resolving {len(SP500_TOP30)} constituent securities...")
    constituents = resolve_constituents(dry_run)
    found = sum(1 for t, u, w in constituents)
    print(f"  {found} constituent securities resolved")

    # Step 3: connect to IndexCompositionService
    channel = None
    stub = None
    if not dry_run:
        channel = grpc.insecure_channel(LEDGER_SERVICE_HOST)
        stub = IndexCompositionStub(channel)
        print(f"\nConnected to IndexCompositionService at {LEDGER_SERVICE_HOST}")

    # Step 4: create composition
    print(f"\nStep 3: Creating index composition (effective_date={effective_date})...")
    comp_uuid = create_composition(
        stub, spx_proto, spx_uuid, constituents, effective_date, dry_run
    )

    if comp_uuid is None:
        _update_status("blocked", "CreateOrUpdate failed — see console output")
        if channel:
            channel.close()
        return

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"  {prefix}Composition stored (uuid={str(comp_uuid)[:8]}...)")

    # Step 5: verify
    if not dry_run:
        print(f"\nStep 4: Verifying via GetIndexComposition...")
        ok = verify_composition(stub, spx_uuid, effective_date, len(SP500_TOP30))
        if not ok:
            print("  WARNING: constituent count mismatch")
    else:
        print(f"\n  [DRY RUN] Skipping verification (no data written)")

    if channel:
        channel.close()

    _update_status(
        "review",
        (
            f"{prefix}S&P 500 index composition loaded. "
            f"SPX uuid={str(spx_uuid)[:8]}..., "
            f"{len(SP500_TOP30)} constituents, effective_date={effective_date}."
        ),
    )
    print(f"\n{prefix}Done.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load S&P 500 index composition into ledger-service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 securities/load_sp500.py                    # Today as effective date
  python3 securities/load_sp500.py --date 2026-01-01  # Specific effective date
  python3 securities/load_sp500.py --dry-run          # Preview
        """,
    )
    parser.add_argument(
        "--date",
        dest="effective_date",
        type=str,
        default=date.today().isoformat(),
        help="Effective date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing to ledger-service",
    )
    args = parser.parse_args()

    effective_date = datetime.strptime(args.effective_date, "%Y-%m-%d").date()

    print(f"S&P 500 Index Composition Loader")
    print(f"  Effective date:  {effective_date}")
    print(f"  Constituents:    {len(SP500_TOP30)} (top-30 by approx market cap)")
    print(f"  Service:         {LEDGER_SERVICE_HOST}")
    print(f"  Dry run:         {args.dry_run}")
    print()

    run(effective_date=effective_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
