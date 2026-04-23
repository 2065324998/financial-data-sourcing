"""
Equity Securities — S&P 500, Nasdaq-100, Dow Jones
===================================================

Downloads current index constituents from Wikipedia and creates SecurityProto
records via SecurityService gRPC (localhost:8082).

Idempotent: checks for an existing EXCH_TICKER match before creating. Safe to
re-run; already-present securities are counted as skipped, not re-uploaded.

Usage:
  python3 equity_securities.py            # Create all three indices
  python3 equity_securities.py --dry-run  # Preview without uploading
  python3 equity_securities.py --index "S&P 500"  # Single index

Data source: Wikipedia (pandas.read_html)
  S&P 500:   https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
  Nasdaq-100: https://en.wikipedia.org/wiki/Nasdaq-100
  Dow Jones:  https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average
"""

import argparse
import os
import sys
import time
import uuid as _uuid
from datetime import datetime



import pandas as pd
import requests

from equity.sources import WikipediaSource, LiveWikipediaSource, MockWikipediaSource
from google.protobuf.timestamp_pb2 import Timestamp

from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.security.security_type_pb2 import EQUITY_SECURITY
from fintekkers.models.security.security_quantity_type_pb2 import UNITS
from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
from fintekkers.models.security.identifier.identifier_type_pb2 import EXCH_TICKER
from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
from fintekkers.models.util.uuid_pb2 import UUIDProto
from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
from fintekkers.models.position.position_util_pb2 import FieldMapEntry
from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
from fintekkers.wrappers.models.security.security import Security
from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
from fintekkers.wrappers.requests.security import CreateSecurityRequest
from fintekkers.wrappers.services.security import SecurityService

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")
STATUS_UPDATE_INTERVAL = 300  # seconds

# Deterministic UUID namespace — fixed so repeated runs yield the same UUID per ticker
_EQUITY_NS = _uuid.UUID("5e6f7a8b-9c0d-1e2f-3a4b-5c6d7e8f9a0b")


# ---------------------------------------------------------------------------
# Wikipedia constituent fetchers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def _wiki_tables(url: str) -> list[pd.DataFrame]:
    """Fetch Wikipedia page and parse HTML tables, bypassing 403 blocks."""
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return pd.read_html(resp.text)


def _fetch_sp500() -> list[tuple[str, str]]:
    """Return list of (ticker, name) for current S&P 500 constituents."""
    tables = _wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    # The constituents table is always the first table on the page
    df = tables[0]
    df.columns = [c.strip() for c in df.columns]
    ticker_col = next(c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower())
    name_col = next(c for c in df.columns if "security" in c.lower() or "company" in c.lower())
    return [(str(row[ticker_col]).strip(), str(row[name_col]).strip()) for _, row in df.iterrows()]


def _fetch_nasdaq100() -> list[tuple[str, str]]:
    """Return list of (ticker, name) for current Nasdaq-100 constituents."""
    tables = _wiki_tables("https://en.wikipedia.org/wiki/Nasdaq-100")
    # Find the table that has a ticker/symbol column
    for df in tables:
        df.columns = [str(c).strip() for c in df.columns]
        ticker_candidates = [c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()]
        name_candidates = [c for c in df.columns if "company" in c.lower() or "name" in c.lower() or "security" in c.lower()]
        if ticker_candidates and name_candidates and len(df) > 50:
            ticker_col, name_col = ticker_candidates[0], name_candidates[0]
            return [(str(row[ticker_col]).strip(), str(row[name_col]).strip()) for _, row in df.iterrows()]
    raise ValueError("Could not find Nasdaq-100 constituents table on Wikipedia")


def _fetch_dow() -> list[tuple[str, str]]:
    """Return list of (ticker, name) for current Dow Jones Industrial Average constituents."""
    tables = _wiki_tables("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")
    for df in tables:
        df.columns = [str(c).strip() for c in df.columns]
        ticker_candidates = [c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower()]
        name_candidates = [c for c in df.columns if "company" in c.lower() or "name" in c.lower()]
        # Dow has exactly 30 components
        if ticker_candidates and name_candidates and 25 <= len(df) <= 35:
            ticker_col, name_col = ticker_candidates[0], name_candidates[0]
            return [(str(row[ticker_col]).strip(), str(row[name_col]).strip()) for _, row in df.iterrows()]
    raise ValueError("Could not find Dow Jones constituents table on Wikipedia")


def _get_indices(source: WikipediaSource = None):
    source = source or LiveWikipediaSource()
    return {
        "S&P 500": source.fetch_sp500,
        "Nasdaq-100": source.fetch_nasdaq100,
        "Dow Jones": source.fetch_dow,
    }

INDICES = _get_indices()


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def _update_status(status: str, task: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: {task}\n"
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
# Security creation
# ---------------------------------------------------------------------------

def _get_usd_cash_security() -> SecurityProto:
    """Fetch the USD cash security proto (used as settlement_currency on equities)."""
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.security.identifier.identifier_type_pb2 import CASH
    id_proto = IdentifierProto(identifier_type=CASH, identifier_value="USD")
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    as_of_proto = ProtoSerializationUtil.serialize(datetime.now())
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=as_of_proto,
    )
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    request = QuerySecurityRequest(proto=request_proto)
    for sec in SecurityService().search(request):
        return sec.proto
    raise RuntimeError("USD cash security not found in ledger — cannot set settlement_currency")


def _make_security_proto(ticker: str, name: str, usd_cash: SecurityProto) -> SecurityProto:
    """Build a SecurityProto for an equity with a deterministic UUID."""
    sec_uuid = _uuid.uuid5(_EQUITY_NS, f"equity-{ticker}")
    ts_seconds = int(time.time())

    return SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="America/New_York",
            timestamp=Timestamp(seconds=ts_seconds, nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
        security_type=EQUITY_SECURITY,
        asset_class="Equity",
        issuer_name=name,
        description=f"{name} Common Stock",
        quantity_type=UNITS,
        identifier=IdentifierProto(
            identifier_type=EXCH_TICKER,
            identifier_value=ticker,
        ),
        settlement_currency=usd_cash,
    )


def _security_exists(ticker: str) -> bool:
    """Return True if a security with this EXCH_TICKER already exists in the ledger."""
    from google.protobuf.any_pb2 import Any

    id_proto = IdentifierProto(identifier_type=EXCH_TICKER, identifier_value=ticker)
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    as_of_proto = ProtoSerializationUtil.serialize(datetime.now())
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=as_of_proto,
    )
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    request = QuerySecurityRequest(proto=request_proto)
    for _ in SecurityService().search(request):
        return True
    return False


def _create_equity(ticker: str, name: str, usd_cash: SecurityProto) -> bool:
    """Create the security. Returns True on success."""
    proto = _make_security_proto(ticker, name, usd_cash)
    security = Security(proto)
    request = CreateSecurityRequest.create_or_update_request(security)
    SecurityService().create_or_update(request)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_index(
    index_name: str,
    constituents: list[tuple[str, str]],
    dry_run: bool,
    usd_cash: SecurityProto,
    total_created: list,
    total_skipped: list,
    total_errors: list,
    last_status: list,
) -> dict:
    """Process one index. Returns per-index counts."""
    created = skipped = errors = 0

    for ticker, name in constituents:
        # Skip junk rows (headers repeated mid-table, etc.)
        if not ticker or ticker.lower() in ("ticker", "symbol", "nan"):
            continue

        try:
            if _security_exists(ticker):
                skipped += 1
            elif dry_run:
                print(f"  [DRY RUN] Would create: {ticker} — {name}")
                created += 1
            else:
                _create_equity(ticker, name, usd_cash)
                print(f"  Created: {ticker} — {name}")
                created += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR {ticker}: {e}")

        # Periodic status update
        if time.time() - last_status[0] >= STATUS_UPDATE_INTERVAL:
            _update_status(
                status="in_progress",
                task="Create equity securities for S&P 500, Nasdaq-100, Dow Jones",
                progress=(
                    f"Processing {index_name}, at {ticker}. "
                    f"Running totals — created: {total_created[0] + created}, "
                    f"skipped: {total_skipped[0] + skipped}, errors: {total_errors[0] + errors}"
                ),
            )
            last_status[0] = time.time()

    return {"created": created, "skipped": skipped, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(description="Create equity securities for major US indices")
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("--mock", action="store_true", help="Use mock data from fixtures/")
    parser.add_argument(
        "--index",
        choices=list(INDICES.keys()),
        default=None,
        help="Process a single index (default: all)",
    )
    args = parser.parse_args()

    wiki_source = MockWikipediaSource() if args.mock else LiveWikipediaSource()
    all_indices = _get_indices(wiki_source)
    indices_to_run = {args.index: all_indices[args.index]} if args.index else all_indices

    _update_status(
        status="in_progress",
        task="Create equity securities for S&P 500, Nasdaq-100, Dow Jones",
        progress="Fetching constituent lists from Wikipedia...",
    )

    # --- Fetch USD cash security (required as settlement_currency on all equities) ---
    usd_cash: SecurityProto | None = None
    if not args.dry_run:
        print("Fetching USD cash security from ledger...")
        usd_cash = _get_usd_cash_security()
        print(f"  USD cash security loaded")

    # --- Fetch all constituent data up front ---
    index_data: dict[str, list[tuple[str, str]]] = {}
    for name, fetcher in indices_to_run.items():
        print(f"Fetching {name} constituents from Wikipedia...")
        try:
            constituents = fetcher()
            index_data[name] = constituents
            print(f"  {len(constituents)} constituents found")
        except Exception as e:
            print(f"  ERROR fetching {name}: {e}")
            index_data[name] = []

    # --- Shared mutable state for status updates across indices ---
    total_created = [0]
    total_skipped = [0]
    total_errors = [0]
    last_status = [time.time()]

    results: dict[str, dict] = {}

    for index_name, constituents in index_data.items():
        print(f"\n--- {index_name} ({len(constituents)} constituents) ---")

        counts = process_index(
            index_name=index_name,
            constituents=constituents,
            dry_run=args.dry_run,
            usd_cash=usd_cash,
            total_created=total_created,
            total_skipped=total_skipped,
            total_errors=total_errors,
            last_status=last_status,
        )
        results[index_name] = counts
        total_created[0] += counts["created"]
        total_skipped[0] += counts["skipped"]
        total_errors[0] += counts["errors"]

        print(
            f"  {index_name}: created={counts['created']}, "
            f"skipped={counts['skipped']}, errors={counts['errors']}"
        )

    # --- Final summary ---
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Equity securities upload complete:")
    for index_name, counts in results.items():
        print(
            f"  {index_name:12s}  created={counts['created']:4d}  "
            f"skipped={counts['skipped']:4d}  errors={counts['errors']:3d}"
        )
    print(
        f"  {'TOTAL':12s}  created={total_created[0]:4d}  "
        f"skipped={total_skipped[0]:4d}  errors={total_errors[0]:3d}"
    )

    _update_status(
        status="review",
        task="Create equity securities for S&P 500, Nasdaq-100, Dow Jones",
        progress=(
            f"Complete{'(dry run)' if args.dry_run else ''}. "
            + ", ".join(
                f"{n}: created={c['created']} skipped={c['skipped']} errors={c['errors']}"
                for n, c in results.items()
            )
        ),
    )


if __name__ == "__main__":
    main()
