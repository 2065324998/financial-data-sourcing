"""
Yahoo Finance Equity Price Loader
==================================

Downloads daily OHLCV closing prices for equity tickers via yfinance and
uploads them to price-service-rust (:8083) via PriceService.CreateOrUpdate.

Securities are looked up in ledger-service by EXCH_TICKER identifier. If a
security is not found, the script creates it automatically using the same
UUID namespace as equity_securities.py, so the two scripts are compatible.

Idempotency:
  Price UUIDs are deterministic: uuid5(_YAHOO_PRICE_NS, f"yahoo-{ticker}-{date}").
  Re-running is safe — PriceService.CreateOrUpdate is a no-op for duplicate UUIDs.

Usage:
  python3 prices/yahoo_prices.py --tickers AAPL MSFT GOOG   # Today as end date
  python3 prices/yahoo_prices.py --tickers AAPL --from 2026-01-01 --to 2026-03-23
  python3 prices/yahoo_prices.py --tickers AAPL MSFT --dry-run
"""

import argparse
import calendar
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from equity.sources import YahooFinanceSource, LiveYahooFinanceSource, MockYahooFinanceSource



# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECURITY_SERVICE_HOST = "localhost:8082"
PRICE_SERVICE_HOST = "localhost:8083"
STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")

# Shared namespace with equity_securities.py — must stay in sync
_EQUITY_NS = uuid.UUID("5e6f7a8b-9c0d-1e2f-3a4b-5c6d7e8f9a0b")
# Separate namespace for price records
_YAHOO_PRICE_NS = uuid.UUID("c1d2e3f4-a5b6-7890-cdef-012345678901")


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def _update_status(status: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Issue #134 — Equity price loader from Yahoo Finance\n"
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
# yfinance download
# ---------------------------------------------------------------------------
def download_prices(
    tickers: List[str], start: date, end: date, source: YahooFinanceSource = None
) -> Dict[str, Dict[date, float]]:
    """Download adjusted close prices for all tickers in one yfinance call.

    Returns {ticker: {date: close_price}}.
    Missing dates (weekends/holidays) are excluded.
    """
    source = source or LiveYahooFinanceSource()
    print(f"Downloading prices ({start} to {end})...")
    raw = source.download_prices(tickers, start, end)

    # Convert ISO date strings back to date objects for downstream consumers
    result: Dict[str, Dict[date, float]] = {}
    for ticker in tickers:
        ticker_data = raw.get(ticker, {})
        result[ticker] = {}
        for d_str, price in ticker_data.items():
            if isinstance(d_str, str):
                result[ticker][datetime.strptime(d_str, "%Y-%m-%d").date()] = price
            else:
                result[ticker][d_str] = price
        print(f"  {ticker}: {len(result[ticker])} trading days")

    return result


# ---------------------------------------------------------------------------
# Security lookup / create
# ---------------------------------------------------------------------------
def _find_security_uuid(ticker: str) -> Optional[uuid.UUID]:
    """Look up an equity security by EXCH_TICKER; return its UUID or None."""
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
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)
    for sec in SecurityService().search(request):
        raw = sec.proto.uuid.raw_uuid
        return uuid.UUID(bytes=bytes(raw))
    return None


def _get_usd_cash():
    """Fetch the USD cash SecurityProto (used as settlement_currency)."""
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
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)
    for sec in SecurityService().search(request):
        return sec.proto
    raise RuntimeError("USD cash security not found in ledger")


def _create_equity_security(ticker: str, usd_cash) -> uuid.UUID:
    """Create an EQUITY_SECURITY in the ledger. Returns the new security UUID."""
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
    ts_seconds = int(time.time())

    proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="America/New_York",
            timestamp=PbTimestamp(seconds=ts_seconds, nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
        security_type=EQUITY_SECURITY,
        asset_class="Equity",
        issuer_name=ticker,
        description=f"{ticker} Common Stock",
        quantity_type=UNITS,
        identifier=IdentifierProto(
            identifier_type=EXCH_TICKER,
            identifier_value=ticker,
        ),
        settlement_currency=usd_cash,
    )

    security = Security(proto)
    request = CreateSecurityRequest.create_or_update_request(security)
    SecurityService().create_or_update(request)
    return sec_uuid


def resolve_securities(
    tickers: List[str], dry_run: bool
) -> Dict[str, uuid.UUID]:
    """Return {ticker: security_uuid} for all tickers.

    Looks up each ticker; creates the security if absent (unless dry-run).
    In dry-run, uses the deterministic UUID without hitting the ledger.
    """
    result: Dict[str, uuid.UUID] = {}
    usd_cash = None

    for ticker in tickers:
        det_uuid = uuid.uuid5(_EQUITY_NS, f"equity-{ticker}")

        if dry_run:
            result[ticker] = det_uuid
            print(f"  {ticker}: [DRY RUN] uuid={str(det_uuid)[:8]}...")
            continue

        existing = _find_security_uuid(ticker)
        if existing is not None:
            result[ticker] = existing
            print(f"  {ticker}: found in ledger (uuid={str(existing)[:8]}...)")
        else:
            print(f"  {ticker}: not found — creating equity security...")
            if usd_cash is None:
                usd_cash = _get_usd_cash()
            created_uuid = _create_equity_security(ticker, usd_cash)
            result[ticker] = created_uuid
            print(f"  {ticker}: created (uuid={str(created_uuid)[:8]}...)")

    return result


# ---------------------------------------------------------------------------
# Price upload
# ---------------------------------------------------------------------------
def _upload_price(
    stub,
    ticker: str,
    security_uuid: uuid.UUID,
    price_date: date,
    close_price: float,
    dry_run: bool,
) -> bool:
    price_uuid = uuid.uuid5(_YAHOO_PRICE_NS, f"yahoo-{ticker}-{price_date.isoformat()}")
    as_of = _make_timestamp(price_date)

    if dry_run:
        print(f"    [DRY RUN] {ticker} {price_date}: {close_price:.4f} (uuid={str(price_uuid)[:8]}...)")
        return True

    from fintekkers.models.price.price_pb2 import PriceProto
    from fintekkers.models.price.price_type_pb2 import ABSOLUTE
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.decimal_value_pb2 import DecimalValueProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto

    request = CreatePriceRequestProto(
        object_class="CreatePriceRequestProto",
        version="0.0.1",
        create_price_input=PriceProto(
            object_class="PriceProto",
            version="0.0.1",
            uuid=UUIDProto(raw_uuid=price_uuid.bytes),
            as_of=as_of,
            valid_from=as_of,
            price=DecimalValueProto(arbitrary_precision_value=str(round(close_price, 6))),
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
        print(f"    gRPC error {ticker} {price_date}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main backfill
# ---------------------------------------------------------------------------
def run(
    tickers: List[str],
    start: date,
    end: date,
    dry_run: bool,
    source: YahooFinanceSource = None,
) -> None:
    _update_status("in_progress", f"Resolving {len(tickers)} equity securities...")

    # Step 1: resolve / create securities
    print("\nResolving equity securities...")
    securities = resolve_securities(tickers, dry_run)

    # Step 2: download prices from Yahoo Finance
    print()
    price_data = download_prices(tickers, start, end, source=source)

    # Step 3: connect to PriceService
    stub = None
    channel = None
    if not dry_run:
        import grpc
        from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub
        channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
        stub = PriceStub(channel)
        print(f"\nConnected to PriceService at {PRICE_SERVICE_HOST}")

    # Step 4: upload prices
    print(f"\nUploading prices ({start} to {end})...\n")
    total_uploaded = 0
    total_skipped = 0
    total_errors = 0

    for ticker in tickers:
        sec_uuid = securities.get(ticker)
        if sec_uuid is None:
            print(f"  {ticker}: no security UUID — skipping")
            continue

        prices_for_ticker = price_data.get(ticker, {})
        if not prices_for_ticker:
            print(f"  {ticker}: no price data from Yahoo Finance")
            total_skipped += 1
            continue

        ticker_uploaded = 0
        ticker_errors = 0

        # Upload in date order
        for price_date in sorted(prices_for_ticker):
            if price_date < start or price_date > end:
                continue
            close_price = prices_for_ticker[price_date]
            ok = _upload_price(stub, ticker, sec_uuid, price_date, close_price, dry_run)
            if ok:
                ticker_uploaded += 1
            else:
                ticker_errors += 1

        prefix = "[DRY RUN] " if dry_run else ""
        print(
            f"  {prefix}{ticker}: {ticker_uploaded} prices uploaded"
            + (f", {ticker_errors} errors" if ticker_errors else "")
        )
        total_uploaded += ticker_uploaded
        total_errors += ticker_errors

    if channel:
        channel.close()

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Done: {total_uploaded} prices uploaded, {total_errors} errors")

    _update_status(
        "review",
        (
            f"{prefix}Complete. {total_uploaded} equity prices uploaded "
            f"for {len(tickers)} tickers ({start} to {end}). "
            f"Tickers: {', '.join(tickers)}. Errors: {total_errors}."
        ),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load daily equity closing prices from Yahoo Finance into FinTekkers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prices/yahoo_prices.py --tickers AAPL MSFT GOOG BRK-B --from 2026-01-01
  python3 prices/yahoo_prices.py --tickers AAPL --from 2025-01-01 --to 2025-12-31
  python3 prices/yahoo_prices.py --tickers AAPL MSFT --dry-run
        """,
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="One or more Yahoo Finance ticker symbols (e.g. AAPL MSFT BRK-B)",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=(date.today() - timedelta(days=30)).isoformat(),
        help="Start date YYYY-MM-DD (default: 30 days ago)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("--mock", action="store_true", help="Use mock data from fixtures/")
    args = parser.parse_args()

    start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.to_date, "%Y-%m-%d").date()

    if start > end:
        print(f"ERROR: --from ({start}) must be on or before --to ({end})")
        sys.exit(1)

    print(f"Yahoo Finance Equity Price Loader")
    print(f"  Tickers:        {' '.join(args.tickers)}")
    print(f"  Date range:     {start} to {end}")
    print(f"  SecurityService:{SECURITY_SERVICE_HOST}")
    print(f"  PriceService:   {PRICE_SERVICE_HOST}")
    print(f"  Dry run:        {args.dry_run}")

    yahoo_source = MockYahooFinanceSource() if args.mock else LiveYahooFinanceSource()
    run(tickers=args.tickers, start=start, end=end, dry_run=args.dry_run, source=yahoo_source)


if __name__ == "__main__":
    main()
