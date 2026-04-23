"""
Historical FedInvest Price Backfill (gRPC)
==========================================

Downloads daily per-CUSIP Treasury security prices from FedInvest and uploads
them via the PriceService gRPC API (localhost:8083).

Security types covered: MARKET BASED BOND, MARKET BASED NOTE, MARKET BASED FRN,
MARKET BASED BILL, TIPS.

Resumable: saves a checkpoint after each trading day so the script can be
killed and restarted without re-uploading already-processed dates.

Usage:
  python3 prices/backfill_fedinvest.py                     # Full backfill (resumes from checkpoint)
  python3 prices/backfill_fedinvest.py --from 2015-01-01   # Explicit start (overrides checkpoint)
  python3 prices/backfill_fedinvest.py --dry-run            # Preview only
"""

import argparse
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta


# Allow running from repo root: python3 prices/backfill_fedinvest.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fedinvest import (
    DB_CONFIG,
    fetch_prices_for_date,
    _make_timestamp,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BACKFILL_START = date(2014, 1, 27)
BACKFILL_END = date(2025, 12, 31)
REQUEST_DELAY = 1.0  # seconds between FedInvest requests
PRICE_SERVICE_HOST = "localhost:8083"
STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")
CHECKPOINT_FILE = os.path.expanduser("~/second-brain/status/price_backfill_checkpoint.txt")
STATUS_UPDATE_INTERVAL = 300  # 5 minutes between status file writes


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
def _load_checkpoint():
    """Return the last successfully processed date, or None."""
    try:
        with open(CHECKPOINT_FILE) as f:
            text = f.read().strip()
            return datetime.strptime(text, "%Y-%m-%d").date()
    except (FileNotFoundError, ValueError):
        return None


def _save_checkpoint(d: date) -> None:
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(d.isoformat())
    except Exception:
        pass


# ---------------------------------------------------------------------------
# CUSIP → security UUID lookup (raw Postgres, fast startup)
# ---------------------------------------------------------------------------
def _build_cusip_lookup() -> dict[str, str]:
    """CUSIP -> security UUID (as string) from the security table."""
    import psycopg2

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT primarykey, binarydata FROM security")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    lookup = {}
    for pk, bdata in rows:
        if bdata:
            text = bytes(bdata).decode("latin-1", errors="ignore")
            m = re.search(r"[A-Z0-9]{9}", text)
            if m:
                lookup[m.group()] = str(pk)
    return lookup


# ---------------------------------------------------------------------------
# Status file
# ---------------------------------------------------------------------------
def _update_status(
    total_upserted: int, days_processed: int, current_date: date, errors: int
) -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: in_progress\n"
        f"task: Backfill historical prices via PriceService gRPC API\n"
        f"updated: {now}\n"
        f"blockers: none\n"
        f"progress: {total_upserted} prices inserted, {days_processed} days processed, "
        f"currently at {current_date.isoformat()}, {errors} errors\n"
    )
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(content)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main backfill loop
# ---------------------------------------------------------------------------
def backfill(start: date, end: date, dry_run: bool = False) -> None:
    import grpc
    from fintekkers.models.security.security_pb2 import SecurityProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.wrappers.requests.price import CreatePriceRequest

    cusip_lookup = _build_cusip_lookup()
    print(f"Loaded {len(cusip_lookup)} CUSIP -> security UUID mappings")

    stub = None
    channel = None
    if not dry_run:
        channel = grpc.insecure_channel(PRICE_SERVICE_HOST)
        from fintekkers.services.price_service.price_service_pb2_grpc import PriceStub
        stub = PriceStub(channel)
        print(f"Connected to PriceService at {PRICE_SERVICE_HOST}")

    current = start
    total_upserted = 0
    total_skipped_cusip = 0
    days_processed = 0
    days_no_data = 0
    errors = 0
    last_status_update = time.time()

    while current <= end:
        if current.weekday() >= 5:  # skip weekends
            current += timedelta(days=1)
            continue

        if days_processed > 0:
            time.sleep(REQUEST_DELAY)

        try:
            prices = fetch_prices_for_date(current)
        except Exception as e:
            print(f"  ERROR fetching {current}: {e}")
            errors += 1
            current += timedelta(days=1)
            continue

        if not prices:
            days_no_data += 1
            current += timedelta(days=1)
            _save_checkpoint(current - timedelta(days=1))
            continue

        day_upserted = 0

        for p in prices:
            sec_uuid_str = cusip_lookup.get(p.cusip)
            if not sec_uuid_str:
                total_skipped_cusip += 1
                continue

            price_value = p.eod_price if p.eod_price > 0 else p.sell_price
            if price_value <= 0:
                price_value = p.buy_price
            if price_value <= 0:
                continue

            sec_uuid = uuid.UUID(sec_uuid_str)
            # Deterministic UUID for idempotency
            price_uuid = uuid.uuid5(
                uuid.UUID("00000000-0000-0000-0000-000000000000"),
                f"fedinvest-{p.cusip}-{p.price_date.isoformat()}",
            )

            if dry_run:
                day_upserted += 1
                continue

            # Build SecurityProto link (by UUID only — no round-trip lookup needed)
            security_proto = SecurityProto(
                object_class="Security",
                version="0.0.1",
                uuid=UUIDProto(raw_uuid=sec_uuid.bytes),
                is_link=True,
            )

            # Use the ledger-models wrapper for proto construction
            request = CreatePriceRequest.create_or_update_request(
                security=security_proto,
                price=price_value,
                as_of_date=datetime(
                    p.price_date.year, p.price_date.month, p.price_date.day
                ),
                price_uuid=price_uuid,
            )
            try:
                stub.CreateOrUpdate(request.proto)
                day_upserted += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  gRPC ERROR: {p.cusip} {p.price_date}: {e}")

        total_upserted += day_upserted
        days_processed += 1

        # Save checkpoint after every successfully processed day
        _save_checkpoint(current)

        if days_processed % 20 == 0:
            print(
                f"  Progress: {days_processed} days, {total_upserted} upserted, "
                f"{errors} errors, at {current}"
            )

        now = time.time()
        if now - last_status_update >= STATUS_UPDATE_INTERVAL:
            _update_status(total_upserted, days_processed, current, errors)
            last_status_update = now

        current += timedelta(days=1)

    if channel:
        channel.close()

    _update_status(total_upserted, days_processed, end, errors)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Backfill complete:")
    print(f"  Days processed:       {days_processed}")
    print(f"  Days no data:         {days_no_data}")
    print(f"  Prices upserted:      {total_upserted}")
    print(f"  Skipped (no CUSIP):   {total_skipped_cusip}")
    print(f"  gRPC errors:          {errors}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    checkpoint = _load_checkpoint()
    default_start = (checkpoint + timedelta(days=1)) if checkpoint else BACKFILL_START

    parser = argparse.ArgumentParser(description="Backfill FedInvest historical prices via gRPC")
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=default_start.isoformat(),
        help=f"Start date (default: {default_start} — from checkpoint)" if checkpoint
        else f"Start date (default: {BACKFILL_START})",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=BACKFILL_END.isoformat(),
        help=f"End date (default: {BACKFILL_END})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    args = parser.parse_args()

    start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.to_date, "%Y-%m-%d").date()

    biz_days = sum(
        1
        for d in range((end - start).days + 1)
        if (start + timedelta(days=d)).weekday() < 5
    )
    est_time = biz_days * REQUEST_DELAY

    if checkpoint:
        print(f"Resuming from checkpoint: last completed {checkpoint.isoformat()}")
    print(f"Backfill range: {start} to {end}")
    print(f"Estimated business days: {biz_days}")
    print(f"Estimated time: {est_time:.0f}s ({est_time / 60:.1f} min)")
    print(f"Using PriceService gRPC at {PRICE_SERVICE_HOST}")
    print(f"Checkpoint file: {CHECKPOINT_FILE}")
    print(f"Status update interval: {STATUS_UPDATE_INTERVAL}s")
    print()

    _update_status(0, 0, start, 0)
    backfill(start, end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
