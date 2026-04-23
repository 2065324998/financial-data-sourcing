"""
collect_data.py — Full pipeline: download → convert → upload UST securities
=============================================================================

Runs three steps serially:
  1. Download XML auction files from TreasuryDirect into data/raw_xml/
  2. Convert XML → JSON into data/raw_json/
  3. Upload all securities into the ledger-service via gRPC

Usage:
  # Full pipeline (default)
  API_URL=localhost python3 collect_data.py

  # Skip download (use existing data/raw_xml/)
  API_URL=localhost python3 collect_data.py --skip-download

  # Skip download and conversion (use existing data/raw_json/)
  API_URL=localhost python3 collect_data.py --skip-download --skip-convert

  # Dry run: show what securities would be uploaded without uploading
  API_URL=localhost python3 collect_data.py --dry-run

  # After securities are loaded, re-run the price backfill
  python3 prices/backfill_fedinvest.py
"""

import argparse
import json
import multiprocessing as mp
import os
import random
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.expanduser("~/projects/ledger-models/ledger-models-python"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from bond.convert_xml import convert_xml_to_json
from bond.treasury_auction import RawAuctionData
from bond.treasury import (
    get_security_by_id,
    upload_security_from_data_dict,
    _get_security_types_from_auction_data,
)
from fintekkers.models.security.identifier.identifier_type_pb2 import IdentifierTypeProto

STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")
STATUS_UPDATE_INTERVAL = 120  # seconds

TREASURYDIRECT_XML_BASE = "https://www.treasurydirect.gov/xml"


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def update_status(progress: str, status: str = "in_progress", blockers: str = "none"):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Download, convert, and upload all UST securities\n"
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
# Step 1: Download
# ---------------------------------------------------------------------------

def _download_file(href: str):
    dest = os.path.join("data/raw_xml", href)
    if os.path.exists(dest) or "DM_" in href:
        return
    try:
        resp = requests.get(f"{TREASURYDIRECT_XML_BASE}/{href}", timeout=30)
        with open(dest, "wb") as f:
            f.write(resp.content)
        print(f"  Downloaded: {href}")
    except Exception as e:
        print(f"  ERROR downloading {href}: {e}")


def download_xml(dry_run: bool = False):
    """Scrape TreasuryDirect XML index and download any new files to data/raw_xml/."""
    print("=" * 60)
    print("Step 1: Download XML from TreasuryDirect")
    print("=" * 60)

    os.makedirs("data/raw_xml", exist_ok=True)

    update_status("Fetching TreasuryDirect XML index")
    response = requests.get(TREASURYDIRECT_XML_BASE, timeout=30)
    soup = BeautifulSoup(response.text, "html.parser")
    hrefs = [a.attrs["href"] for a in soup.find_all("a") if "href" in a.attrs]
    random.shuffle(hrefs)

    new_files = [h for h in hrefs if not os.path.exists(os.path.join("data/raw_xml", h)) and "DM_" not in h]
    print(f"  {len(hrefs)} files listed, {len(new_files)} new to download")

    if dry_run:
        print(f"  [DRY RUN] Would download {len(new_files)} files")
        return

    with mp.Pool(processes=4) as pool:
        pool.map(_download_file, hrefs)

    print(f"  Download complete\n")


# ---------------------------------------------------------------------------
# Step 2: Convert XML → JSON
# ---------------------------------------------------------------------------

def convert_all_xml(dry_run: bool = False):
    """Convert all XML files in data/raw_xml/ to JSON in data/raw_json/."""
    print("=" * 60)
    print("Step 2: Convert XML → JSON")
    print("=" * 60)

    os.makedirs("data/raw_json", exist_ok=True)

    files = os.listdir("data/raw_xml")
    xml_files = [f for f in files if f.lower().endswith(".xml")]
    print(f"  {len(xml_files)} XML files to process")

    if dry_run:
        print(f"  [DRY RUN] Would convert {len(xml_files)} XML files")
        return

    update_status(f"Converting {len(xml_files)} XML files to JSON")

    with mp.Pool(processes=100) as pool:
        pool.map(convert_xml_to_json, xml_files)

    json_count = len([f for f in os.listdir("data/raw_json") if f.endswith(".json")])
    print(f"  Conversion complete. {json_count} JSON files in data/raw_json/\n")


# ---------------------------------------------------------------------------
# Step 3: Collect and upload securities
# ---------------------------------------------------------------------------

def collect_securities_from_json() -> list[tuple[str, RawAuctionData]]:
    """Scan data/raw_json/ and return (filename, RawAuctionData) for all entries.

    Groups by CUSIP and picks the entry with the earliest issue date per CUSIP
    (the initial auction, not reopenings).
    """
    json_dir = "data/raw_json"
    if not os.path.isdir(json_dir):
        print(f"ERROR: {json_dir} directory not found")
        return []

    cusip_files: dict[str, list[tuple[str, RawAuctionData]]] = {}

    for filename in os.listdir(json_dir):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(json_dir, filename)
        try:
            with open(filepath) as f:
                raw = json.load(f)
            data = RawAuctionData.from_json(json.dumps(raw))
        except Exception:
            continue

        cusip = data.cusip
        if not cusip:
            continue

        if cusip not in cusip_files:
            cusip_files[cusip] = []
        cusip_files[cusip].append((filename, data))

    result = []
    for cusip, entries in cusip_files.items():
        entries.sort(key=lambda x: x[1].get_issue_date())
        result.append(entries[0])

    return result


def collect_securities_from_odm_xml() -> list[RawAuctionData]:
    """Discover CUSIPs from ODM XML secondary source not present in auction JSON.

    Defaults to BOND for any CUSIP not found in known auction data.
    """
    known_types = _get_security_types_from_auction_data()

    holdings_files = [
        "data/raw_xml/odm_ga_11032022.xml",
        "./data/raw_xml/odm_ga_07092024.xml",
    ]

    result = []
    seen_cusips = set()

    for _file in holdings_files:
        if not os.path.exists(_file):
            print(f"  Skipping {_file} (not found)")
            continue

        try:
            import xmltodict
            xml = open(_file).read()
            doc = xmltodict.parse(xml)
            doc = doc["bpd:ODMDataFeed"]["ODMData"]
        except Exception as e:
            print(f"  Error parsing {_file}: {e}")
            continue

        for entry in doc:
            cusip = entry["SecurityIdentifier"]
            if cusip in seen_cusips:
                continue
            seen_cusips.add(cusip)

            sec_type = known_types.get(cusip, "BOND")
            data_dict = {
                "issue_date": entry["IssueDate"],
                "maturity_date": entry["MaturityDate"],
                "cusip": cusip,
                "investment_rate": entry["InterestRate"],
                "security_type": sec_type,
                "accrued_interest": [0],
                "original_issue_date": entry["IssueDate"],
                "announcement_date": entry["IssueDate"],
                "dated_date": entry["IssueDate"],
            }
            result.append(RawAuctionData.from_json(json.dumps(data_dict)))

    return result


def load_securities(dry_run: bool = False):
    """Upload all securities from data/raw_json/ (and ODM XML) to the ledger-service."""
    print("=" * 60)
    print("Step 3: Upload securities to ledger-service")
    print("=" * 60)

    update_status("Scanning raw JSON for all securities")

    json_entries = collect_securities_from_json()

    type_counts: dict[str, int] = {}
    for _, d in json_entries:
        t = d.security_type or "UNKNOWN"
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"Found in raw JSON ({len(json_entries)} unique CUSIPs):")
    for t, count in sorted(type_counts.items()):
        print(f"  {t}: {count}")

    print("\nScanning ODM XML for additional securities...")
    odm_entries = collect_securities_from_odm_xml()
    json_cusips = {d.cusip for _, d in json_entries}
    odm_new = [d for d in odm_entries if d.cusip not in json_cusips]
    print(f"  Additional from ODM XML: {len(odm_new)} CUSIPs")

    all_entries: list[tuple[str | None, RawAuctionData]] = []
    all_entries.extend(json_entries)
    all_entries.extend([(None, d) for d in odm_new])

    total = len(all_entries)
    print(f"\nTotal securities to process: {total}")

    if dry_run:
        print("\n[DRY RUN] Would upload the following securities:")
        for source, data in all_entries:
            existing = get_security_by_id(data.cusip, IdentifierTypeProto.CUSIP)
            status = "EXISTS" if existing else "NEW"
            print(
                f"  {status} {data.security_type:4s}  {data.cusip}  "
                f"issue={data.get_issue_date()}  "
                f"maturity={data.get_maturity_date()}  "
                f"coupon={data.get_coupon_rate_float()}"
            )
        return

    created = 0
    errors = 0
    skipped = 0
    last_status_time = time.time()

    for i, (source, data) in enumerate(all_entries, 1):
        cusip = data.cusip

        if not cusip or not cusip.isalnum() or len(cusip) != 9:
            skipped += 1
            print(f"  [{i}/{total}] SKIP {data.security_type} — invalid CUSIP: {cusip!r}")
            continue

        try:
            existing = get_security_by_id(cusip, IdentifierTypeProto.CUSIP)

            if existing:
                found_match = any(
                    sec.get_maturity_date() == data.get_maturity_date()
                    for sec in existing
                )
                if found_match:
                    action = "UPDATE"
                else:
                    action = "CREATE"
            else:
                action = "CREATE"

            print(
                f"  [{i}/{total}] {action} {data.security_type} {cusip}  "
                f"issue={data.get_issue_date()} maturity={data.get_maturity_date()}"
            )
            upload_security_from_data_dict(data)
            created += 1

        except Exception as e:
            errors += 1
            print(f"  [{i}/{total}] ERROR {data.security_type} {cusip}: {e}")

        now = time.time()
        if now - last_status_time >= STATUS_UPDATE_INTERVAL:
            update_status(
                f"Processed {i}/{total} securities: {created} upserted, {errors} errors"
            )
            last_status_time = now

    print(f"\nDone loading securities:")
    print(f"  Upserted: {created}")
    print(f"  Skipped:  {skipped} (invalid CUSIP)")
    print(f"  Errors:   {errors}")
    print(f"  Total:    {total}")

    update_status(
        f"Security loading complete: {created} upserted, {errors} errors"
    )

    return created, errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Full pipeline: download XML, convert to JSON, upload securities"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip Step 1 (use existing data/raw_xml/)",
    )
    parser.add_argument(
        "--skip-convert",
        action="store_true",
        help="Skip Step 2 (use existing data/raw_json/); implies --skip-download",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making any changes",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("UST Security Pipeline")
    print("=" * 60)
    print(f"API_URL: {os.environ.get('API_URL', '(default: api.fintekkers.org)')}")
    print()

    if not args.skip_download and not args.skip_convert:
        download_xml(dry_run=args.dry_run)

    if not args.skip_convert:
        convert_all_xml(dry_run=args.dry_run)

    load_securities(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
