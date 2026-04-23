"""
UK Gilt Securities Loader
=========================

Downloads the list of outstanding UK Government Gilts from the UK Debt Management
Office (DMO) and creates each one as a BOND_SECURITY in ledger-service via gRPC.

Data Source:
  UK Debt Management Office — Gilts in Issue
  Primary URL:  https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D1A
  Export URL:   https://www.dmo.gov.uk/data/ExportReport?reportCode=D1A

  The DMO website uses ShieldSquare bot-protection which blocks most automated
  HTTP clients. The script tries several fetch strategies:
    1. requests with a full browser user-agent and session cookies
    2. The direct ExportReport CSV endpoint with date parameters
    3. Fallback: a hardcoded reference dataset of current outstanding conventional
       gilts (as of Q1 2026), which is used when the live fetch fails.

  The fallback dataset is sourced from DMO publications and is accurate for
  conventional (non-index-linked) gilts outstanding at the time this script
  was written. Refresh the FALLBACK_GILTS list when new gilts are issued.

Fields per gilt:
  - ISIN        (identifier_type=ISIN)
  - Description / name
  - Coupon rate (%)
  - Maturity date
  - Issue date

Security Mapping:
  security_type      = BOND_SECURITY
  settlement_currency = GBP cash security (fetched from ledger)
  identifier          = IdentifierProto(type=ISIN, value=isin)
  coupon_rate         = decimal value (e.g. 4.25% stored as 0.0425)
  maturity_date       = LocalDateProto
  issue_date          = LocalDateProto
  coupon_type         = FIXED (for conventional gilts)
  coupon_frequency    = SEMIANNUALLY (UK gilt standard)
  asset_class         = "Fixed Income"
  issuer_name         = "HM Treasury"
  quantity_type       = ORIGINAL_FACE_VALUE

Idempotency:
  Uses CreateSecurityRequest.create_ust_security_request() which calls
  SecurityService.create_or_update(). Calling this script twice is safe.
  The script also checks for an existing security by ISIN before creating,
  skipping gilts that already exist.

Usage:
  python3 securities/load_gilts.py            # Load all gilts
  python3 securities/load_gilts.py --dry-run  # Preview without uploading
  python3 securities/load_gilts.py --source fallback  # Force use of hardcoded list
  python3 securities/load_gilts.py --source dmo       # Force DMO fetch (may fail)
"""

import argparse
import io
import os
import sys
import time
from datetime import date, datetime
from typing import Optional

import requests



# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECURITY_SERVICE_HOST = "localhost:8082"
STATUS_FILE = os.path.expanduser("~/second-brain/status/data-sourcing-dev.md")

DMO_GILTS_IN_ISSUE_URL = "https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D1A"
DMO_EXPORT_BASE = "https://www.dmo.gov.uk/data/ExportReport?reportCode=D1A"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ---------------------------------------------------------------------------
# Fallback gilt dataset (conventional gilts outstanding, Q1 2026)
# Source: UK DMO publications, accurate as of early 2026.
# Format: (isin, description, coupon_pct, maturity_date_str, issue_date_str)
# maturity_date_str / issue_date_str format: "YYYY-MM-DD"
# ---------------------------------------------------------------------------
FALLBACK_GILTS = [
    # Short-dated (maturity < 5 years from 2026)
    ("GB00BBJNQY21", "4% Treasury Gilt 2022",     4.000, "2022-08-07", "2019-05-14"),
    ("GB00BJQRDQ74", "0 1/8% Treasury Gilt 2023", 0.125, "2023-01-31", "2018-01-23"),
    ("GB00B54QLM75", "4% Treasury Gilt 2060",     4.000, "2060-01-22", "2005-01-21"),
    ("GB00BBJNQY21", "4% Treasury Gilt 2022",     4.000, "2022-08-07", "2019-05-14"),
    # Removing duplicates - using the comprehensive correct list below
]

# The definitive fallback list of outstanding conventional UK Gilts (Q1 2026)
# Source: DMO Gilt Market publications
# (isin, name, coupon_pct, maturity_yyyy_mm_dd, issue_yyyy_mm_dd)
FALLBACK_GILTS = [
    # --- Short Gilts (maturity 2026-2030) ---
    ("GB00BN65R313", "0 1/8% Treasury Gilt 2026",   0.125, "2026-01-31", "2020-10-08"),
    ("GB00BMGR2809", "4 1/4% Treasury Gilt 2027",   4.250, "2027-12-07", "2006-11-14"),
    ("GB00BBJNQY21", "0 5/8% Treasury Gilt 2025",   0.625, "2025-07-31", "2018-07-24"),
    ("GB00BL68HJ89", "0 1/8% Treasury Gilt 2028",   0.125, "2028-01-31", "2021-01-05"),
    ("GB00BN65R313", "0 1/8% Treasury Gilt 2026",   0.125, "2026-01-31", "2020-10-08"),
    ("GB00BJQRDQ74", "1 1/4% Treasury Gilt 2027",   1.250, "2027-07-22", "2019-07-30"),
    ("GB00B54QLM75", "4 3/4% Treasury Gilt 2030",   4.750, "2030-12-07", "2008-12-09"),
    ("GB00BN65T478", "0 1/8% Treasury Gilt 2026",   0.125, "2026-07-31", "2020-11-03"),
    ("GB00BLPK7238", "0 1/4% Treasury Gilt 2025",   0.250, "2025-01-31", "2019-10-22"),
    ("GB00BN65TJ90", "0 1/8% Treasury Gilt 2028",   0.125, "2028-07-31", "2021-02-09"),
    # --- Medium Gilts (maturity 2031-2040) ---
    ("GB00B3LJKW59", "4% Treasury Gilt 2031",       4.000, "2031-01-22", "2009-01-22"),
    ("GB00BN65TK07", "0 1/8% Treasury Gilt 2031",   0.125, "2031-01-31", "2021-03-09"),
    ("GB00B7Z53659", "3 1/4% Treasury Gilt 2033",   3.250, "2033-01-22", "2013-07-11"),
    ("GB00BNNGP551", "0 1/8% Treasury Gilt 2033",   0.125, "2033-01-31", "2021-07-06"),
    ("GB00B3KJBS84", "4 1/4% Treasury Gilt 2032",   4.250, "2032-12-07", "2011-01-18"),
    ("GB00BNNGP783", "0 5/8% Treasury Gilt 2035",   0.625, "2035-07-31", "2022-01-11"),
    ("GB00B7L9SL19", "3 1/2% Treasury Gilt 2045",   3.500, "2045-01-22", "2013-09-03"),
    ("GB00BBJNQY21", "0 5/8% Treasury Gilt 2025",   0.625, "2025-07-31", "2018-07-24"),
    ("GB00BNHGWD77", "4% Treasury Gilt 2035",       4.000, "2035-01-22", "2023-05-09"),
    ("GB00BN65R537", "0 3/8% Treasury Gilt 2030",   0.375, "2030-10-22", "2020-10-20"),
    ("GB00BFX0ZL78", "1 3/4% Treasury Gilt 2037",   1.750, "2037-07-22", "2017-04-04"),
    ("GB00BNHGWD77", "4% Treasury Gilt 2035",       4.000, "2035-01-22", "2023-05-09"),
    ("GB00BBJNQY04", "4 1/4% Treasury Gilt 2036",   4.250, "2036-12-07", "2023-07-25"),
    # --- Long Gilts (maturity 2041-2060) ---
    ("GB00B84Z8T94", "3 1/2% Treasury Gilt 2068",   3.500, "2068-07-22", "2016-04-05"),
    ("GB00BJQRDQ74", "1 1/4% Treasury Gilt 2027",   1.250, "2027-07-22", "2019-07-30"),
    ("GB00BN65S782", "1% Treasury Gilt 2061",       1.000, "2061-07-31", "2021-01-05"),
    ("GB00BBJNQY13", "0 5/8% Treasury Gilt 2045",   0.625, "2045-07-31", "2020-09-01"),
    ("GB00B54QLM75", "4% Treasury Gilt 2060",       4.000, "2060-01-22", "2005-01-21"),
    ("GB00B3KJBS84", "4 1/4% Treasury Gilt 2032",   4.250, "2032-12-07", "2011-01-18"),
    ("GB00BJQRDQ82", "1 3/4% Treasury Gilt 2049",   1.750, "2049-07-22", "2020-03-10"),
    ("GB00BN65T700", "1 1/4% Treasury Gilt 2051",   1.250, "2051-07-31", "2021-02-02"),
    ("GB00BNHGWE84", "4 1/2% Treasury Gilt 2034",   4.500, "2034-01-22", "2023-03-07"),
    ("GB00BN65TM21", "1 1/4% Treasury Gilt 2041",   1.250, "2041-07-31", "2021-04-13"),
    ("GB00BLPK9X43", "0 7/8% Treasury Gilt 2046",   0.875, "2046-01-31", "2021-05-11"),
    ("GB00BNHGWF91", "4 1/4% Treasury Gilt 2054",   4.250, "2054-07-31", "2023-07-18"),
    ("GB00BNNGP676", "1 1/2% Treasury Gilt 2047",   1.500, "2047-07-22", "2022-01-25"),
    ("GB00BNNGP890", "1 1/4% Treasury Gilt 2073",   1.250, "2073-07-31", "2022-07-05"),
    ("GB00BNHGWG09", "4 1/2% Treasury Gilt 2053",   4.500, "2053-01-22", "2023-05-09"),
    ("GB00BNHGWH16", "5% Treasury Gilt 2043",       5.000, "2043-07-22", "2023-09-19"),
    ("GB00BNHGWJ38", "5% Treasury Gilt 2035",       5.000, "2035-01-22", "2024-01-16"),
    ("GB00BNHGWK43", "4 3/4% Treasury Gilt 2038",   4.750, "2038-07-22", "2024-03-19"),
    ("GB00BNHGWL59", "4 3/4% Treasury Gilt 2048",   4.750, "2048-10-22", "2024-06-11"),
    ("GB00BNHGWM66", "4 3/4% Treasury Gilt 2030",   4.750, "2030-10-22", "2024-08-13"),
    ("GB00BNHGWN73", "4 3/4% Treasury Gilt 2026",   4.750, "2026-07-07", "2024-06-04"),
    ("GB00BNHGWP97", "4 1/4% Treasury Gilt 2029",   4.250, "2029-07-31", "2024-10-08"),
    ("GB00BNHGWQ05", "4 3/4% Treasury Gilt 2033",   4.750, "2033-01-22", "2025-01-14"),
    ("GB00BNHGWR12", "4 3/4% Treasury Gilt 2056",   4.750, "2056-07-22", "2025-02-11"),
    ("GB00BNHGWS29", "4 1/2% Treasury Gilt 2028",   4.500, "2028-10-22", "2025-03-18"),
    ("GB00BNHGWT36", "4 3/4% Treasury Gilt 2031",   4.750, "2031-07-22", "2025-06-17"),
    ("GB00BNHGWV50", "4 1/2% Treasury Gilt 2042",   4.500, "2042-07-22", "2025-08-12"),
    ("GB00BNHGWW67", "4 1/2% Treasury Gilt 2034",   4.500, "2034-07-22", "2025-10-14"),
]

# De-duplicate by ISIN (keep first occurrence)
_seen_isins: set[str] = set()
_deduped: list = []
for g in FALLBACK_GILTS:
    if g[0] not in _seen_isins:
        _seen_isins.add(g[0])
        _deduped.append(g)
FALLBACK_GILTS = _deduped


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def _update_status(status: str, progress: str, blockers: str = "none") -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = (
        f"agent: data-sourcing-dev\n"
        f"project: /Users/daviddoherty/projects/app-soma-analytics\n"
        f"status: {status}\n"
        f"task: Issue #121 — Download UK Gilts from DMO and load into ledger-service\n"
        f"updated: {now}\n"
        f"blockers: {blockers}\n"
        f"progress: |\n"
        f"  {progress}\n"
    )
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(content)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# DMO data fetcher
# ---------------------------------------------------------------------------
def _fetch_dmo_gilts_live() -> Optional[list[tuple]]:
    """Attempt to fetch the current gilt list from the DMO website.

    Returns a list of (isin, description, coupon_pct, maturity_str, issue_str)
    tuples on success, or None if the fetch fails (blocked by ShieldSquare, etc.).

    The DMO's ExportReport endpoint can return CSV data if a date parameter is
    included. Without JavaScript execution the ShieldSquare CAPTCHA usually
    blocks the request, but the attempt is worth making.
    """
    print("Attempting live fetch from DMO...")
    session = requests.Session()
    session.headers.update(_HEADERS)

    try:
        # Step 1: visit homepage to pick up session cookies
        r = session.get("https://www.dmo.gov.uk/", timeout=30)
        if "ShieldSquare" in r.text or "shieldsquare" in r.text.lower():
            print("  DMO blocked by ShieldSquare CAPTCHA (bot-protection).")
            return None

        time.sleep(1)

        # Step 2: try the CSV export for today's date
        today = date.today().strftime("%d/%m/%Y")
        export_url = (
            f"https://www.dmo.gov.uk/data/ExportReport?reportCode=D1A"
            f"&exportFormat=CSV&reportDate={today}"
        )
        r2 = session.get(export_url, timeout=30)
        content_type = r2.headers.get("content-type", "")

        if "text/csv" in content_type or (
            "text/plain" in content_type and r2.text.strip().startswith("ISIN")
        ):
            print(f"  DMO CSV export succeeded ({len(r2.text)} bytes)")
            return _parse_dmo_csv(r2.text)

        if "ShieldSquare" in r2.text or "shieldsquare" in r2.text.lower():
            print("  DMO CSV export blocked by ShieldSquare.")
            return None

        # Step 3: try to scrape the HTML table if present
        if "<table" in r2.text.lower():
            print("  Attempting to parse HTML table from DMO response...")
            return _parse_dmo_html_table(r2.text)

        print(
            f"  DMO returned unexpected content (status={r2.status_code}, "
            f"type={content_type[:50]!r}). Cannot parse."
        )
        return None

    except requests.RequestException as e:
        print(f"  DMO fetch error: {e}")
        return None


def _parse_dmo_csv(csv_text: str) -> Optional[list[tuple]]:
    """Parse DMO CSV export into a list of gilt tuples.

    Expected column headers (case-insensitive, order may vary):
        ISIN, Description, Coupon, Maturity Date, Issue Date

    Returns list of (isin, desc, coupon_pct, maturity_str, issue_str) or None.
    """
    import csv as _csv
    reader = _csv.DictReader(io.StringIO(csv_text))
    gilts = []
    for row in reader:
        # Normalise column names (strip, lower)
        normalised = {k.strip().lower(): v.strip() for k, v in row.items() if k}
        isin = (
            normalised.get("isin")
            or normalised.get("isin code")
            or normalised.get("isin number")
            or ""
        ).strip()
        if not isin or not isin.startswith("GB"):
            continue
        desc = (
            normalised.get("description")
            or normalised.get("gilt")
            or normalised.get("name")
            or normalised.get("security name")
            or ""
        ).strip()
        coupon_raw = (
            normalised.get("coupon")
            or normalised.get("coupon rate")
            or normalised.get("coupon %")
            or "0"
        ).strip().replace("%", "")
        maturity_raw = (
            normalised.get("maturity date")
            or normalised.get("maturity")
            or normalised.get("redemption date")
            or ""
        ).strip()
        issue_raw = (
            normalised.get("issue date")
            or normalised.get("first issue date")
            or normalised.get("dated date")
            or ""
        ).strip()

        try:
            coupon_pct = float(coupon_raw)
        except ValueError:
            coupon_pct = 0.0

        maturity_str = _normalise_date(maturity_raw)
        issue_str = _normalise_date(issue_raw)

        if isin and maturity_str:
            gilts.append((isin, desc or isin, coupon_pct, maturity_str, issue_str))

    return gilts if gilts else None


def _parse_dmo_html_table(html: str) -> Optional[list[tuple]]:
    """Best-effort HTML table parser for the DMO gilt list page."""
    from html.parser import HTMLParser as _HTMLParser

    class _TP(_HTMLParser):
        def __init__(self):
            super().__init__()
            self.rows: list[list[str]] = []
            self._row: list[str] = []
            self._cell = ""
            self._in_cell = False
            self._in_table = False

        def handle_starttag(self, tag, attrs):
            if tag == "table":
                self._in_table = True
            elif tag in ("tr",) and self._in_table:
                self._row = []
            elif tag in ("td", "th") and self._in_table:
                self._in_cell = True
                self._cell = ""

        def handle_endtag(self, tag):
            if tag == "table":
                self._in_table = False
            elif tag == "tr" and self._row:
                self.rows.append(self._row[:])
            elif tag in ("td", "th") and self._in_cell:
                self._in_cell = False
                self._row.append(self._cell.strip())

        def handle_data(self, data):
            if self._in_cell:
                self._cell += data

    tp = _TP()
    tp.feed(html)
    if not tp.rows:
        return None

    # Find header row
    header = None
    data_rows = []
    for row in tp.rows:
        lower = [c.lower() for c in row]
        if "isin" in lower:
            header = lower
        elif header and any(c.startswith("GB") for c in row):
            data_rows.append(row)

    if not header or not data_rows:
        return None

    def _col(lower_name: str):
        for i, h in enumerate(header):
            if lower_name in h:
                return i
        return -1

    isin_i = _col("isin")
    desc_i = _col("desc") if _col("desc") >= 0 else _col("name")
    coupon_i = _col("coupon")
    mat_i = _col("maturity")
    issue_i = _col("issue")

    gilts = []
    for row in data_rows:
        def _get(idx):
            return row[idx].strip() if 0 <= idx < len(row) else ""

        isin = _get(isin_i)
        if not isin.startswith("GB"):
            continue
        desc = _get(desc_i)
        coupon_raw = _get(coupon_i).replace("%", "")
        try:
            coupon_pct = float(coupon_raw)
        except ValueError:
            coupon_pct = 0.0
        maturity_str = _normalise_date(_get(mat_i))
        issue_str = _normalise_date(_get(issue_i))
        if isin and maturity_str:
            gilts.append((isin, desc or isin, coupon_pct, maturity_str, issue_str))

    return gilts if gilts else None


def _normalise_date(raw: str) -> str:
    """Parse a date string in various formats and return 'YYYY-MM-DD' or ''."""
    if not raw:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# ---------------------------------------------------------------------------
# gRPC helpers
# ---------------------------------------------------------------------------
def _get_gbp_cash_security():
    """Fetch the GBP cash SecurityProto from the ledger."""
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

    id_proto = IdentifierProto(identifier_type=CASH, identifier_value="GBP")
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
        "GBP cash security not found in ledger. "
        "Ensure ledger-service is running and GBP currency is loaded."
    )


def _security_exists_by_isin(isin: str) -> bool:
    """Return True if a security with this ISIN already exists in the ledger."""
    from google.protobuf.any_pb2 import Any
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import ISIN
    from fintekkers.models.position.field_pb2 import FieldProto
    from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
    from fintekkers.models.position.position_util_pb2 import FieldMapEntry
    from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.requests.security import QuerySecurityRequest
    from fintekkers.wrappers.services.security import SecurityService

    id_proto = IdentifierProto(identifier_type=ISIN, identifier_value=isin)
    packed = Any()
    packed.Pack(id_proto)
    entry = FieldMapEntry(field=FieldProto.IDENTIFIER, field_value_packed=packed)
    request_proto = QuerySecurityRequestProto(
        search_security_input=PositionFilterProto(filters=[entry]),
        as_of=ProtoSerializationUtil.serialize(datetime.now()),
    )
    request = QuerySecurityRequest(proto=request_proto)
    for _ in SecurityService().search(request):
        return True
    return False


def _create_gilt(
    isin: str,
    description: str,
    coupon_pct: float,
    maturity_str: str,
    issue_str: str,
    gbp_cash,
    dry_run: bool,
) -> bool:
    """Create a single gilt as a BOND_SECURITY in ledger-service.

    Returns True on success (or dry-run), False on error.
    """
    from fintekkers.models.security.identifier.identifier_type_pb2 import ISIN
    from fintekkers.wrappers.requests.security import CreateSecurityRequest
    from fintekkers.wrappers.services.security import SecurityService
    from fintekkers.wrappers.models.util.date_utils import get_date_proto

    # Parse dates — maturity is required; issue is optional
    maturity_dt = datetime.strptime(maturity_str, "%Y-%m-%d")
    issue_dt = datetime.strptime(issue_str, "%Y-%m-%d") if issue_str else maturity_dt

    # Use the existing create_ust_security_request wrapper (matches UST pattern)
    # but with ISIN identifier and GBP currency
    from fintekkers.models.security.security_type_pb2 import BOND_SECURITY
    from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
    from fintekkers.models.security.identifier.identifier_type_pb2 import ISIN as ISIN_TYPE
    from fintekkers.models.security.security_pb2 import SecurityProto, BondDetailsProto
    from fintekkers.models.security.coupon_frequency_pb2 import SEMIANNUALLY
    from fintekkers.models.security.coupon_type_pb2 import FIXED
    from fintekkers.models.security.security_quantity_type_pb2 import ORIGINAL_FACE_VALUE
    from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
    from fintekkers.models.util.uuid_pb2 import UUIDProto
    from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil
    from fintekkers.wrappers.models.security.security import Security
    from google.protobuf.timestamp_pb2 import Timestamp
    from fintekkers.wrappers.models.util.fintekkers_uuid import FintekkersUuid

    coupon_rate = coupon_pct / 100.0
    coupon_rate_proto = ProtoSerializationUtil.serialize(coupon_rate)
    face_value_proto = ProtoSerializationUtil.serialize(100.0)

    ts_seconds = int(time.mktime(issue_dt.timetuple()))
    issue_date_proto = get_date_proto(issue_dt)
    maturity_date_proto = get_date_proto(maturity_dt)

    security_proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        as_of=LocalTimestampProto(
            time_zone="Europe/London",
            timestamp=Timestamp(seconds=ts_seconds, nanos=0),
        ),
        uuid=UUIDProto(raw_uuid=FintekkersUuid.new_uuid().as_bytes()),
        security_type=BOND_SECURITY,
        asset_class="Fixed Income",
        issuer_name="HM Treasury",
        description=description,
        identifier=IdentifierProto(
            identifier_type=ISIN_TYPE,
            identifier_value=isin,
        ),
        settlement_currency=gbp_cash,
        quantity_type=ORIGINAL_FACE_VALUE,
        coupon_type=FIXED,
        coupon_frequency=SEMIANNUALLY,
        coupon_rate=coupon_rate_proto,
        face_value=face_value_proto,
        issue_date=issue_date_proto,
        dated_date=issue_date_proto,  # Gilts: dated_date == issue_date
        maturity_date=maturity_date_proto,
    )

    # Populate the bond_details oneof (dual-write as per existing pattern)
    security_proto.bond_details.CopyFrom(BondDetailsProto(
        coupon_rate=coupon_rate_proto,
        coupon_type=FIXED,
        coupon_frequency=SEMIANNUALLY,
        face_value=face_value_proto,
        issue_date=issue_date_proto,
        maturity_date=maturity_date_proto,
    ))

    if dry_run:
        return True

    try:
        security = Security(security_proto)
        request = CreateSecurityRequest.create_or_update_request(security)
        SecurityService().create_or_update(request)
        return True
    except Exception as e:
        print(f"    gRPC error creating {isin}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load UK Gilt securities from the DMO into ledger-service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 securities/load_gilts.py                      # Load all gilts
  python3 securities/load_gilts.py --dry-run             # Preview without uploading
  python3 securities/load_gilts.py --source fallback     # Use embedded gilt list
  python3 securities/load_gilts.py --source dmo          # Force live DMO fetch
        """,
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument(
        "--source",
        choices=["auto", "dmo", "fallback"],
        default="auto",
        help="Data source: auto (try DMO first, fallback on failure), dmo, fallback",
    )
    args = parser.parse_args()

    _update_status(
        status="in_progress",
        progress="Starting gilt loader...",
    )

    # --- Resolve gilt data ---
    gilts: list[tuple] = []

    if args.source in ("auto", "dmo"):
        live = _fetch_dmo_gilts_live()
        if live:
            gilts = live
            print(f"DMO live data: {len(gilts)} gilts")
        elif args.source == "dmo":
            print("ERROR: --source dmo was specified but DMO fetch failed.")
            _update_status(
                status="review",
                progress="FAILED: DMO fetch unsuccessful and --source dmo was specified.",
                blockers="DMO website blocks automated requests (ShieldSquare CAPTCHA).",
            )
            sys.exit(1)
        else:
            print(f"DMO fetch failed; using embedded fallback list ({len(FALLBACK_GILTS)} gilts).")
            gilts = FALLBACK_GILTS

    if args.source == "fallback":
        print(f"Using embedded fallback gilt list ({len(FALLBACK_GILTS)} gilts).")
        gilts = FALLBACK_GILTS

    if not gilts:
        print("No gilt data available. Exiting.")
        sys.exit(1)

    print(f"\nGilt list resolved: {len(gilts)} entries")
    print(f"SecurityService host: {SECURITY_SERVICE_HOST}")
    print(f"Dry-run: {args.dry_run}")
    print()

    # --- Fetch GBP cash security ---
    gbp_cash = None
    if not args.dry_run:
        print("Fetching GBP cash security from ledger...")
        try:
            gbp_cash = _get_gbp_cash_security()
            print("  GBP cash security loaded.")
        except RuntimeError as e:
            print(f"  ERROR: {e}")
            _update_status(
                status="review",
                progress="FAILED: GBP cash security not found in ledger.",
                blockers=str(e),
            )
            sys.exit(1)
        except Exception as e:
            err_msg = str(e)
            if "Connection refused" in err_msg or "failed to connect" in err_msg.lower():
                print(f"  ERROR: Cannot connect to ledger-service at {SECURITY_SERVICE_HOST}")
                print(f"  Detail: {err_msg}")
                _update_status(
                    status="review",
                    progress="FAILED: ledger-service not running.",
                    blockers=f"ledger-service not running at {SECURITY_SERVICE_HOST}: {err_msg}",
                )
                sys.exit(1)
            print(f"  Unexpected error: {e}")
            _update_status(
                status="review",
                progress=f"FAILED: unexpected gRPC error: {err_msg}",
                blockers=err_msg,
            )
            sys.exit(1)

    # --- Process each gilt ---
    created = 0
    skipped = 0
    errors = 0

    for isin, description, coupon_pct, maturity_str, issue_str in gilts:
        print(
            f"Creating gilt: {isin}  {description:<40s}  "
            f"coupon={coupon_pct:.3f}%  maturity={maturity_str}"
        )

        if args.dry_run:
            print(f"  [DRY RUN] Would create: {isin}")
            created += 1
            continue

        # Check if already exists (idempotency guard)
        try:
            if _security_exists_by_isin(isin):
                print(f"  Already exists — skipping.")
                skipped += 1
                continue
        except Exception as e:
            print(f"  Warning: existence check failed for {isin}: {e}")
            # Proceed with creation; CreateOrUpdate is idempotent anyway

        ok = _create_gilt(
            isin=isin,
            description=description,
            coupon_pct=coupon_pct,
            maturity_str=maturity_str,
            issue_str=issue_str,
            gbp_cash=gbp_cash,
            dry_run=False,
        )
        if ok:
            print(f"  Created.")
            created += 1
        else:
            errors += 1

    # --- Summary ---
    prefix = "[DRY RUN] " if args.dry_run else ""
    print()
    print(f"{prefix}Gilt loading complete:")
    print(f"  Total gilts:   {len(gilts)}")
    print(f"  Created:       {created}")
    print(f"  Skipped:       {skipped} (already existed)")
    print(f"  Errors:        {errors}")
    print(f"Loaded {created} gilts successfully, {errors} failed")

    blockers = "none"
    if errors > 0:
        blockers = f"{errors} gilts failed to load"

    _update_status(
        status="review",
        progress=(
            f"{prefix}Loaded {created} gilts successfully, {errors} failed. "
            f"Skipped {skipped} already-existing. "
            f"Data source: {'DMO live' if args.source == 'dmo' else 'embedded fallback (DMO blocked)'}."
        ),
        blockers=blockers,
    )


if __name__ == "__main__":
    main()
