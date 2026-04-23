"""
Bond data source protocols with live and mock implementations.

External sources:
  - FedInvest: daily Treasury prices from treasurydirect.gov
  - TreasuryDirect: XML auction files from treasurydirect.gov
  - DMO: UK Gilt reference data from dmo.gov.uk
"""

import json
import os
import random
import re
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Optional

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures", "bond")


# -----------------------------------------------------------------------
# FedInvest — Treasury prices
# -----------------------------------------------------------------------

class FedInvestSource(ABC):
    @abstractmethod
    def fetch_prices_for_date(self, target_date: date) -> list[dict]:
        """Return list of price dicts for a single trading day.

        Each dict has keys: cusip, security_type, rate, maturity_date,
        call_date, buy_price, sell_price, eod_price, price_date (ISO str).
        """

    @abstractmethod
    def fetch_today_prices(self) -> list[dict]:
        """Return list of price dicts for today."""


class LiveFedInvestSource(FedInvestSource):
    BASE_URL = "https://www.treasurydirect.gov/GA-FI/FedInvest"
    TODAY_URL = f"{BASE_URL}/todaySecurityPriceDetail"
    HISTORICAL_URL = f"{BASE_URL}/selectSecurityPriceDate"
    REQUEST_DELAY = 1.5

    def fetch_today_prices(self) -> list[dict]:
        import requests
        resp = requests.get(self.TODAY_URL, timeout=30)
        resp.raise_for_status()
        return self._parse_html(resp.text)

    def fetch_prices_for_date(self, target_date: date) -> list[dict]:
        import requests
        session = requests.Session()
        session.get(self.HISTORICAL_URL, timeout=30)
        form_data = {
            "priceDate.month": str(target_date.month),
            "priceDate.day": str(target_date.day),
            "priceDate.year": str(target_date.year),
            "submit": "Show Prices",
        }
        resp = session.post(self.HISTORICAL_URL, data=form_data,
                            allow_redirects=True, timeout=30)
        resp.raise_for_status()
        if "Error Message" in resp.text and '<table class="data1">' not in resp.text:
            return []
        return self._parse_html(resp.text, fallback_date=target_date)

    def _parse_html(self, html: str, fallback_date: date = None) -> list[dict]:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        price_date = self._parse_date_from_header(soup) or fallback_date
        if price_date is None:
            raise ValueError("Could not determine price date from HTML")

        table = soup.find("table", class_="data1")
        if table is None:
            return []

        prices = []
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 8:
                continue
            cusip = cells[0].get_text(strip=True)
            if not re.match(r"^[A-Z0-9]{9}$", cusip):
                continue
            prices.append({
                "cusip": cusip,
                "security_type": cells[1].get_text(strip=True),
                "rate": cells[2].get_text(strip=True),
                "maturity_date": cells[3].get_text(strip=True),
                "call_date": cells[4].get_text(strip=True),
                "buy_price": self._parse_price(cells[5].get_text()),
                "sell_price": self._parse_price(cells[6].get_text()),
                "eod_price": self._parse_price(cells[7].get_text()),
                "price_date": price_date.isoformat(),
            })
        return prices

    @staticmethod
    def _parse_price(text: str) -> float:
        text = text.strip()
        if not text or text == "-":
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_date_from_header(soup) -> Optional[date]:
        for h2 in soup.find_all("h2"):
            match = re.search(r"Prices For:\s*(.+)", h2.get_text())
            if match:
                date_str = re.sub(r"\s*\(.*", "", match.group(1).strip())
                try:
                    return datetime.strptime(date_str, "%b %d, %Y").date()
                except ValueError:
                    pass
        return None


class MockFedInvestSource(FedInvestSource):
    def __init__(self, fixtures_dir: str = None):
        self._dir = fixtures_dir or os.path.join(FIXTURES_DIR, "fedinvest")

    def fetch_today_prices(self) -> list[dict]:
        files = sorted(f for f in os.listdir(self._dir) if f.endswith(".json"))
        if not files:
            return []
        with open(os.path.join(self._dir, files[-1])) as f:
            return json.load(f)

    def fetch_prices_for_date(self, target_date: date) -> list[dict]:
        path = os.path.join(self._dir, f"{target_date.isoformat()}.json")
        if not os.path.exists(path):
            return []
        with open(path) as f:
            return json.load(f)


# -----------------------------------------------------------------------
# TreasuryDirect — XML auction files
# -----------------------------------------------------------------------

class TreasuryDirectSource(ABC):
    @abstractmethod
    def list_xml_hrefs(self) -> list[str]:
        """Return list of XML file hrefs available on TreasuryDirect."""

    @abstractmethod
    def download_xml(self, href: str) -> bytes:
        """Download a single XML file and return its content."""


class LiveTreasuryDirectSource(TreasuryDirectSource):
    BASE_URL = "https://www.treasurydirect.gov/xml"

    def list_xml_hrefs(self) -> list[str]:
        import requests
        from bs4 import BeautifulSoup
        resp = requests.get(self.BASE_URL, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        hrefs = [a.attrs["href"] for a in soup.find_all("a") if "href" in a.attrs]
        return [h for h in hrefs if "DM_" not in h]

    def download_xml(self, href: str) -> bytes:
        import requests
        resp = requests.get(f"{self.BASE_URL}/{href}", timeout=30)
        return resp.content


class MockTreasuryDirectSource(TreasuryDirectSource):
    def __init__(self, fixtures_dir: str = None):
        self._dir = fixtures_dir or os.path.join(FIXTURES_DIR, "treasury_direct")

    def list_xml_hrefs(self) -> list[str]:
        index_path = os.path.join(self._dir, "index.json")
        if not os.path.exists(index_path):
            return []
        with open(index_path) as f:
            return json.load(f)

    def download_xml(self, href: str) -> bytes:
        path = os.path.join(self._dir, href)
        if not os.path.exists(path):
            return b""
        with open(path, "rb") as f:
            return f.read()


# -----------------------------------------------------------------------
# DMO — UK Gilt reference data
# -----------------------------------------------------------------------

class DMOSource(ABC):
    @abstractmethod
    def fetch_gilts(self) -> Optional[list[dict]]:
        """Return list of gilt dicts or None if fetch fails.

        Each dict has keys: isin, description, coupon_pct, maturity_date, issue_date.
        """


class LiveDMOSource(DMOSource):
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    def fetch_gilts(self) -> Optional[list[dict]]:
        import requests
        session = requests.Session()
        session.headers.update(self._HEADERS)
        try:
            r = session.get("https://www.dmo.gov.uk/", timeout=30)
            if "ShieldSquare" in r.text or "shieldsquare" in r.text.lower():
                return None
            time.sleep(1)
            today = date.today().strftime("%d/%m/%Y")
            export_url = (
                f"https://www.dmo.gov.uk/data/ExportReport?reportCode=D1A"
                f"&exportFormat=CSV&reportDate={today}"
            )
            r2 = session.get(export_url, timeout=30)
            ct = r2.headers.get("content-type", "")
            if "text/csv" in ct or ("text/plain" in ct and r2.text.strip().startswith("ISIN")):
                return self._parse_csv(r2.text)
            return None
        except Exception:
            return None

    def _parse_csv(self, csv_text: str) -> Optional[list[dict]]:
        import csv
        import io
        reader = csv.DictReader(io.StringIO(csv_text))
        gilts = []
        for row in reader:
            norm = {k.strip().lower(): v.strip() for k, v in row.items() if k}
            isin = (norm.get("isin") or norm.get("isin code") or "").strip()
            if not isin.startswith("GB"):
                continue
            gilts.append({
                "isin": isin,
                "description": (norm.get("description") or norm.get("gilt") or isin).strip(),
                "coupon_pct": float((norm.get("coupon") or norm.get("coupon rate") or "0").replace("%", "")),
                "maturity_date": (norm.get("maturity date") or norm.get("maturity") or "").strip(),
                "issue_date": (norm.get("issue date") or norm.get("first issue date") or "").strip(),
            })
        return gilts if gilts else None


class MockDMOSource(DMOSource):
    def __init__(self, fixtures_dir: str = None):
        self._dir = fixtures_dir or os.path.join(FIXTURES_DIR, "dmo")

    def fetch_gilts(self) -> Optional[list[dict]]:
        path = os.path.join(self._dir, "gilts.json")
        if not os.path.exists(path):
            return None
        with open(path) as f:
            return json.load(f)
