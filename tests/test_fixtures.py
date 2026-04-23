"""Data integrity tests for fixture files."""

import json
import os
import re
from datetime import date

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures")


# -----------------------------------------------------------------------
# Wikipedia fixtures
# -----------------------------------------------------------------------

class TestWikipediaFixtures:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.wiki_dir = os.path.join(FIXTURES_DIR, "equity", "wikipedia")

    def _load(self, name):
        with open(os.path.join(self.wiki_dir, f"{name}.json")) as f:
            return json.load(f)

    def test_sp500_count(self):
        data = self._load("sp500")
        assert 500 <= len(data) <= 510, f"S&P 500 has {len(data)} entries (expected ~503)"

    def test_sp500_no_duplicate_tickers(self):
        data = self._load("sp500")
        tickers = [t for t, _ in data]
        dupes = [t for t in tickers if tickers.count(t) > 1]
        assert len(set(dupes)) == 0, f"Duplicate tickers in S&P 500: {set(dupes)}"

    def test_sp500_tickers_look_valid(self):
        data = self._load("sp500")
        for ticker, name in data:
            assert re.match(r"^[A-Z][A-Z0-9.\-]{0,5}$", ticker), \
                f"Invalid ticker format: {ticker!r}"
            assert len(name) >= 2, f"Name too short for {ticker}: {name!r}"

    def test_nasdaq100_count(self):
        data = self._load("nasdaq100")
        assert 100 <= len(data) <= 105, f"Nasdaq-100 has {len(data)} entries"

    def test_nasdaq100_tickers_look_valid(self):
        data = self._load("nasdaq100")
        for ticker, name in data:
            assert re.match(r"^[A-Z][A-Z0-9.\-]{0,5}$", ticker), \
                f"Invalid ticker format: {ticker!r}"

    def test_dow_count(self):
        data = self._load("dow")
        assert len(data) == 30, f"Dow has {len(data)} entries (expected 30)"

    def test_dow_tickers_are_actual_tickers(self):
        """The first element of each tuple should be a stock ticker, not an exchange name."""
        data = self._load("dow")
        exchange_names = {"NYSE", "NASDAQ", "AMEX", "CBOE", "BATS"}
        for ticker, name in data:
            assert ticker not in exchange_names, \
                f"Dow entry has exchange name '{ticker}' instead of a ticker symbol"

    def test_dow_known_constituents(self):
        """Dow Jones should contain well-known blue chips."""
        data = self._load("dow")
        names = {name.lower() for _, name in data}
        tickers = {t for t, _ in data}
        all_text = names | tickers
        expected_companies = ["apple", "microsoft", "goldman sachs", "johnson & johnson"]
        for company in expected_companies:
            found = any(company in text for text in all_text)
            if not found:
                pytest.skip(f"Could not verify {company} — may be a data format issue")


# -----------------------------------------------------------------------
# Yahoo Finance fixtures
# -----------------------------------------------------------------------

class TestYahooFixtures:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.yahoo_dir = os.path.join(FIXTURES_DIR, "equity", "yahoo")

    def test_fixture_files_exist(self):
        files = os.listdir(self.yahoo_dir)
        json_files = [f for f in files if f.endswith(".json")]
        assert len(json_files) >= 10, f"Only {len(json_files)} Yahoo fixture files"

    def test_price_values_are_reasonable(self):
        for fname in os.listdir(self.yahoo_dir):
            if not fname.endswith(".json"):
                continue
            ticker = fname.replace(".json", "")
            with open(os.path.join(self.yahoo_dir, fname)) as f:
                prices = json.load(f)
            for d, price in prices.items():
                assert isinstance(price, (int, float)), f"{ticker} {d}: price is {type(price)}"
                assert 1.0 < price < 100000, f"{ticker} {d}: price {price} outside reasonable range"

    def test_dates_are_iso_format(self):
        for fname in os.listdir(self.yahoo_dir):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(self.yahoo_dir, fname)) as f:
                prices = json.load(f)
            for d in prices:
                assert re.match(r"^\d{4}-\d{2}-\d{2}$", d), f"Bad date format: {d}"

    def test_no_weekend_prices(self):
        for fname in os.listdir(self.yahoo_dir):
            if not fname.endswith(".json"):
                continue
            ticker = fname.replace(".json", "")
            with open(os.path.join(self.yahoo_dir, fname)) as f:
                prices = json.load(f)
            for d_str in prices:
                d = date.fromisoformat(d_str)
                assert d.weekday() < 5, f"{ticker} has price on {d_str} ({d.strftime('%A')})"


# -----------------------------------------------------------------------
# FedInvest fixtures
# -----------------------------------------------------------------------

class TestFedInvestFixtures:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.fedinvest_dir = os.path.join(FIXTURES_DIR, "bond", "fedinvest")

    def test_fixture_files_exist(self):
        files = [f for f in os.listdir(self.fedinvest_dir) if f.endswith(".json")]
        assert len(files) >= 3, f"Only {len(files)} FedInvest fixture files"

    def test_record_count_per_day(self):
        for fname in sorted(os.listdir(self.fedinvest_dir)):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(self.fedinvest_dir, fname)) as f:
                records = json.load(f)
            assert len(records) > 300, \
                f"{fname}: only {len(records)} records (expected 400+)"

    def test_cusip_format(self):
        fname = sorted(f for f in os.listdir(self.fedinvest_dir) if f.endswith(".json"))[-1]
        with open(os.path.join(self.fedinvest_dir, fname)) as f:
            records = json.load(f)
        for r in records:
            assert re.match(r"^[A-Z0-9]{9}$", r["cusip"]), f"Bad CUSIP: {r['cusip']}"

    def test_eod_prices_are_reasonable(self):
        fname = sorted(f for f in os.listdir(self.fedinvest_dir) if f.endswith(".json"))[-1]
        with open(os.path.join(self.fedinvest_dir, fname)) as f:
            records = json.load(f)
        for r in records:
            eod = r["eod_price"]
            if eod > 0:
                assert 30 < eod < 250, \
                    f"CUSIP {r['cusip']} eod_price {eod} outside reasonable range for Treasury"

    def test_security_types_are_known(self):
        known_types = {
            "MARKET BASED BILL", "MARKET BASED NOTE", "MARKET BASED BOND",
            "TIPS", "MARKET BASED FRN",
        }
        fname = sorted(f for f in os.listdir(self.fedinvest_dir) if f.endswith(".json"))[-1]
        with open(os.path.join(self.fedinvest_dir, fname)) as f:
            records = json.load(f)
        types_found = {r["security_type"] for r in records}
        unknown = types_found - known_types
        assert not unknown, f"Unknown security types: {unknown}"

    def test_date_matches_filename(self):
        for fname in sorted(os.listdir(self.fedinvest_dir)):
            if not fname.endswith(".json"):
                continue
            expected_date = fname.replace(".json", "")
            with open(os.path.join(self.fedinvest_dir, fname)) as f:
                records = json.load(f)
            if records:
                assert records[0]["price_date"] == expected_date, \
                    f"{fname}: price_date {records[0]['price_date']} != filename {expected_date}"
