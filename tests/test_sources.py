"""Tests for bond and equity source abstractions using fixture data."""

import json
import os
from datetime import date

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures")
BOND_FIXTURES = os.path.join(FIXTURES_DIR, "bond")
EQUITY_FIXTURES = os.path.join(FIXTURES_DIR, "equity")


# -----------------------------------------------------------------------
# MockWikipediaSource
# -----------------------------------------------------------------------

class TestMockWikipediaSource:
    @pytest.fixture(autouse=True)
    def setup(self):
        from equity.sources import MockWikipediaSource
        self.source = MockWikipediaSource()

    def test_fetch_sp500_returns_list(self):
        result = self.source.fetch_sp500()
        assert isinstance(result, list)
        assert len(result) > 400

    def test_fetch_sp500_tuple_structure(self):
        result = self.source.fetch_sp500()
        for ticker, name in result:
            assert isinstance(ticker, str)
            assert isinstance(name, str)
            assert len(ticker) > 0
            assert len(name) > 0

    def test_fetch_sp500_known_constituents(self):
        result = self.source.fetch_sp500()
        tickers = {t for t, _ in result}
        for expected in ["AAPL", "MSFT", "GOOG", "AMZN", "META"]:
            assert expected in tickers, f"{expected} missing from S&P 500"

    def test_fetch_nasdaq100_returns_list(self):
        result = self.source.fetch_nasdaq100()
        assert isinstance(result, list)
        assert len(result) >= 100

    def test_fetch_nasdaq100_known_constituents(self):
        result = self.source.fetch_nasdaq100()
        tickers = {t for t, _ in result}
        for expected in ["AAPL", "MSFT", "NVDA", "AMZN"]:
            assert expected in tickers, f"{expected} missing from Nasdaq-100"

    def test_fetch_dow_returns_30(self):
        result = self.source.fetch_dow()
        assert isinstance(result, list)
        assert len(result) == 30

    def test_fetch_dow_tuple_structure(self):
        result = self.source.fetch_dow()
        for ticker, name in result:
            assert isinstance(ticker, str)
            assert isinstance(name, str)


# -----------------------------------------------------------------------
# MockYahooFinanceSource
# -----------------------------------------------------------------------

class TestMockYahooFinanceSource:
    @pytest.fixture(autouse=True)
    def setup(self):
        from equity.sources import MockYahooFinanceSource
        self.source = MockYahooFinanceSource()

    def test_download_prices_returns_dict(self):
        result = self.source.download_prices(
            ["AAPL"], date(2026, 1, 1), date(2026, 4, 30)
        )
        assert isinstance(result, dict)
        assert "AAPL" in result

    def test_download_prices_has_data(self):
        result = self.source.download_prices(
            ["AAPL"], date(2026, 1, 1), date(2026, 4, 30)
        )
        assert len(result["AAPL"]) > 0

    def test_download_prices_values_are_positive(self):
        result = self.source.download_prices(
            ["AAPL", "MSFT"], date(2026, 1, 1), date(2026, 4, 30)
        )
        for ticker, prices in result.items():
            for d, price in prices.items():
                assert price > 0, f"{ticker} on {d} has non-positive price {price}"

    def test_download_prices_date_filtering(self):
        start = date(2026, 3, 1)
        end = date(2026, 3, 31)
        result = self.source.download_prices(["AAPL"], start, end)
        for d_str in result["AAPL"]:
            assert start.isoformat() <= d_str <= end.isoformat()

    def test_download_prices_missing_ticker(self):
        result = self.source.download_prices(
            ["NONEXISTENT"], date(2026, 1, 1), date(2026, 4, 30)
        )
        assert result["NONEXISTENT"] == {}

    def test_download_prices_multiple_tickers(self):
        tickers = ["AAPL", "MSFT", "GOOG"]
        result = self.source.download_prices(tickers, date(2026, 1, 1), date(2026, 4, 30))
        for t in tickers:
            assert t in result


# -----------------------------------------------------------------------
# MockFedInvestSource
# -----------------------------------------------------------------------

class TestMockFedInvestSource:
    @pytest.fixture(autouse=True)
    def setup(self):
        from bond.sources import MockFedInvestSource
        self.source = MockFedInvestSource()

    def test_fetch_today_prices_returns_list(self):
        result = self.source.fetch_today_prices()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_fetch_today_prices_record_structure(self):
        result = self.source.fetch_today_prices()
        required_keys = {"cusip", "security_type", "rate", "maturity_date",
                         "call_date", "buy_price", "sell_price", "eod_price", "price_date"}
        for record in result[:10]:
            assert required_keys.issubset(record.keys()), f"Missing keys: {required_keys - record.keys()}"

    def test_fetch_prices_for_date(self):
        result = self.source.fetch_prices_for_date(date(2026, 3, 19))
        assert isinstance(result, list)
        assert len(result) > 400

    def test_fetch_prices_for_missing_date(self):
        result = self.source.fetch_prices_for_date(date(2020, 1, 1))
        assert result == []

    def test_cusip_format(self):
        result = self.source.fetch_today_prices()
        import re
        for record in result:
            assert re.match(r"^[A-Z0-9]{9}$", record["cusip"]), \
                f"Invalid CUSIP format: {record['cusip']}"


# -----------------------------------------------------------------------
# RawAuctionData
# -----------------------------------------------------------------------

fintekkers = pytest.importorskip("fintekkers", reason="fintekkers-ledger-models not installed")


class TestRawAuctionData:
    def test_from_json(self):
        from bond.treasury_auction import RawAuctionData
        data = {
            "cusip": "912828DN7",
            "issue_date": "2010-01-15",
            "maturity_date": "2020-01-15",
            "security_type": "NOTE",
            "investment_rate": "3.625",
            "announcement_date": "2010-01-06",
            "dated_date": "2010-01-15",
            "accrued_interest": [0],
            "original_issue_date": "2010-01-15",
        }
        auction = RawAuctionData.from_json(json.dumps(data))
        assert auction.cusip == "912828DN7"
        assert auction.get_issue_date() == date(2010, 1, 15)
        assert auction.get_maturity_date() == date(2020, 1, 15)
        assert auction.get_coupon_rate_float() == 3.625

    def test_security_type_proto_bond(self):
        from bond.treasury_auction import RawAuctionData
        from fintekkers.models.security.security_type_pb2 import BOND_SECURITY
        data = {"security_type": "BOND", "issue_date": "2020-01-01",
                "maturity_date": "2050-01-01", "cusip": "TEST12345",
                "announcement_date": "2020-01-01", "dated_date": "2020-01-01"}
        auction = RawAuctionData.from_json(json.dumps(data))
        assert auction.get_security_type_proto() == BOND_SECURITY

    def test_security_type_proto_tips(self):
        from bond.treasury_auction import RawAuctionData
        from fintekkers.models.security.security_type_pb2 import TIPS
        data = {"security_type": "TIPS", "issue_date": "2020-01-01",
                "maturity_date": "2050-01-01", "cusip": "TEST12345",
                "announcement_date": "2020-01-01", "dated_date": "2020-01-01"}
        auction = RawAuctionData.from_json(json.dumps(data))
        assert auction.get_security_type_proto() == TIPS

    def test_security_type_proto_frn(self):
        from bond.treasury_auction import RawAuctionData
        from fintekkers.models.security.security_type_pb2 import FRN
        data = {"security_type": "FRN", "issue_date": "2020-01-01",
                "maturity_date": "2050-01-01", "cusip": "TEST12345",
                "announcement_date": "2020-01-01", "dated_date": "2020-01-01"}
        auction = RawAuctionData.from_json(json.dumps(data))
        assert auction.get_security_type_proto() == FRN

    def test_coupon_rate_from_high_yield_fallback(self):
        from bond.treasury_auction import RawAuctionData
        data = {"security_type": "BOND", "issue_date": "2020-01-01",
                "maturity_date": "2050-01-01", "cusip": "TEST12345",
                "investment_rate": "", "high_yield": ["4.300"],
                "announcement_date": "2020-01-01", "dated_date": "2020-01-01"}
        auction = RawAuctionData.from_json(json.dumps(data))
        assert auction.get_coupon_rate_float() == 4.25  # floor to nearest 1/8%

    def test_to_json_roundtrip(self):
        from bond.treasury_auction import RawAuctionData
        data = {"cusip": "TEST12345", "issue_date": "2020-01-01",
                "maturity_date": "2050-01-01", "security_type": "BOND",
                "investment_rate": "2.5", "announcement_date": "2020-01-01",
                "dated_date": "2020-01-01"}
        auction = RawAuctionData.from_json(json.dumps(data))
        json_str = auction.to_json()
        auction2 = RawAuctionData.from_json(json_str)
        assert auction2.cusip == auction.cusip
        assert auction2.get_issue_date() == auction.get_issue_date()

    def test_real_auction_file(self):
        from bond.treasury_auction import RawAuctionData
        json_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw_json")
        files = [f for f in os.listdir(json_dir) if f.endswith(".json")][:5]
        for f in files:
            with open(os.path.join(json_dir, f)) as fh:
                auction = RawAuctionData.from_json(fh.read())
            assert len(auction.cusip) == 9
            assert auction.get_issue_date() is not None
            assert auction.get_maturity_date() is not None
            assert auction.get_maturity_date() > auction.get_issue_date()
