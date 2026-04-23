"""
Equity data source protocols with live and mock implementations.

External sources:
  - Wikipedia: index constituent lists (S&P 500, Nasdaq-100, Dow Jones)
  - YahooFinance: daily equity closing prices via yfinance
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Dict, List, Tuple

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures", "equity")


# -----------------------------------------------------------------------
# Wikipedia — index constituents
# -----------------------------------------------------------------------

class WikipediaSource(ABC):
    @abstractmethod
    def fetch_sp500(self) -> List[Tuple[str, str]]:
        """Return list of (ticker, company_name) for S&P 500 constituents."""

    @abstractmethod
    def fetch_nasdaq100(self) -> List[Tuple[str, str]]:
        """Return list of (ticker, company_name) for Nasdaq-100 constituents."""

    @abstractmethod
    def fetch_dow(self) -> List[Tuple[str, str]]:
        """Return list of (ticker, company_name) for Dow Jones constituents."""


class LiveWikipediaSource(WikipediaSource):
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    def _wiki_tables(self, url: str):
        import pandas as pd
        import requests
        resp = requests.get(url, headers=self._HEADERS, timeout=30)
        resp.raise_for_status()
        return pd.read_html(resp.text)

    def fetch_sp500(self) -> List[Tuple[str, str]]:
        tables = self._wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        df.columns = [c.strip() for c in df.columns]
        ticker_col = next(c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower())
        name_col = next(c for c in df.columns if "security" in c.lower() or "company" in c.lower())
        return [(str(row[ticker_col]).strip(), str(row[name_col]).strip()) for _, row in df.iterrows()]

    def fetch_nasdaq100(self) -> List[Tuple[str, str]]:
        tables = self._wiki_tables("https://en.wikipedia.org/wiki/Nasdaq-100")
        for df in tables:
            df.columns = [str(c).strip() for c in df.columns]
            ticker_cols = [c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()]
            name_cols = [c for c in df.columns if "company" in c.lower() or "name" in c.lower() or "security" in c.lower()]
            if ticker_cols and name_cols and len(df) > 50:
                return [(str(row[ticker_cols[0]]).strip(), str(row[name_cols[0]]).strip()) for _, row in df.iterrows()]
        raise ValueError("Could not find Nasdaq-100 constituents table")

    def fetch_dow(self) -> List[Tuple[str, str]]:
        tables = self._wiki_tables("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")
        for df in tables:
            df.columns = [str(c).strip() for c in df.columns]
            ticker_cols = [c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower() or "exchange" in c.lower()]
            name_cols = [c for c in df.columns if "company" in c.lower() or "name" in c.lower()]
            if ticker_cols and name_cols and 25 <= len(df) <= 35:
                return [(str(row[ticker_cols[0]]).strip(), str(row[name_cols[0]]).strip()) for _, row in df.iterrows()]
        raise ValueError("Could not find Dow Jones constituents table")


class MockWikipediaSource(WikipediaSource):
    def __init__(self, fixtures_dir: str = None):
        self._dir = fixtures_dir or os.path.join(FIXTURES_DIR, "wikipedia")

    def _load(self, name: str) -> List[Tuple[str, str]]:
        path = os.path.join(self._dir, f"{name}.json")
        with open(path) as f:
            return [tuple(item) for item in json.load(f)]

    def fetch_sp500(self) -> List[Tuple[str, str]]:
        return self._load("sp500")

    def fetch_nasdaq100(self) -> List[Tuple[str, str]]:
        return self._load("nasdaq100")

    def fetch_dow(self) -> List[Tuple[str, str]]:
        return self._load("dow")


# -----------------------------------------------------------------------
# Yahoo Finance — equity prices
# -----------------------------------------------------------------------

class YahooFinanceSource(ABC):
    @abstractmethod
    def download_prices(self, tickers: List[str], start: date, end: date) -> Dict[str, Dict[str, float]]:
        """Download closing prices for tickers over a date range.

        Returns {ticker: {date_iso_str: close_price}}.
        """


class LiveYahooFinanceSource(YahooFinanceSource):
    def download_prices(self, tickers: List[str], start: date, end: date) -> Dict[str, Dict[str, float]]:
        import yfinance as yf
        from datetime import datetime

        ticker_str = " ".join(tickers)
        data = yf.download(
            ticker_str,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            progress=False,
            auto_adjust=True,
        )

        result: Dict[str, Dict[str, float]] = {t: {} for t in tickers}
        if data.empty:
            return result

        if len(tickers) == 1:
            ticker = tickers[0]
            if "Close" in data.columns:
                for ts, val in data["Close"].dropna().items():
                    d = ts.date() if hasattr(ts, "date") else datetime.utcfromtimestamp(ts.timestamp()).date()
                    result[ticker][d.isoformat()] = float(val)
        else:
            try:
                close_data = data["Close"]
            except KeyError:
                return result
            for ticker in tickers:
                col = ticker
                if col not in close_data.columns:
                    alt = ticker.replace("-", ".") if "-" in ticker else ticker.replace(".", "-")
                    col = alt if alt in close_data.columns else None
                if col is None:
                    continue
                for ts, val in close_data[col].dropna().items():
                    d = ts.date() if hasattr(ts, "date") else datetime.utcfromtimestamp(ts.timestamp()).date()
                    result[ticker][d.isoformat()] = float(val)
        return result


class MockYahooFinanceSource(YahooFinanceSource):
    def __init__(self, fixtures_dir: str = None):
        self._dir = fixtures_dir or os.path.join(FIXTURES_DIR, "yahoo")

    def download_prices(self, tickers: List[str], start: date, end: date) -> Dict[str, Dict[str, float]]:
        result: Dict[str, Dict[str, float]] = {}
        for ticker in tickers:
            path = os.path.join(self._dir, f"{ticker}.json")
            if os.path.exists(path):
                with open(path) as f:
                    all_prices = json.load(f)
                result[ticker] = {
                    d: p for d, p in all_prices.items()
                    if start.isoformat() <= d <= end.isoformat()
                }
            else:
                result[ticker] = {}
        return result
