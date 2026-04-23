# Financial Data Sourcing

Sources bond and equity security reference data and market prices from public data providers,
then loads them into the FinTekkers ledger via gRPC.

## Data Sources

### Securities
| Source | Asset Class | Script |
|--------|------------|--------|
| [TreasuryDirect](https://www.treasurydirect.gov/xml) | US Treasury Bonds/Notes/Bills/TIPS/FRN | `pipeline/collect.py` |
| [UK DMO](https://www.dmo.gov.uk/) | UK Government Gilts | `securities/gilts.py` |
| [Wikipedia](https://en.wikipedia.org/) | US Equities (S&P 500, Nasdaq-100, DJIA) | `securities/equities.py` |
| Hardcoded (Q1 2026) | S&P 500 Index Composition | `securities/sp500_index.py` |

### Prices
| Source | Asset Class | Script |
|--------|------------|--------|
| [FedInvest](https://www.treasurydirect.gov/GA-FI/FedInvest/) | US Treasury daily prices | `prices/fedinvest.py` |
| [Yahoo Finance](https://finance.yahoo.com/) | Equity closing prices | `prices/yahoo.py` |
| yfinance / DMO / fallback | UK Gilt prices | `prices/backfill_gilts.py` |

## Project Structure

```
securities/          Security reference data loaders
  treasury.py        US Treasury bonds, notes, bills, TIPS, FRN
  equities.py        S&P 500, Nasdaq-100, Dow Jones equities
  gilts.py           UK Government Gilts
  sp500_index.py     S&P 500 index composition

prices/              Market price loaders
  fedinvest.py       FedInvest Treasury prices (daily scraper)
  backfill_fedinvest.py  Historical FedInvest backfill (2014-present)
  yahoo.py           Yahoo Finance equity prices
  backfill_gilts.py  UK Gilt price loader

pipeline/            Treasury data ETL pipeline
  collect.py         Full pipeline: download XML -> convert -> upload
  download.py        TreasuryDirect XML downloader
  convert_xml.py     XML auction data -> JSON converter

auction_data.py      RawAuctionData model (Treasury auction results)
env.py               gRPC channel configuration
```

## Quick Start

```bash
pip install -r requirements.txt

# Load US Treasury securities (full pipeline)
API_URL=localhost python3 pipeline/collect.py

# Load equity securities from Wikipedia
python3 securities/equities.py

# Load UK Gilt securities
python3 securities/gilts.py

# Fetch today's Treasury prices
python3 prices/fedinvest.py

# Fetch equity prices
python3 prices/yahoo.py --tickers AAPL MSFT GOOG

# All scripts support --dry-run for preview
python3 pipeline/collect.py --dry-run
```

## Architecture

All scripts load data into FinTekkers services via gRPC:
- **SecurityService** (port 8082) — security reference data
- **PriceService** (port 8083) — market prices

Scripts are idempotent and safe to re-run. UUIDs are deterministic (uuid5)
so duplicate uploads are no-ops.
