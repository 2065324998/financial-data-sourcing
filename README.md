# Financial Data Sourcing

Sources bond and equity security reference data and market prices from public data providers,
then loads them into the FinTekkers ledger via gRPC.

## Data Sources

### Bond Securities & Prices
| Source | Type | Script |
|--------|------|--------|
| [TreasuryDirect](https://www.treasurydirect.gov/xml) | US Treasury Bonds/Notes/Bills/TIPS/FRN | `bond/collect.py` |
| [UK DMO](https://www.dmo.gov.uk/) | UK Government Gilts | `bond/gilts.py` |
| [FedInvest](https://www.treasurydirect.gov/GA-FI/FedInvest/) | US Treasury daily prices | `bond/fedinvest.py` |
| yfinance / DMO / fallback | UK Gilt prices | `bond/backfill_gilts.py` |

### Equity Securities & Prices
| Source | Type | Script |
|--------|------|--------|
| [Wikipedia](https://en.wikipedia.org/) | S&P 500, Nasdaq-100, DJIA constituents | `equity/equities.py` |
| Hardcoded (Q1 2026) | S&P 500 Index Composition | `equity/sp500_index.py` |
| [Yahoo Finance](https://finance.yahoo.com/) | Equity closing prices | `equity/yahoo.py` |

## Project Structure

Organized by product type, matching the `SecurityTypeProto` taxonomy in
[ledger-models](https://github.com/FinTekkers/ledger-models):

```
bond/                    BOND_SECURITY, TIPS, FRN
  treasury.py            US Treasury securities (from TreasuryDirect auction data)
  gilts.py               UK Government Gilts (from DMO)
  fedinvest.py           FedInvest daily Treasury prices
  backfill_fedinvest.py  Historical FedInvest price backfill (2014-present)
  backfill_gilts.py      UK Gilt price loader
  collect.py             Full Treasury ETL pipeline: download -> convert -> upload
  download.py            TreasuryDirect XML downloader
  convert_xml.py         XML auction data -> JSON converter

equity/                  EQUITY_SECURITY, EQUITY_INDEX_SECURITY
  equities.py            S&P 500, Nasdaq-100, Dow Jones constituents (from Wikipedia)
  sp500_index.py         S&P 500 index composition loader
  yahoo.py               Yahoo Finance equity price loader

auction_data.py          RawAuctionData model (Treasury auction results)
env.py                   gRPC channel configuration
```

## Quick Start

```bash
pip install -r requirements.txt

# Load US Treasury securities (full pipeline)
API_URL=localhost python3 bond/collect.py

# Load UK Gilt securities
python3 bond/gilts.py

# Fetch today's Treasury prices
python3 bond/fedinvest.py

# Load equity securities from Wikipedia
python3 equity/equities.py

# Fetch equity prices
python3 equity/yahoo.py --tickers AAPL MSFT GOOG

# All scripts support --dry-run
python3 bond/collect.py --dry-run
```

## Architecture

All scripts load data into FinTekkers services via gRPC:
- **SecurityService** (port 8082) — security reference data
- **PriceService** (port 8083) — market prices

Scripts are idempotent and safe to re-run. UUIDs are deterministic (uuid5)
so duplicate uploads are no-ops.
