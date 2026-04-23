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


## Mock Data Sources

All external data sources are abstracted behind protocols (`bond/sources.py`,
`equity/sources.py`) with live and mock implementations. This enables testing
without hitting external APIs.

### Fetching fixtures (real data)

```bash
# Fetch ~10,000 real data points and save to fixtures/
python3 fetch_fixtures.py

# Fetch a specific source
python3 fetch_fixtures.py --source fedinvest
python3 fetch_fixtures.py --source wikipedia
python3 fetch_fixtures.py --source yahoo
```

### Using mock data

Every script supports `--mock` to use cached fixture data:

```bash
python3 bond/fedinvest.py --mock --dry-run
python3 equity/equities.py --mock --dry-run
python3 equity/yahoo.py --mock --tickers AAPL MSFT --dry-run
```

### Fixture structure

```
fixtures/
  bond/
    fedinvest/       One JSON file per trading day (~400 CUSIPs each)
    treasury_direct/ Cached XML auction files
    dmo/             UK Gilt reference data
  equity/
    wikipedia/       sp500.json, nasdaq100.json, dow.json
    yahoo/           One JSON file per ticker (date -> close price)
```
