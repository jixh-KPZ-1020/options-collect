# CXq_data

Stock market data ingestion, normalization, storage, and serving infrastructure.

Data-only repository -- no pricing models, no backtesting. Fetches raw price data from multiple providers, normalizes it into a common schema, stores it as Hive-partitioned Parquet files, and serves it through DuckDB views. All operations are driven through the `cxq_data` CLI.

## Architecture

```
API Sources (yfinance, Alpha Vantage, Stooq)
        |
        v
   +---------+     +------------+     +---------------------+
   | Ingest   |---->| data/raw/  |---->| Normalize + Partition|
   | (CLI)    |     | (immutable)|     | (CLI: process)       |
   +---------+     +------------+     +----------+----------+
                                                  |
                                                  v
                                      +---------------------+
                                      | data/processed/      |
                                      | Hive Parquet files   |
                                      | symbol=X/source=Y/   |
                                      |   year=Z/data.parquet|
                                      +----------+----------+
                                                  |
                                                  v
                                      +---------------------+
                                      | DuckDB views         |
                                      | (SQL query layer)    |
                                      +----------+----------+
                                                  |
                                      +-----------+-----------+
                                      v           v           v
                                   Query     Validate   Cross-validate
```

### Key design decisions

- **Source-agnostic ingestor Protocol** -- `BaseIngestor` uses structural subtyping (`@runtime_checkable Protocol`), not inheritance. Any class with the right methods conforms.
- **Two-tier data model** -- `data/raw/` stores immutable API responses; `data/processed/` holds normalized Parquet. If a normalizer has a bug, fix it and reprocess -- raw data is intact.
- **Hive partitioning with source dimension** -- `symbol=X/source=Y/year=Z/data.parquet` lets multiple providers coexist for the same symbol without overwriting each other.
- **DuckDB views, not copies** -- The `.duckdb` file is tiny (just SQL text). Parquet is the source of truth. `hive_partitioning=true` enables partition pruning on queries like `WHERE symbol = 'AAPL'`.
- **Lazy ingestor registry** -- Ingestor modules are only imported when first needed, so you don't pay the cost of importing `yfinance` if you're only using Stooq.

### Data sources

| Source | Key | API Key | Adjusted Close | Intraday |
|--------|-----|---------|----------------|----------|
| yfinance | `yf` | No | Yes | Yes |
| Alpha Vantage | `av` | Yes (`AV_API_KEY`) | Yes | Yes |
| Stooq | `stooq` | No | No (set to close) | No |

### Canonical OHLCV schema

| Column | Type | Notes |
|--------|------|-------|
| `date` | DATE | Trading date |
| `open` | FLOAT64 | |
| `high` | FLOAT64 | |
| `low` | FLOAT64 | |
| `close` | FLOAT64 | Unadjusted |
| `adjusted_close` | FLOAT64 | Split/dividend adjusted |
| `volume` | INT64 | |
| `source` | VARCHAR | Origin provider |
| `ingested_at` | TIMESTAMP | UTC ingestion time |

Partition columns (`symbol`, `source`, `year`) are extracted from the directory path by DuckDB, not stored inside Parquet files.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage

```bash
# Ingest daily data
cxq_data ingest daily --source yf --symbols AAPL,MSFT --start 2024-01-01

# Process raw data into Parquet
cxq_data process run --source yfinance --symbols AAPL

# Initialize DuckDB views
cxq_data db init

# Query data
cxq_data query latest --symbol AAPL --rows 10

# Validate data quality
cxq_data validate run --symbols AAPL

# Cross-validate across sources
cxq_data crossvalidate compare --symbol AAPL --sources yfinance,stooq --tolerance 1.0
cxq_data crossvalidate matrix --symbols AAPL,MSFT --sources yfinance,stooq
```

