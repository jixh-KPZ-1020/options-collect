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

## Options Pipeline (Databento -> S3)

Standalone pipeline for converting Databento options data (`.dbn.zst` format) to Parquet and uploading to AWS S3 with Hive partitioning. Separate from the main CLI.

### Data

- Source: `data/raw/databento/OPRA-20260208-3T68RYYKF9.zip` (~12.8 GB)
- 127 daily `.dbn.zst` files (SPY options, CBBO 1-minute bars)
- Date range: 2025-08-07 to 2026-02-06

### S3 Layout

```
s3://<bucket>/options/cbbo-1m/underlying=SPY/date=2025-08-07/data.parquet
                                              date=2025-08-08/data.parquet
                                              ...
```

### Pipeline Modules

```
scripts/
  upload_options_to_s3.py          # CLI entry point (argparse)
  _options_pipeline/
    config.py                      # PipelineConfig dataclass (from env vars)
    extractor.py                   # ZIP extraction + SHA-256 hash verification
    converter.py                   # DBN -> Parquet via databento library
    uploader.py                    # S3 upload with checksum + retry
    state.py                       # Atomic JSON state file (write-then-rename)
    shutdown.py                    # Graceful SIGINT/SIGTERM handling
    preflight.py                   # Pre-flight validation (disk, deps, S3)
    validator.py                   # Post-run S3 dataset validation
```

### Safety Features

- **One file at a time**: Peak disk ~200 MB instead of extracting all 12.8 GB
- **Atomic state writes**: `tempfile.mkstemp()` + `os.fsync()` + `os.replace()` — state file is never partially written
- **Graceful shutdown**: First Ctrl+C finishes current file and exits; second force-quits
- **S3 verification**: Content-MD5 on upload, `HeadObject` size check, SHA-256 in state file
- **Auto-cleanup**: `TemporaryDirectory` removes intermediate files on any exit path
- **Resume**: Re-running skips completed files; failed files are retried

### Setup

```bash
pip install -e ".[pipeline]"
```

Set AWS credentials in `.env` (see `.env.example`):
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=options/cbbo-1m
```

### Usage

```bash
# Dry run (single date, convert only, no S3)
python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip --dry-run --dates 2025-08-07

# Full pipeline
python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip

# Resume after interruption (auto-skips completed dates)
python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip

# Validate S3 dataset after upload
python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip --validate
```

### Tests

```bash
pytest tests/test_pipeline/ -v
```

46 tests covering: state atomicity, S3 upload/verification, signal handling, pre-flight checks, ZIP extraction, and full integration (using `moto` mock S3).

### Remaining Phases

- **Phase B**: Review scripts to understand each module
- **Phase C**: Run unit tests + dry-run on a single real date
- **Phase D**: Create AWS bucket/IAM, configure credentials, run full upload
