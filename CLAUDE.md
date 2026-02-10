# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Stock market data ingestion, normalization, storage, and serving infrastructure. Data-only repo (no pricing models or backtesting). Package name: `cxq-data`, importable as `CXq_data`, CLI command: `cxq_data`. Uses **Polars** (not pandas) for all DataFrame operations.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"            # Core + dev dependencies
pip install -e ".[pipeline]"       # Options pipeline (databento, boto3)

# Tests
pytest                             # All tests
pytest tests/test_ingestors/ -v    # Single directory
pytest -k "test_normalize" -v      # Filter by name
pytest --cov=src --cov-report=term-missing

# Lint & type check
ruff check src tests               # Lint
ruff format src tests              # Auto-format
mypy src                           # Type check (strict mode)

# CLI
cxq_data ingest daily --source yf --symbols AAPL --start 2024-01-01
cxq_data ingest intraday --source yf --symbols AAPL --interval 5m
cxq_data process run --source yfinance --symbols AAPL
cxq_data process reprocess --source yfinance --symbols AAPL
cxq_data db init
cxq_data query latest --symbol AAPL --rows 10
cxq_data validate run --symbols AAPL
cxq_data crossvalidate compare --symbol AAPL --sources yfinance,stooq --tolerance 1.0
cxq_data crossvalidate matrix --symbols AAPL,MSFT --sources yfinance,stooq
```

Note: Python on this system is `python3` (3.12.0), not `python`.

## Architecture

**Source layout:** `src/CXq_data/` with hatchling build system.

**Data flow:** API Sources → `ingest` CLI → `data/raw/` (immutable) → `process` CLI → `data/processed/` (Hive-partitioned Parquet) → DuckDB views → query/validate/crossvalidate CLI.

**Two-tier data model:** `data/raw/` stores immutable API responses (CSV/JSON). `data/processed/` holds normalized Parquet. Raw data is never modified; reprocessing is safe.

**Hive partition layout:** `data/processed/daily_ohlcv/symbol=X/source=Y/year=Z/data.parquet`. The `source` dimension is required — without it, sources overwrite each other. Partition columns are extracted by DuckDB from paths, not stored in Parquet.

**Ingestor Protocol:** `BaseIngestor` in `ingestors/base.py` uses `@runtime_checkable Protocol` (structural subtyping, not inheritance). Registry in `ingestors/registry.py` lazy-loads modules on first use. Three sources: yfinance (`yf`), Alpha Vantage (`av`), Stooq (`stooq`).

**Source key vs. directory name:** `ingest` CLI uses registry keys (`yf`, `av`, `stooq`). `process` CLI uses the raw directory name (`yfinance`, `alpha_vantage`, `stooq`) because it scans `data/raw/{source}/{symbol}/`. These are different — don't mix them up.

**Canonical OHLCV schema:** Defined in `processing/schemas.py` as a Polars type dict (`DAILY_OHLCV_SCHEMA`). All sources normalize to: date, open, high, low, close, adjusted_close, volume, source, ingested_at. Stooq has no adjusted close — normalizer sets `adjusted_close = close`.

**Configuration:** pydantic-settings with priority: env vars > `.env` file > `config.toml` > defaults. Requires explicit `TomlConfigSettingsSource` in `settings_customise_sources`. Settings are lru_cached via `config/loader.py:get_settings()`.

**DuckDB:** `.duckdb` file is tiny (just view definitions). Parquet is the source of truth. Views use `hive_partitioning=true`.

## Key Modules

| Path | Purpose |
|------|---------|
| `src/CXq_data/cli/app.py` | Root Typer app, sub-command registration |
| `src/CXq_data/ingestors/base.py` | `BaseIngestor` Protocol, `RateLimit` dataclass |
| `src/CXq_data/ingestors/registry.py` | Lazy ingestor loader and factory |
| `src/CXq_data/processing/normalizer.py` | Raw → canonical schema (per-source adapters) |
| `src/CXq_data/processing/schemas.py` | `DAILY_OHLCV_SCHEMA` — canonical Polars column/type definition |
| `src/CXq_data/processing/partitioner.py` | Hive-partitioned Parquet writer |
| `src/CXq_data/storage/duckdb_manager.py` | DuckDB connection and view creation |
| `src/CXq_data/storage/paths.py` | Centralized path resolution for raw/processed/duckdb dirs |
| `src/CXq_data/config/settings.py` | Pydantic BaseSettings with TOML support |
| `src/CXq_data/validation/checks.py` | Four data quality checks (gaps, price sanity, stale data, OHLC consistency) |
| `src/CXq_data/validation/runner.py` | Orchestrates all checks, produces `CheckReport` |
| `src/CXq_data/utils/rate_limiter.py` | Rate limiting for API calls |
| `scripts/upload_options_to_s3.py` | Standalone options pipeline (Databento → S3) |

## Testing

Shared fixtures in `tests/conftest.py`: `tmp_data_dir` (temp raw/processed dirs), `sample_ohlcv_df` (canonical Polars DataFrame), `sample_yfinance_csv`, `sample_stooq_csv`. Pipeline tests in `tests/test_pipeline/` use `moto` for mock S3.

## Options Pipeline

Standalone script (not part of main CLI) at `scripts/upload_options_to_s3.py` with modules in `scripts/_options_pipeline/`. Converts Databento `.dbn.zst` files to Parquet and uploads to S3. Key design: one-file-at-a-time extraction, atomic state writes (fsync + os.replace), graceful shutdown on SIGINT, resume support.

## Code Style

- **Ruff:** line-length 100, target py311, rules: E, F, W, I, N, UP, B, A, SIM
- **MyPy:** strict mode with pydantic plugin
- **Pytest:** `-v --tb=short` defaults, testpaths=`tests`
- yfinance returns timezone-aware datetime strings (`2024-12-02 00:00:00-05:00`) — normalizer handles both formats
