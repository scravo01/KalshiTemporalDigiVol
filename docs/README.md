# Documentation Index

Reference documentation for the KalshiTemporalDigiVol research pipeline.

## Architecture & Entry Points

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Medallion pipeline overview, data flow diagram, layer contracts, module map, snapshot labelling scheme |
| [cli.md](cli.md) | All `kvol` CLI commands (`bronze-kalshi`, `bronze-binance`, `silver`, `pipeline`), flags, defaults, and example invocations |

## API Clients

| Document | Description |
|----------|-------------|
| [client_kalshi.md](client_kalshi.md) | RSA-PSS auth scheme, rate limiting, retry logic, historical vs. live endpoint routing, candle price format normalization, output schemas |
| [client_binance.md](client_binance.md) | Binance US base URL rationale, async klines fetch, pagination, output schema |

## ETL Layers

| Document | Description |
|----------|-------------|
| [etl_bronze_kalshi.md](etl_bronze_kalshi.md) | Incremental date fetching, ATM strike ladder filter, candle window logic, per-date parquet writes |
| [etl_bronze_binance.md](etl_bronze_binance.md) | Padded fetch window (T-1/T+1 coverage), single-file parquet write, ordering constraint relative to Kalshi bronze |
| [etl_silver.md](etl_silver.md) | DuckDB join SQL, first-bar selection, snapshot labelling, dynamic T computation, IV inversion loop, output schema, VolSurfaceETL variant |
| [etl_gold.md](etl_gold.md) | ATM IV and 25Δ skew feature computation, consecutive snapshot t-tests and Wilcoxon tests, Cohen's d, CSV and boxplot outputs |

## Research Modules

| Document | Description |
|----------|-------------|
| [implied_vol.md](implied_vol.md) | Digital Black-Scholes inversion derivation, quadratic-in-u closed form, ITM/OTM/ATM root selection rules, edge cases, `invert_iv` API |

## UI

| Document | Description |
|----------|-------------|
| [ui.md](ui.md) | Streamlit dashboard sections, sidebar filters, cached data loaders, launch instructions |

## Legacy / Data Reference

| Document | Description |
|----------|-------------|
| [data.md](data.md) | Pre-existing data schema reference (reflects original three-snapshot design; some details superseded by the current 24-window architecture) |

---

## Quick Reference: Running the Pipeline

```bash
# 1. Install dependencies
uv sync

# 2. Set credentials in .env
echo 'KEY_ID=your-uuid' >> .env
echo 'KALSHI_API_KEY="-----BEGIN RSA PRIVATE KEY-----\n..."' >> .env

# 3. Run full pipeline
uv run kvol pipeline --start-date 2026-03-21 --end-date 2026-05-18

# 4. Launch dashboard
uv run streamlit run src/ui/app.py
```

## Key Architectural Invariants

- Each layer reads only from the layer directly below it — bronze from APIs, silver from bronze, gold from silver.
- All timestamps are UTC everywhere in the codebase.
- Polars is the primary DataFrame library. DuckDB is used only in the silver-layer joins.
- Kalshi markets are hourly binary options as of 2026 (format: `KXBTCD-26MAY1901-T85799.99`). The old daily format (`-B` prefix) is no longer active but is still parsed for historical data compatibility.
