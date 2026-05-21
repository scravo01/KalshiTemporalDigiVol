# Documentation Index

Reference documentation for the KalshiTemporalDigiVol research pipeline.

## Research

| Document | Description |
|----------|-------------|
| [../ResearchReport.md](../ResearchReport.md) | Full strategy research memo — vol premium analysis, backtest results (all-hours and Asia-session), risk factors, and next steps |

## Architecture & Entry Points

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Medallion pipeline overview, data flow diagram, layer contracts, module map, snapshot labelling scheme |
| [cli.md](cli.md) | All `kvol` CLI commands (`bronze-kalshi`, `bronze-binance`, `silver`, `vol-surface`, `gold`, `rv-iv`, `pipeline`), flags, defaults, and example invocations |
| [airflow.md](airflow.md) | Airflow + Docker Compose setup, DAG overview, manual trigger, log visibility, and Airflow 3.x notes |

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
| [etl_rv_iv.md](etl_rv_iv.md) | RV vs IV analysis — 5-min and hourly realized vol, ATM IV selection, vol premium computation, output schema |

## Research Modules

| Document | Description |
|----------|-------------|
| [implied_vol.md](implied_vol.md) | Digital Black-Scholes inversion derivation, quadratic-in-u closed form, ITM/OTM/ATM root selection rules, edge cases, `invert_iv` API |

## Data Reference

| Document | Description |
|----------|-------------|
| [data.md](data.md) | Full schema reference for all bronze, silver, and gold parquet files |

---

## Quick Reference: Running the Pipeline

```bash
# 1. Install dependencies
uv sync

# 2. Set credentials in .env
KEY_ID=your-uuid
KALSHI_API_KEY="-----BEGIN RSA PRIVATE KEY-----..."

# 3. Run full pipeline
uv run kvol pipeline --start-date 2026-03-21 --end-date 2026-05-18

# 4. Launch dashboard
uv run kvol rv-iv
```

## Key Architectural Invariants

- Each layer reads only from the layer directly below it — bronze from APIs, silver from bronze, gold from silver.
- All timestamps are UTC everywhere in the codebase.
- Polars is the primary DataFrame library. DuckDB is used only in the silver-layer joins.
- Kalshi markets are hourly binary options as of 2026 (format: `KXBTCD-26MAY1901-T85799.99`). The old daily format (`-B` prefix) is no longer active.
- `prob_itm` column in both `contracts.parquet` and `vol_surface.parquet` equals `digi_px / 100`.
