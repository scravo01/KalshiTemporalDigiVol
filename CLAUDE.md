# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Research project testing whether ATM implied vol and 25-delta skew on Kalshi BTC daily binary markets shift around the Asian market open (00:00 UTC). See `PLAN.md` for the full research design, hypothesis, and success criteria.

## Commands

```bash
# Run the full pipeline (bronze → silver → gold)
python run_pipeline.py

# Install dependencies
pip install -r requirements.txt
```

There is no test suite or linter configured yet. The `.gitignore` includes `.ruff_cache/`, so Ruff is the intended linter when added.

## Architecture

The pipeline follows a bronze/silver/gold medallion pattern with strict layer contracts — each layer may only read from the layer below, never skip layers or access raw data from gold.

```
src/clients/         # API wrappers — Kalshi and Binance, return polars DataFrames
src/etl/bronze/      # Extract API → parquet (raw, never drop rows)
src/etl/silver/      # Join + feature engineering using DuckDB
src/etl/gold/        # Stats and plots only — consumes silver parquet
data/{bronze,silver,gold}/
run_pipeline.py      # Single entrypoint, runs all stages in sequence
```

**Clients** (`src/clients/`): Both clients return `polars` DataFrames. The Kalshi client must route between live and historical endpoints based on the cutoff returned by `GET /historical/cutoff` — contracts older than ~3 months require `/historical/`. The Binance client paginates `BTCUSDT` 1m klines (max 1000 bars per call).

**Bronze** (`src/etl/bronze/`): Minimal transformation — standardize column names/dtypes, add `ingested_at` UTC timestamp, write to parquet. Exception: zero-volume Kalshi candle bars are dropped at ingest (logged). See `docs/data.md` for full schemas, dtype choices, and size estimates.

**Silver** (`src/etl/silver/`): The most complex stage. Uses DuckDB for joining Kalshi 1m candles to Binance spot on UTC timestamp across millions of rows. Filters to three daily snapshots (23:00, 00:00, 01:00 UTC). Calls `implied_vol.py` to invert digital Black-Scholes per (ticker, snapshot). Output: one row per `(trade_date, snapshot)` with `atm_iv` and `skew_25d`. Drops rows with fewer than 3 valid strikes (log reason, don't crash).

**Gold** (`src/etl/gold/`): Computes Δatm_iv and Δskew between snapshots, runs paired t-test + Wilcoxon + Cohen's d, outputs `summary_stats.csv` and two boxplots.

## Key Implementation Constraints

**Timestamps**: All timestamps must be UTC throughout — no local time anywhere.

**Kalshi contract prices**: Raw values are in cents (0–100). Normalize to (0–1) before IV inversion. Filter out contracts priced below 2 or above 98 — too deep ITM/OTM for reliable vol.

**Implied vol inversion** (`src/etl/silver/implied_vol.py`): Kalshi binary markets are cash-or-nothing digital calls. Invert `digital_price = N(d2)` using `scipy.optimize.brentq`. Expiry T must use settlement at 4pm ET (~21:00 UTC), not midnight.

**25-delta skew**: +25Δ strike is where `N(d2) ≈ 0.75`; −25Δ is where `N(d2) ≈ 0.25`. Kalshi strikes are discrete — use the closest available strike to each delta target.

**Primary DataFrame library**: `polars` everywhere — clients, ETL, analysis. DuckDB is used only for the silver-layer joins/windowing where SQL expressiveness helps across large datasets.
