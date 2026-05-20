# Data — Sources, Schemas, and Storage

## Overview

All timestamps are UTC throughout the pipeline. Only closing prices are stored — open/high/low are not needed since IV inversion uses a single well-defined price per bar. All parquet files use **zstd level 3** compression.

**Approximate sizes (61-day window):**

| File | Rows | Parquet (zstd) |
|------|------|----------------|
| `data/bronze/binance_btc_1m.parquet` | ~88,000 | ~0.5 MB |
| `data/bronze/kalshi_markets_YYYY-MM-DD.parquet` (per date) | ~261,000 total | ~20 KB/file |
| `data/bronze/kalshi_candles_YYYY-MM-DD.parquet` (per date) | varies | ~3–5 MB/file |
| `data/silver/contracts.parquet` | ~45,000 | ~2 MB |
| `data/silver/vol_surface.parquet` | ~2.5M | ~25 MB |
| `data/gold/rv_iv.parquet` | ~17,500 | ~0.5 MB |
| `data/gold/rv_iv_hourly.parquet` | ~1,460 | ~50 KB |

---

## Bronze Layer

### `data/bronze/binance_btc_1m.parquet`

Source: Binance US REST API — `GET /api/v3/klines`, symbol `BTCUSDT`, interval `1m`.

| Column | Polars dtype | Notes |
|--------|-------------|-------|
| `timestamp` | `Datetime(us, UTC)` | Bar open time |
| `close` | `Float32` | BTC spot price at end of bar |
| `ingested_at` | `Datetime(us, UTC)` | Pipeline run time |

---

### `data/bronze/kalshi_markets_YYYY-MM-DD.parquet`

Source: Kalshi REST API — `GET /markets` (live) and `GET /historical/markets` (settled before cutoff). One file per trading date; partitioned this way to support incremental ingest (existing dates are skipped on re-run).

Markets are **hourly binary options** as of 2026 (e.g., `KXBTCD-26MAY1901-T85799.99`), with ~261,000 total markets across the 61-day window.

| Column | Polars dtype | Notes |
|--------|-------------|-------|
| `ticker` | `Categorical` | e.g. `KXBTCD-26MAY1901-T85799.99` |
| `trade_date` | `Date` | The trading day this contract belongs to |
| `strike` | `UInt32` | Strike price in whole dollars |
| `expiry_time` | `Datetime(us, UTC)` | Contract settlement time (each UTC hour) |
| `status` | `Categorical` | `open` or `settled` |
| `settlement_price` | `Float32` | Null if unsettled |
| `ingested_at` | `Datetime(us, UTC)` | Pipeline run time |

---

### `data/bronze/kalshi_candles_YYYY-MM-DD.parquet`

Source: Kalshi batch candlestick API. One file per trading date. **Zero-volume bars are dropped at ingest.**

| Column | Polars dtype | Notes |
|--------|-------------|-------|
| `ticker` | `Categorical` | Foreign key to `kalshi_markets` |
| `trade_date` | `Date` | Added by `KalshiBronzeETL.transform()` via join |
| `timestamp` | `Datetime(us, UTC)` | Bar open time |
| `close` | `UInt8` | Last trade price in cents (0–100) |
| `volume` | `UInt32` | Contracts traded; kept for downstream filtering |
| `ingested_at` | `Datetime(us, UTC)` | Pipeline run time |

`UInt8` is exact for Kalshi cent prices (integer 0–100); 8× smaller than Float64.

---

## Silver Layer

### `data/silver/contracts.parquet`

Built by `SilverETL`: DuckDB join of kalshi candles → binance spot, then IV inversion per (ticker, snapshot). One row per valid `(trade_date, snapshot, ticker)`.

| Column | Polars dtype | Notes |
|--------|-------------|-------|
| `trade_date` | `Date` | |
| `snapshot` | `Categorical` | UTC hour label, e.g. `0`, `1`, `23` |
| `snapshot_ts` | `Datetime(us, UTC)` | Exact UTC timestamp of the snapshot bar |
| `digi_contract_name` | `Categorical` | Kalshi ticker |
| `strike` | `UInt32` | Strike in whole dollars |
| `expiry_time` | `Datetime(us, UTC)` | Contract settlement time |
| `digi_px` | `UInt8` | Mid-price in cents (0–100) |
| `prob_itm` | `Float32` | `digi_px / 100` — probability of finishing in the money |
| `implied_vol` | `Float32` | Annualized implied vol (digital Black-Scholes inversion) |
| `btc_close` | `Float32` | BTC spot price at snapshot time |
| `volume` | `UInt32` | Candle volume; rows with volume=0 are excluded |

Rows with fewer than 3 valid IV strikes in a snapshot group are dropped before writing.

---

### `data/silver/vol_surface.parquet`

Built by `VolSurfaceETL`: same join and IV inversion as `SilverETL` but computed at **every traded minute** across all 24 UTC expiry hours (not just snapshot windows). Used by `rv_iv_analysis.py`.

| Column | Polars dtype | Notes |
|--------|-------------|-------|
| `trade_date` | `Date` | |
| `snapshot` | `Categorical` | UTC hour of the bar (0–23) |
| `bar_ts` | `Datetime(us, UTC)` | Exact bar timestamp |
| `minutes_to_expiry` | `Float32` | Positive = contract still open |
| `digi_contract_name` | `Categorical` | Kalshi ticker |
| `strike` | `UInt32` | Strike in whole dollars |
| `expiry_time` | `Datetime(us, UTC)` | Contract settlement time |
| `digi_px` | `UInt8` | Mid-price in cents |
| `prob_itm` | `Float32` | `digi_px / 100` |
| `implied_vol` | `Float32` | Annualized IV; filtered to [20%, 500%] |
| `btc_close` | `Float32` | BTC spot at bar time |

---

## Gold Layer

### `data/gold/rv_iv.parquet` and `data/gold/rv_iv_hourly.parquet`

Output of `rv_iv_analysis.py` (`kvol rv-iv`). See [`etl_rv_iv.md`](etl_rv_iv.md) for the full schema and methodology.

| Column | Type | Notes |
|--------|------|-------|
| `bucket` | `Datetime(us, UTC)` | Period start (5-min or 1-hour truncated) |
| `rv_ann` | `Float64` | Annualized realized vol |
| `spot` | `Float32` | BTC spot at end of period |
| `atm_iv_mean` | `Float32` | Mean ATM IV across the period |
| `vol_premium` | `Float64` | `rv_ann − atm_iv_mean` (negative = IV > RV) |
| `var_premium` | `Float64` | `rv_ann² − atm_iv_mean²` |
| `hour_utc` | `Int8` | UTC hour (0–23) |
| `date` | `Date` | Calendar date |

### `data/gold/summary_stats.csv`

Output of `GoldETL`. Statistical test results for the snapshot feature analysis.

| Column | Type | Notes |
|--------|------|-------|
| `feature` | string | `atm_iv` or `skew_25d` |
| `shift` | string | Snapshot transition, e.g. `T0-T-1` |
| `mean_shift` | float | Mean of the pairwise difference |
| `std` | float | |
| `t_stat` | float | Paired t-test statistic |
| `p_value` | float | |
| `cohens_d` | float | Effect size |
| `n_days` | int | Valid trading days in sample |

### `data/gold/plots/`

PNG plots produced by `GoldETL`: `atm_iv_boxplot.png`, `skew_25d_boxplot.png`.

---

## Compression Strategy

All parquet files use zstd level 3 (`write_parquet(..., compression="zstd", compression_level=3)`).

Dtype choices reduce raw row size before compression:
- Kalshi prices: `UInt8` (1 byte) vs `Float64` (8 bytes) — 8× reduction per price column
- Tickers: `Categorical` → parquet dictionary encoding, ~2–4 bytes effective per row
- Timestamps: microsecond precision stored as `int64`, zstd compresses repeated date prefixes well

---

## API Endpoint Reference

Base URL: `https://api.elections.kalshi.com/trade-api/v2`

| Purpose | Endpoint |
|---------|----------|
| Routing cutoff | `GET /historical/cutoff` |
| Live market metadata | `GET /markets?series_ticker=KXBTCD` |
| Historical market metadata | `GET /historical/markets?series_ticker=KXBTCD` |
| Live candles (batch) | `GET /markets/candlesticks` |
| Historical candles (batch) | `GET /historical/market-candlesticks` |

Batch candle endpoint caps at 10,000 total candlesticks across tickers; with ~1,440 bars/contract/day the practical limit is 7 tickers per call.
