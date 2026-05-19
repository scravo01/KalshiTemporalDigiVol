# Data — Sources, Schemas, and Storage

## Overview

Three bronze parquet files feed the pipeline. All timestamps are UTC. The silver layer produces one small aggregation; gold is CSV + plots.

**Estimated sizes (60-day window):**

| File | Rows | Parquet (zstd) |
|---|---|---|
| `data/bronze/binance_btc_1m.parquet` | ~86,400 | ~1.5–2 MB |
| `data/bronze/kalshi_markets.parquet` | ~1,260 | ~20 KB |
| `data/bronze/kalshi_candles.parquet` | ~1.59M | ~5–8 MB |
| `data/silver/snapshot_features.parquet` | ~180 | < 1 KB |

---

## Bronze Layer

### `data/bronze/binance_btc_1m.parquet`

Source: Binance REST API — `GET /api/v3/klines`, symbol `BTCUSDT`, interval `1m`. Paginated at 1,000 bars/call (~87 calls for 60 days).

| Column | Polars dtype | Notes |
|---|---|---|
| `timestamp` | `Datetime(time_unit="s", time_zone="UTC")` | Bar open time |
| `open` | `Float32` | |
| `high` | `Float32` | |
| `low` | `Float32` | |
| `close` | `Float32` | |
| `volume` | `Float32` | Base asset (BTC) |
| `quote_volume` | `Float32` | Quote asset (USDT) |
| `num_trades` | `UInt32` | |
| `taker_buy_base` | `Float32` | |
| `taker_buy_quote` | `Float32` | |
| `ingested_at` | `Datetime(time_unit="s", time_zone="UTC")` | Pipeline run time |

`Float32` is sufficient for BTC prices (7 significant digits covers $99,999.99 with cents precision).

---

### `data/bronze/kalshi_markets.parquet`

Source: Kalshi REST API — `GET /markets` (live) and `GET /historical/markets` (>~3 months old). One row per contract per trading day.

**Strike selection**: 10 strikes below ATM + ATM + 10 strikes above ATM at $500 increments, anchored to BTC spot at **00:00 UTC** each day = **21 contracts/day**.

| Column | Polars dtype | Notes |
|---|---|---|
| `ticker` | `Categorical` | e.g. `KXBTCD-25MAY24-B95000` |
| `trade_date` | `Date` | The trading day this contract belongs to |
| `strike` | `UInt32` | Strike price in whole dollars |
| `expiry_time` | `Datetime(time_unit="s", time_zone="UTC")` | Always ~21:00 UTC (4pm ET) |
| `status` | `Categorical` | `open` or `settled` |
| `settlement_price` | `Float32` | Null if unsettled |
| `ingested_at` | `Datetime(time_unit="s", time_zone="UTC")` | Pipeline run time |

---

### `data/bronze/kalshi_candles.parquet`

Source: Kalshi REST API — `GET /markets/{ticker}/candles`, 1-minute resolution. One call per ticker (~1,260 calls for 60 days). Routes to `/historical/` endpoint for contracts older than the cutoff returned by `GET /historical/cutoff`.

Candles span **00:00 UTC → 21:00 UTC** per contract (1,260 bars max). **Zero-volume bars are dropped at ingest** — they carry no information and the silver layer only reads three snapshot minutes per day. Row count before/after is logged.

| Column | Polars dtype | Notes |
|---|---|---|
| `ticker` | `Categorical` | Foreign key to `kalshi_markets` |
| `timestamp` | `Datetime(time_unit="s", time_zone="UTC")` | Bar open time |
| `open` | `UInt8` | Price in cents (0–100) |
| `high` | `UInt8` | Price in cents (0–100) |
| `low` | `UInt8` | Price in cents (0–100) |
| `close` | `UInt8` | Price in cents (0–100) |
| `volume` | `UInt32` | Number of contracts traded |
| `ingested_at` | `Datetime(time_unit="s", time_zone="UTC")` | Pipeline run time |

`UInt8` is exact for Kalshi cent prices (integer 0–100); using `Float64` here would waste 7 bytes per cell across ~6M price values.

---

## Silver Layer

### `data/silver/snapshot_features.parquet`

One row per `(trade_date, snapshot)`. Built by DuckDB joining `kalshi_candles` to `binance_btc_1m` on UTC timestamp, filtering to the three daily windows, then calling `implied_vol.py` per group.

| Column | Polars dtype | Notes |
|---|---|---|
| `trade_date` | `Date` | |
| `snapshot` | `Categorical` | `T-1` (23:00), `T0` (00:00), `T+1` (01:00) |
| `atm_iv` | `Float32` | Annualized, e.g. 1.2 = 120% |
| `skew_25d` | `Float32` | `(IV(+25Δ) − IV(−25Δ)) / ATM IV` |
| `atm_strike` | `UInt32` | Strike closest to spot at snapshot |
| `n_valid_strikes` | `UInt8` | Strikes used; rows with < 3 are dropped |

---

## Gold Layer

### `data/gold/summary_stats.csv`

Plain CSV — small enough that parquet adds no value.

| Column | Type | Notes |
|---|---|---|
| `feature` | string | `atm_iv` or `skew_25d` |
| `shift` | string | `T0-T-1` or `T+1-T0` |
| `mean_shift` | float | |
| `std` | float | |
| `t_stat` | float | Paired t-test |
| `p_value` | float | |
| `cohens_d` | float | Effect size |
| `n_days` | int | Valid trading days in sample |

### `data/gold/plots/`

Two PNG files: `atm_iv_boxplot.png` and `skew_25d_boxplot.png`. Each shows the distribution at T-1 / T0 / T+1.

---

## Compression Strategy

All parquet files use **zstd level 3** (set via `write_parquet(..., compression="zstd", compression_level=3)`).

Reasons over the default snappy:
- **25–35% better compression ratio** at comparable write speed for time-series with high repetition
- Kalshi candles benefit most: `ticker` repeats ~1,260× per day (dictionary-encoded), timestamps increment by 60s (delta-encoded), and `UInt8` price columns have very low cardinality

The dtype choices above already reduce raw row size from ~80 bytes → ~32 bytes before any compression is applied. Combined with zstd, the candles file sits at ~5–8 MB rather than the ~19–25 MB a naive float64/snappy approach would produce.
