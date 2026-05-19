# Data — Sources, Schemas, and Storage

## Overview

Three bronze parquet files feed the pipeline. All timestamps are UTC. Only **closing prices** are stored — open/high/low are not needed since IV inversion uses a single well-defined price per snapshot minute.

**Estimated sizes (60-day window):**

| File | Rows | Parquet (zstd) |
|---|---|---|
| `data/bronze/binance_btc_1m.parquet` | ~86,400 | ~0.5 MB |
| `data/bronze/kalshi_markets.parquet` | ~1,260 | ~20 KB |
| `data/bronze/kalshi_candles.parquet` | ~1.59M | ~3–5 MB |
| `data/silver/snapshot_features.parquet` | ~180 | < 1 KB |

---

## Bronze Layer

### `data/bronze/binance_btc_1m.parquet`

Source: Binance REST API — `GET /api/v3/klines`, symbol `BTCUSDT`, interval `1m`. Paginated at 1,000 bars/call (~87 calls for 60 days).

| Column | Polars dtype | Notes |
|---|---|---|
| `timestamp` | `Datetime(time_unit="s", time_zone="UTC")` | Bar open time |
| `close` | `Float32` | BTC spot price at end of bar |
| `ingested_at` | `Datetime(time_unit="s", time_zone="UTC")` | Pipeline run time |

`Float32` is sufficient for BTC prices (7 significant digits covers $99,999.99 with cent precision). All other Binance kline fields (open, high, low, volume, trades) are discarded at ingest — the silver layer only needs spot price at a given timestamp.

---

### `data/bronze/kalshi_markets.parquet`

Source: Kalshi REST API — `GET /markets` (live) and `GET /historical/markets` (settled before cutoff). One row per contract per trading day.

**Strike selection**: 10 strikes below ATM + ATM + 10 strikes above ATM at **$500 increments**, anchored to BTC `close` price at **00:00 UTC** each day = **21 contracts/day**.

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

Source: Kalshi batch candlestick API — see [Kalshi API Endpoints](#kalshi-api-endpoints) below. One call fetches up to 7 tickers simultaneously. Routes to `/historical/` endpoint for contracts older than the cutoff.

Candles span **00:00 UTC → 21:00 UTC** per contract (1,260 bars max). **Zero-volume bars are dropped at ingest** — they carry no information and the silver layer only reads three snapshot minutes per day. Row count before/after is logged.

| Column | Polars dtype | Notes |
|---|---|---|
| `ticker` | `Categorical` | Foreign key to `kalshi_markets` |
| `timestamp` | `Datetime(time_unit="s", time_zone="UTC")` | Bar open time |
| `close` | `UInt8` | Last trade price in cents (0–100) |
| `volume` | `UInt32` | Contracts traded; kept for zero-volume filtering |
| `ingested_at` | `Datetime(time_unit="s", time_zone="UTC")` | Pipeline run time |

`UInt8` is exact for Kalshi cent prices (integer 0–100); `Float64` would waste 7 bytes per cell across ~1.6M rows.

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

## Implied Volatility Methodology

Kalshi BTC daily contracts are cash-or-nothing digital calls: they pay $1 if BTC > K at 4pm ET. With r=0 (appropriate for sub-day expiry):

```
price = N(d2)
d2 = [ln(S/K) − σ²T/2] / (σ√T)
```

### Closed-Form Inversion

Given observed `close` price `p` (cents ÷ 100), spot `S`, strike `K`, time-to-expiry `T`:

**Step 1** — invert the normal CDF:
```
d2* = N⁻¹(p)    # scipy.stats.norm.ppf
```

**Step 2** — substituting `u = σ√T` turns the d2 equation into a quadratic:
```
u² + 2·d2*·u − 2·ln(S/K) = 0
discriminant = d2*² + 2·ln(S/K)
```

**Step 3** — root selection (the quadratic has two roots; the physically meaningful one depends on moneyness):
- **ITM** (S > K): one positive root → `u = −d2* + √discriminant`
- **OTM** (S < K): two positive roots → take the **smaller**: `u = −d2* − √discriminant`
- **ATM** (S ≈ K): formula degenerates; fall back to `scipy.optimize.brentq`

**Step 4**:
```
σ = u / √T
```

This is O(1) per contract with no iteration. The discriminant stays positive across all valid Kalshi prices (2–98 cents) for strikes within ±$5,000 of spot.

### Time-to-Expiry per Snapshot

Contracts settle at **4pm ET = 21:00 UTC**.

| Snapshot | UTC time | Hours to expiry | T (years) |
|---|---|---|---|
| T-1 | 23:00 UTC (prior day) | 22 h | 22/8760 ≈ 0.002511 |
| T0 | 00:00 UTC | 21 h | 21/8760 ≈ 0.002397 |
| T+1 | 01:00 UTC | 20 h | 20/8760 ≈ 0.002283 |

---

## Compression Strategy

All parquet files use **zstd level 3** (`write_parquet(..., compression="zstd", compression_level=3)`).

The dtype choices reduce raw row size before compression applies:
- Kalshi prices: `UInt8` (1 byte) vs `Float64` (8 bytes) — 8× reduction per price column
- Timestamps: second precision `int32`-equivalent vs microsecond `int64`
- Tickers: `Categorical` → parquet dictionary encodes, ~2–4 bytes effective per row

Combined with zstd and the close-only schema, the candles file sits at ~3–5 MB rather than the ~19–25 MB a naive float64/snappy/OHLC approach would produce.

---

## Kalshi API Endpoints

Base URL: `https://external-api.kalshi.com/trade-api/v2`

| Purpose | Endpoint | Notes |
|---|---|---|
| Routing cutoff | `GET /historical/cutoff` | Call once at startup; returns `market_settled_ts` |
| Live market metadata | `GET /markets?series_ticker=KXBTCD` | Paginated via `cursor` |
| Historical market metadata | `GET /historical/markets?series_ticker=KXBTCD` | Same params/shape as live |
| Live candles (batch) | `GET /markets/candlesticks` | `period_interval=1` supported |
| Historical candles (batch) | `GET /historical/market-candlesticks` | Same shape as live batch |

### Batch Candle Sizing

The batch endpoint accepts up to 100 tickers per request but caps at **10,000 total candlesticks** across all tickers. With 1,260 bars/contract/day:

- Max tickers per call: floor(10,000 / 1,260) = **7**
- 1,260 total contracts / 7 = **~180 batch calls** (vs 1,260 per-ticker calls)

### Rate Limits

Token-cost system; most GETs cost 10 tokens. Basic tier: 200 tokens/sec = 20 req/sec.
At 180 calls ÷ 20 req/sec ≈ **9 seconds** for full candle ingest.

Kalshi does **not** return BTC spot/index price in any API response. Binance 1-minute klines are the sole source for BTC spot.
