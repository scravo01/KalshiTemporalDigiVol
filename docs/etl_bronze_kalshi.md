# Bronze ETL — Kalshi

## Purpose

Fetches Kalshi KXBTCD market metadata and 1-minute candles for a date range, filters to a strike ladder around ATM, and writes one pair of parquet files per trade date to `data/bronze/`.

## File Location

`src/etl/bronze/kalshi_bronze.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `KalshiClient` | API calls — see `docs/client_kalshi.md` |
| `BaseETL` | Abstract extract / transform / load lifecycle |
| `polars` | DataFrames |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `BRONZE_DIR` | `Path("data/bronze")` | Default output directory |
| `SETTLEMENT_HOURS` | `list(range(24))` | All 24 UTC hours; all hourly Kalshi contracts in scope |

## File Naming

```python
kalshi_markets_<trade_date>.parquet   # e.g. kalshi_markets_2026-05-19.parquet
kalshi_candles_<trade_date>.parquet
```

One file pair per calendar trade date. This allows incremental runs: if a date's markets file already exists on disk, that date is skipped entirely.

## Class: `KalshiBronzeETL`

Inherits `BaseETL`. Synchronous entry point is `.run()` (inherited).

### Constructor

```python
KalshiBronzeETL(
    client: KalshiClient,
    start_date: date,
    end_date: date,
    bronze_dir: Path = BRONZE_DIR,
    settlement_hours: list[int] = SETTLEMENT_HOURS,
)
```

| Parameter | Description |
|-----------|-------------|
| `client` | Pre-constructed `KalshiClient` (auth already resolved) |
| `start_date` | First trade date to fetch |
| `end_date` | Last trade date to fetch (inclusive) |
| `bronze_dir` | Output directory |
| `settlement_hours` | UTC hours to retain (default: all 24); controls which expiry windows are written |

### `extract() -> tuple[pl.DataFrame, pl.DataFrame]`

Returns `(markets_df, candles_df)`.

**Incremental check**: Computes the set of dates whose `kalshi_markets_<date>.parquet` does not yet exist. Only fetches API data for missing dates. If all dates are present, returns `(empty DataFrame, empty candles DataFrame)` immediately.

**Market fetch**:
1. Calls `client.fetch_markets(fetch_start, fetch_end)`.
2. Filters to missing dates only.
3. Filters to `settlement_hours` (expiry UTC hour must be in the list).

**ATM strike ladder filter** (requires Binance parquet):
- Reads `data/bronze/binance_btc_1m.parquet`.
- Raises `FileNotFoundError` if it does not exist — `bronze-binance` must run first.
- Calls `_filter_strike_ladder()` to retain only ATM ± 4 strikes at $500 increments per expiry window.

**Candle fetch**:
- Groups markets by `expiry_time`.
- For each expiry window: fetches candles from `expiry_ts - 1 hour` to `expiry_ts` at 1-minute intervals.
- Candles for all tickers in a window are fetched in a single batched call.

### `transform(raw) -> tuple[pl.DataFrame, pl.DataFrame]`

Pass-through. No transformation is applied — the client handles normalization and zero-volume filtering.

### `load(data) -> None`

Writes parquet files split by `trade_date`.

**Market parquet write:**
```python
date_markets.write_parquet(mpath, compression="zstd", compression_level=3)
```

**Candle parquet write:**
- Joins candles to markets on `ticker` to recover `trade_date` (candles don't carry it natively).
- Drops `trade_date` before writing (it's encoded in the filename).
- Skips write and logs a WARNING if `date_candles` is empty for a given date.

**Atomicity**: Files are written date by date within a single run. A failed run may leave some dates written and others not; the incremental check in `extract()` ensures a re-run will fetch only the remaining dates.

## Strike Ladder Functions

### `_strike_ladder(spot, available_strikes, n_steps=4, step=500) -> set[int]`

Returns ATM ± `n_steps` strikes at `$step` increments, snapped to the nearest available strike. ATM is the available strike closest to `spot`.

With defaults: ATM ± 4 × $500 = 9 strikes per expiry window.

### `_filter_strike_ladder(markets_df, binance_df, n_steps=4, step=500) -> pl.DataFrame`

For each unique `expiry_time` in `markets_df`:
1. Looks up BTC spot at `expiry_ts - 1 hour` from `binance_df` (latest bar at or before that time).
2. Calls `_strike_ladder` with the available strikes for that expiry.
3. Keeps only matching `(expiry_time, strike)` rows.

If no Binance spot is found for an expiry window, that window is skipped with a WARNING (not an error).

Returns `markets_df.clear()` (empty, correct schema) if no strikes are selected at all.

## Output Schemas

### `kalshi_markets_<date>.parquet`

| Column | Dtype | Notes |
|--------|-------|-------|
| `ticker` | `Categorical` | e.g. `KXBTCD-26MAY1901-T85799.99` |
| `trade_date` | `Date` | Calendar date |
| `strike` | `UInt32` | Strike price in whole dollars |
| `expiry_time` | `Datetime("us", "UTC")` | Settlement UTC timestamp |
| `status` | `Categorical` | `open`, `settled`, `unknown` |
| `settlement_price` | `Float32` | `1.0` / `0.0` / null |
| `ingested_at` | `Datetime("us", "UTC")` | |

### `kalshi_candles_<date>.parquet`

| Column | Dtype | Notes |
|--------|-------|-------|
| `ticker` | `Categorical` | Foreign key to markets file |
| `timestamp` | `Datetime("us", "UTC")` | End of 1-minute bar |
| `close` | `UInt8` | Mid-price in cents (0–100); see candle price format below |
| `volume` | `UInt32` | Zero-volume rows already dropped |
| `ingested_at` | `Datetime("us", "UTC")` | |

## Candle Price Format

See `docs/client_kalshi.md` for the full dual-format normalization logic. In summary:

- **Old API**: `price.close` integer cents → stored directly as `UInt8`.
- **New API**: `yes_ask.close_dollars` and `yes_bid.close_dollars` USD strings → mid = `(ask + bid) / 2 * 100` → rounded to integer cents → `UInt8`.

Both produce values in `[0, 100]`. Values outside this range are discarded before writing.

## Known Limitations

- The incremental check uses only the markets file presence. If the candles file is missing for a date whose markets file exists (e.g., partial write), the date will not be re-fetched.
- `SETTLEMENT_HOURS` defaults to all 24 hours. The original three-snapshot design only needed hours 0, 1, 23. Fetching all 24 increases API call volume and storage by approximately 8×.
- The candle window is fixed at `expiry_ts - 1 hour` to `expiry_ts`. This captures one bar per minute per strike per window, matching what the silver ETL expects.
