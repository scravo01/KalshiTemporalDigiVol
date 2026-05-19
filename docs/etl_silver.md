# Silver ETL

## Purpose

Joins all bronze parquet files using DuckDB, assigns snapshot labels to each Kalshi contract's expiry window, computes implied vol per contract row via digital Black-Scholes inversion, and writes a single analysis-ready parquet file. This is the primary layer consumed by gold ETL and the Streamlit UI.

## File Location

`src/etl/silver/silver_etl.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `duckdb` | Cross-parquet glob join with UTC timezone enforcement |
| `polars` | DataFrames |
| `invert_iv` | Closed-form IV inversion — see `docs/implied_vol.md` |
| `tqdm` | Progress bar on the row-by-row IV computation loop |
| `BaseETL` | Extract / transform / load lifecycle |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `BRONZE_DIR` | `Path("data/bronze")` | Default input |
| `SILVER_DIR` | `Path("data/silver")` | Default output |
| `_MIN_VALID_STRIKES` | `3` | Groups with fewer valid strikes are dropped |
| `_SECONDS_PER_YEAR` | `365.25 * 24 * 3600` | Used in time-to-expiry calculation |

## Class: `SilverETL`

Inherits `BaseETL`. Synchronous entry point is `.run()` (inherited).

### Constructor

```python
SilverETL(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
)
```

Output path: `silver_dir / "contracts.parquet"`.

### `extract() -> pl.DataFrame`

Opens a DuckDB in-memory connection, creates three views over bronze parquet globs, and runs `_JOIN_SQL`.

```python
conn.execute("SET TimeZone='UTC'")
conn.execute("CREATE VIEW kalshi_markets AS SELECT * FROM read_parquet('data/bronze/kalshi_markets_*.parquet')")
conn.execute("CREATE VIEW kalshi_candles AS SELECT * FROM read_parquet('data/bronze/kalshi_candles_*.parquet')")
conn.execute("CREATE VIEW binance_klines  AS SELECT * FROM read_parquet('data/bronze/binance_btc_1m.parquet')")
raw_df = conn.execute(_JOIN_SQL).pl()
conn.close()
```

Raises `FileNotFoundError` if any required glob returns no files or the Binance file is absent.

#### DuckDB Join Logic (`_JOIN_SQL`)

Two CTEs:

**`first_bar`**: For each ticker, finds the timestamp of the first volume-positive candle within the contract's 60-minute window (`expiry_time - 60 min` to `expiry_time`). This gives one representative candle per contract — the opening price of active trading for that window.

**`snapshot_candles`**: Joins that first bar back to the markets table. Assigns a snapshot label based on the `expiry_time` UTC hour:

| Expiry UTC hour | Snapshot label |
|----------------|----------------|
| 01 | T0 |
| 00 | T-1 |
| 02 | T+1 |
| 03–13 | T+2 through T+12 |
| 23–14 | T-2 through T-11 |

**Final SELECT**: Joins `snapshot_candles` to `binance_klines` on:
```sql
b.timestamp = sc.snapshot_ts - INTERVAL '1 minute'
```
This offset exists because Binance timestamps mark bar **open** time while Kalshi timestamps mark bar **close** time. The Binance bar that started 1 minute before the Kalshi close covers the same 60-second window.

`WHERE sc.snapshot IS NOT NULL` ensures rows whose expiry hour maps to no label (none exist in the current schema) are dropped.

### `transform(raw) -> pl.DataFrame`

Iterates over all rows to compute implied vol and delta:

**Time-to-expiry**: Computed dynamically per row:
```python
T = (expiry_time - snapshot_ts).total_seconds() / _SECONDS_PER_YEAR
```
Not hardcoded. If `expiry_time <= snapshot_ts` (expired at snapshot), `T = None` and IV is skipped.

**IV inversion**: `invert_iv(digi_px_cents, btc_close, strike, T)` — see `docs/implied_vol.md`.

**Delta**: `digi_px / 100.0` cast to `Float32`. For a digital call, the price in cents / 100 is the risk-neutral probability, which approximates delta.

**Output column types and renaming**:
- `ticker` → `digi_contract_name` (Categorical)
- `digi_px` → `UInt8` (raw cents)
- `delta` → `Float32`
- `implied_vol` → `Float32` (cast from `Float64` produced by `invert_iv`)

**Quality filters applied in order:**
1. Drop rows with `volume == 0`.
2. Drop rows with `implied_vol == null`.
3. Drop entire `(trade_date, snapshot)` groups where fewer than `_MIN_VALID_STRIKES` (3) rows remain. This prevents skew calculations on degenerate groups. Dropped groups are logged at INFO.

### `load(data) -> None`

```python
self.silver_dir.mkdir(parents=True, exist_ok=True)
data.write_parquet(self.silver_path, compression="zstd", compression_level=3)
```

Overwrites `contracts.parquet` on every run. Re-running silver after adding new bronze dates replaces the full file.

## Output Schema: `data/silver/contracts.parquet`

| Column | Dtype | Notes |
|--------|-------|-------|
| `trade_date` | `Date` | Calendar date of the expiry window |
| `snapshot` | `Categorical` | e.g. `T0`, `T-1`, `T+2` |
| `snapshot_ts` | `Datetime("us", "UTC")` | Timestamp of the first-bar candle |
| `digi_contract_name` | `Categorical` | Kalshi ticker |
| `strike` | `UInt32` | Strike price in whole dollars |
| `expiry_time` | `Datetime("us", "UTC")` | Settlement timestamp |
| `digi_px` | `UInt8` | Close price in cents (0–100) |
| `delta` | `Float32` | `digi_px / 100` (risk-neutral probability) |
| `implied_vol` | `Float32` | Annualised implied vol (e.g. `0.85` = 85%) |
| `btc_close` | `Float32` | BTC spot at `snapshot_ts - 1 min` close |
| `volume` | `UInt32` | Contracts traded in the first bar |

## Related ETL: `VolSurfaceETL`

`src/etl/silver/vol_surface_etl.py` contains `VolSurfaceETL`, a variant that uses **all** volume-positive bars within each 60-minute window (not just the first bar). It adds a `minutes_to_expiry` column and writes to `data/silver/vol_surface.parquet`.

Key differences from `SilverETL`:

| Aspect | SilverETL | VolSurfaceETL |
|--------|-----------|---------------|
| Bars per contract | First bar only | All bars in window |
| Binance join | INNER JOIN | LEFT JOIN (gaps retained, then dropped in Python) |
| Output file | `contracts.parquet` | `vol_surface.parquet` |
| CLI wired | Yes (`kvol silver`) | No — invoke manually |

`VolSurfaceETL` is consumed by `gold_etl._save_vol_surface_plots()` when `SURFACE_PATH` exists.

## Gotchas

- `SET TimeZone='UTC'` in DuckDB is essential. Without it, DuckDB may interpret `INTERVAL` arithmetic with local timezone offsets, producing incorrect snapshot alignments.
- The Binance timestamp offset (`- INTERVAL '1 minute'`) is a deliberate design choice, not a bug. Removing it would misalign Kalshi (close-time) and Binance (open-time) bar conventions.
- The silver ETL reads ALL bronze date files on every run via glob. With 60 dates × 2 files, this is fast (DuckDB lazy scan). With 500+ dates it may be worth partitioning.
- `compute_t()` in `implied_vol.py` is a legacy helper that hardcodes T values for T-1/T0/T+1 only. It is **not** called by `SilverETL`, which computes T dynamically. It exists for test compatibility.
