# Bronze ETL — Binance

## Purpose

Fetches BTCUSDT 1-minute klines from Binance US and writes a single parquet file to `data/bronze/`. The fetch window is padded beyond the requested trade-date range to ensure BTC spot prices are available at all snapshot times used by the silver layer (including T-1 at 23:00 UTC the prior day and T+1 at 01:00 UTC the following day).

## File Location

`src/etl/bronze/binance_bronze.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `BinanceClient` | API calls — see `docs/client_binance.md` |
| `BaseETL` | Extract / transform / load lifecycle |
| `polars` | DataFrames |

## Class: `BinanceBronzeETL`

Inherits `BaseETL`. Synchronous entry point is `.run()` (inherited).

### Constructor

```python
BinanceBronzeETL(
    client: BinanceClient,
    start_date: date,
    end_date: date,
    bronze_dir: Path = Path("data/bronze"),
)
```

| Parameter | Description |
|-----------|-------------|
| `client` | Pre-constructed `BinanceClient` |
| `start_date` | First trade date of interest |
| `end_date` | Last trade date of interest |
| `bronze_dir` | Output directory |

`self.klines_path = bronze_dir / "binance_btc_1m.parquet"` — the single output file path.

### `extract() -> pl.DataFrame`

Constructs the padded fetch window and delegates to `client.fetch_klines()`:

```
start_dt = (start_date - 1 day) at 22:00 UTC
end_dt   = (end_date   + 1 day) at 02:00 UTC
```

The padding ensures:
- The T-1 snapshot at 23:00 UTC on `start_date - 1` has spot coverage for the ATM reference lookup in `_filter_strike_ladder`.
- The T+1 snapshot at 01:00 UTC on `end_date + 1` has spot coverage for silver-layer joins.

Returns the raw `pl.DataFrame` from `BinanceClient.fetch_klines`.

### `transform(raw) -> pl.DataFrame`

Pass-through. No transformation is applied.

### `load(data) -> None`

```python
self.bronze_dir.mkdir(parents=True, exist_ok=True)
data.write_parquet(self.klines_path, compression="zstd", compression_level=3)
```

Overwrites `binance_btc_1m.parquet` on every run — there is no incremental check. This is intentional: re-running with a wider date range should extend the file, and the file is small enough (~0.5 MB for 60 days) that a full rewrite is faster than appending.

## Inputs / Outputs

**Input**: Binance REST API `GET /api/v3/klines` (public, no authentication).

**Output**:

| File | Notes |
|------|-------|
| `data/bronze/binance_btc_1m.parquet` | Single file covering the padded window |

### Output Schema

| Column | Dtype | Notes |
|--------|-------|-------|
| `timestamp` | `Datetime("us", "UTC")` | Bar open time, microsecond precision |
| `close` | `Float32` | BTC/USDT price at end of bar, USD |
| `ingested_at` | `Datetime("us", "UTC")` | Timestamp of this pipeline run |

## Ordering Constraint

`BinanceBronzeETL` **must run before** `KalshiBronzeETL`. The Kalshi bronze ETL reads `binance_btc_1m.parquet` during its `extract()` phase to compute the ATM reference price for strike-ladder filtering. If the Binance file does not exist, `KalshiBronzeETL.extract()` raises `FileNotFoundError`.

The `pipeline` CLI command enforces this order by calling Binance bronze first.

## Gotchas

- There is no incremental/idempotency check. Every run of `bronze-binance` overwrites the file. If you extend the date range, re-run `bronze-binance` before re-running `bronze-kalshi` or `silver`.
- `Float32` is used for `close` (not `Float64`). This is intentional — see `docs/client_binance.md` for the precision rationale.
- If Binance has a data gap (e.g., maintenance window), `fetch_klines` will skip ahead naturally since the API returns fewer than 1000 bars. The silver ETL handles this via a LEFT JOIN on Binance (in `vol_surface_etl.py`) or by the inner join in the main silver query dropping those rows.
