# Binance Client

## Purpose

Async wrapper around the Binance US REST API. Fetches BTCUSDT 1-minute klines (OHLCV bars) for a requested datetime range and returns a Polars DataFrame with only the columns needed by the pipeline: timestamp, close price, and ingestion timestamp.

## File Location

`src/clients/binance_client.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `aiohttp` | Async HTTP client |
| `aiolimiter.AsyncLimiter` | Rate limiter (20 req/s, well within Binance's 1200/min limit) |
| `polars` | Return type |
| `tqdm.asyncio` | Progress bar on the paginated fetch loop |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `BASE_URL` | `https://api.binance.us` | US endpoint; `api.binance.com` is geo-blocked |
| `_KLINES_LIMIT` | `1000` | Max bars per API response page |
| `_RATE_LIMIT` | `20` | Requests per second (conservative) |
| `_TIMEOUT` | `aiohttp.ClientTimeout(total=30)` | Per-request timeout |

**Why `api.binance.us`**: The `.com` domain is geo-blocked from US IP addresses. The `.us` domain provides the same BTCUSDT klines API with identical request/response shape.

## Class: `BinanceClient`

### Constructor

```python
BinanceClient()
```

No parameters. No API key required — klines are a public endpoint. Session is created lazily on the first call to `_get_session()`.

### Session Management

`_get_session()` creates or re-uses a `ClientSession(timeout=_TIMEOUT)`. Pattern matches `KalshiClient` — session is created lazily to avoid issues with event loop lifecycle.

### Public Methods

#### `fetch_klines(start_dt, end_dt) -> pl.DataFrame`

Fetches all BTCUSDT 1-minute bars in `[start_dt, end_dt)`.

- `start_dt`, `end_dt`: UTC-aware `datetime` objects.
- Paginates from `start_dt` by advancing `current_ms` to `last_bar_ts + 60_000 ms` after each page.
- Stops when: the API returns fewer than `_KLINES_LIMIT` bars, or the last bar's timestamp exceeds `end_ms`.
- Rate-limited to 20 req/s via a per-call `AsyncLimiter` (created fresh each invocation).

**Raw Binance klines response shape** (positional array per bar):

| Index | Content |
|-------|---------|
| 0 | Open time (Unix ms) |
| 4 | Close price (string) |
| others | Open, high, low, volume, trades — discarded |

Only index 0 (timestamp) and index 4 (close) are kept.

**Output schema:**

| Column | Dtype | Notes |
|--------|-------|-------|
| `timestamp` | `Datetime("us", "UTC")` | Bar open time, microsecond precision, UTC |
| `close` | `Float32` | BTC price at end of 1-minute bar, USD |
| `ingested_at` | `Datetime("us", "UTC")` | Timestamp of this pipeline run |

Timestamp conversion: `int(bar[0]) // 1000` converts ms → seconds, then multiplied by `1_000_000` and cast to `Datetime("us")` before replacing time zone to UTC.

Returns an empty DataFrame with the correct schema if no rows are fetched (logged at WARNING).

## Inputs / Outputs

**Input**: Two UTC-aware `datetime` objects defining the fetch window. The bronze ETL extends these to cover snapshot edge cases:
- `start_dt` = `start_date - 1 day` at 22:00 UTC
- `end_dt` = `end_date + 1 day` at 02:00 UTC

**Output file**: `data/bronze/binance_btc_1m.parquet` (written by `BinanceBronzeETL`, not by the client itself).

## Gotchas

- `Float32` is sufficient for BTC prices up to ~$99,999.99 at cent precision (7 significant digits). Do not widen to `Float64` — it wastes memory across millions of rows with no benefit.
- The client has no retry logic. A transient network error will raise immediately. The Binance US API is generally stable; the pipeline team accepted this tradeoff.
- The rate limiter is constructed inside `fetch_klines`, not shared at the class level. Multiple concurrent calls from different tasks would not respect a shared budget — but in practice `BinanceBronzeETL` calls this method once per pipeline run.
- Binance returns bars in ascending time order. The pagination logic assumes this.
