# Kalshi Client

## Purpose

Async wrapper around the Kalshi Trade API v2. Handles RSA-PSS authentication, rate limiting, retry logic, historical vs. live endpoint routing, and normalization of two different candle response shapes into a single Polars DataFrame schema.

## File Location

`src/clients/kalshi_client.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `aiohttp` | Async HTTP; `ClientSession` reused across calls |
| `aiolimiter.AsyncLimiter` | Token-bucket rate limiter (10 req/s shared across all calls) |
| `cryptography` | RSA-PSS signing via `hazmat.primitives` |
| `polars` | All return types |
| `tqdm` / `tqdm.asyncio` | Progress bars for paginated market fetch and concurrent candle batches |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `BASE_URL` | `https://api.elections.kalshi.com/trade-api/v2` | API base; do not use the old `external-api.kalshi.com` |
| `_API_PATH_PREFIX` | `/trade-api/v2` | Prepended to the path component when constructing the signing message |
| `BATCH_SIZE` | `5` | Max tickers per candle request |
| `CONCURRENCY` | `5` | Parallel candle batch requests (`asyncio.Semaphore`) |
| `RATE_LIMIT` | `10` | Requests per second |
| `_MAX_RETRIES` | `3` | Retry attempts for transient failures |
| `_RETRY_BACKOFF` | `[1.0, 3.0, 9.0]` | Seconds between retries (exponential, not geometric) |

## Class: `KalshiClient`

### Constructor

```python
KalshiClient(api_key: str)
```

- `api_key`: Full RSA-2048 PEM private key string (the `KALSHI_API_KEY` env var value).
- Reads `KEY_ID` from `os.environ` (the UUID associated with the key on Kalshi's platform).
- Loads the PEM key via `cryptography.hazmat.primitives.serialization.load_pem_private_key`.
- Creates a shared `AsyncLimiter(10, 1.0)` used by all subsequent API calls.
- **Immediately calls `asyncio.run(self._fetch_historical_cutoff())`** — this is a blocking network call at construction time. Do not construct `KalshiClient` inside an already-running event loop.

### Authentication

Kalshi uses **RSA-PSS** (not Bearer token / HMAC).

Every request signs a message constructed as:

```
message = timestamp_ms_str + METHOD.upper() + "/trade-api/v2" + path
```

Where:
- `timestamp_ms_str` = current Unix time in milliseconds as a string
- `METHOD` = `"GET"` (always GET for this client)
- `path` = the endpoint path starting with `/`, e.g. `/historical/markets`

The signature algorithm: `PSS(MGF1(SHA-256), salt_length=DIGEST_LENGTH)`.

Three request headers carry auth:

| Header | Value |
|--------|-------|
| `KALSHI-ACCESS-KEY` | `KEY_ID` (UUID string) |
| `KALSHI-ACCESS-TIMESTAMP` | `timestamp_ms_str` |
| `KALSHI-ACCESS-SIGNATURE` | Base64-encoded PSS signature |

Headers are regenerated per request (timestamp rotates).

### Session Management

`_get_session()` creates or re-uses an `aiohttp.ClientSession` lazily. The session is not created in `__init__` because `asyncio.run()` there creates and destroys its own event loop; a session created in that loop would be invalid in the ETL's subsequent `asyncio.run()` call.

### Retry Logic (`_get`)

- Retries on: `aiohttp.ClientResponseError` with status 5xx or 429, `asyncio.TimeoutError`, `aiohttp.ClientError`.
- Does **not** retry on 4xx responses (except 429) — these are client errors where retrying will not help.
- Backoff sequence: immediate → 1s → 3s → 9s (3 total attempts).
- Raises the last exception if all retries are exhausted.

### Endpoint Routing

At construction, `_fetch_historical_cutoff()` calls `GET /historical/cutoff` and stores the result as `self.historical_cutoff` (a UTC-aware `datetime`).

`_is_historical(trade_date)` returns `True` if the trade date precedes the cutoff.

`fetch_markets()` iterates over two passes — `use_historical=True` (path `/historical/markets`) and `use_historical=False` (path `/markets`) — skipping whichever pass is not needed for the requested date range.

### Public Methods

#### `fetch_markets(start_date, end_date) -> pl.DataFrame`

Fetches all `KXBTCD` market metadata for the date range. Paginates via `cursor` until no cursor is returned.

**Filtering applied during ingest:**
- Only `KXBTCD` tickers are kept (`ticker.startswith("KXBTCD")`).
- Strike must be parseable (skips malformed tickers).
- `trade_date` must fall within `[start_date, end_date]`.

**Output schema:**

| Column | Dtype | Notes |
|--------|-------|-------|
| `ticker` | `Categorical` | e.g. `KXBTCD-26MAY1901-T85799.99` |
| `trade_date` | `Date` | Calendar date extracted from ticker |
| `strike` | `UInt32` | Strike price in whole dollars |
| `expiry_time` | `Datetime("us", "UTC")` | Settlement timestamp |
| `status` | `Categorical` | `open`, `settled`, `unknown` |
| `settlement_price` | `Float32` | `1.0` (yes), `0.0` (no), or null if unsettled |
| `ingested_at` | `Datetime("us", "UTC")` | Timestamp of this pipeline run |

Returns `_empty_markets_df()` (correct schema, zero rows) if no matching markets are found.

#### `fetch_candles(tickers, start_ts, end_ts, is_historical=False, period_interval=1) -> pl.DataFrame`

Fetches 1-minute candles for all tickers. Splits into batches of `BATCH_SIZE` (5) and runs up to `CONCURRENCY` (5) batches concurrently via `asyncio.Semaphore`.

- HTTP 400 on a batch is treated as "settled/unavailable" and that batch is silently skipped (logged at WARNING).
- Zero-volume rows are dropped after collection (logged at DEBUG).

**Output schema:**

| Column | Dtype | Notes |
|--------|-------|-------|
| `ticker` | `Categorical` | |
| `timestamp` | `Datetime("us", "UTC")` | End of the 1-minute bar |
| `close` | `UInt8` | Mid-price in cents (0–100) |
| `volume` | `UInt32` | Contracts traded |
| `ingested_at` | `Datetime("us", "UTC")` | |

## Candle Price Format Normalization

The Kalshi API has returned candle prices in two incompatible formats across versions. `_parse_candle_response` handles both:

**Old format** (`price.close` — integer cents):
```json
{"candles": {"TICKER": [{"price": {"close": 45}, "volume": 12, ...}]}}
```
Used directly as integer cents.

**New format** (`yes_ask.close_dollars` / `yes_bid.close_dollars` — USD strings):
```json
{"markets": [{"market_ticker": "TICKER", "candlesticks": [
  {"yes_ask": {"close_dollars": "0.48"}, "yes_bid": {"close_dollars": "0.42"}, "volume": 12}
]}]}
```
Mid-price computed as `(ask + bid) / 2`, then multiplied by 100 to convert to cents.

If only one side is present, that side alone is used. Any candle where the result falls outside `[0, 100]` is discarded.

A third legacy shape (`markets_candles` key) is also handled for completeness.

## Ticker Parsing

**New hourly format** (as of 2026): `KXBTCD-26MAY1901-T85799.99`
- Date part: `26MAY1901` → year `26` (2026), month `MAY`, day `19`, hour `01`
- Strike: everything after `-T`, rounded to nearest integer → `85800`

**Old daily format** (no longer active): `KXBTCD-25MAY24-B95000`
- Date part: `25MAY24` → parsed with `%d%b%y`
- Strike: everything after `-B` → `95000`

Both `_parse_strike` and `_parse_trade_date` handle both formats. Unknown formats return `None` (row skipped).

## Timestamp Parsing (`_parse_timestamp`)

Accepts Unix epoch (int or float), ISO-8601 strings (with or without `Z`), or `None`. Always returns a UTC-aware `datetime` or `None`.

## Gotchas

- `KalshiClient.__init__` makes a blocking network call (`asyncio.run`). If it fails (network error, bad credentials), the constructor raises immediately before any ETL work begins.
- The `KALSHI-ACCESS-TIMESTAMP` header must match the actual time within a small window (typically 30 seconds). Clock skew will cause 401 errors.
- `settlement_price` parsing accepts `"yes"` → `1.0`, `"no"` → `0.0`, numeric strings, or passes through as `None` for `"unknown"` / null. This covers both old and new API response shapes.
- The `asyncio.Semaphore(CONCURRENCY)` is constructed inside `fetch_candles` on each call, not shared across calls. This is intentional — the semaphore is per-ETL-run.
