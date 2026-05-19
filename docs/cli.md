# CLI Entrypoint

## Purpose

Click-based CLI registered as the `kvol` console script. Provides four commands that map directly to pipeline stages. Loads `.env` before dispatching so `KALSHI_API_KEY` and `KEY_ID` are available without manual export.

## File Location

`src/cli/main.py`

## Entry Point

`pyproject.toml` declares:
```
[project.scripts]
kvol = "src.cli.main:main"
```

`main()` calls `load_dotenv()` then delegates to the Click group `cli`. All commands configure `logging.basicConfig` at `INFO` level using ISO-8601 timestamps.

## Custom Parameter Type

### `_Date` (internal)

Converts a `YYYY-MM-DD` string to a `datetime.date`. Fails with a user-friendly error if the format is wrong. Registered as the module-level constant `DATE`.

## Commands

### `bronze-kalshi`

```
uv run kvol bronze-kalshi [OPTIONS]
```

Fetches Kalshi KXBTCD markets and 1-minute candles and writes per-date parquet files to the bronze directory.

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--start-date` | 90 days ago | — | First trade date (YYYY-MM-DD) |
| `--end-date` | Yesterday | — | Last trade date (YYYY-MM-DD) |
| `--api-key` | — | `KALSHI_API_KEY` | RSA-2048 PEM private key string |
| `--bronze-dir` | `data/bronze` | — | Output directory for parquet files |

Constructs `KalshiClient(api_key=api_key)` (triggers one synchronous `_fetch_historical_cutoff` call) then runs `KalshiBronzeETL(...).run()`.

**Example:**
```bash
uv run kvol bronze-kalshi --start-date 2026-03-21 --end-date 2026-05-18
```

**Outputs**: `data/bronze/kalshi_markets_<YYYY-MM-DD>.parquet` and `data/bronze/kalshi_candles_<YYYY-MM-DD>.parquet` for each trade date.

---

### `bronze-binance`

```
uv run kvol bronze-binance [OPTIONS]
```

Fetches Binance BTCUSDT 1-minute klines and writes a single parquet file. No API key required (public endpoint).

| Flag | Default | Description |
|------|---------|-------------|
| `--start-date` | 90 days ago | First trade date |
| `--end-date` | Yesterday | Last trade date |
| `--bronze-dir` | `data/bronze` | Output directory |

The actual fetch window is extended: starts at 22:00 UTC the day before `start_date` and ends at 02:00 UTC the day after `end_date`, so silver-layer snapshot joins at the edges have spot coverage.

**Example:**
```bash
uv run kvol bronze-binance --start-date 2026-03-21 --end-date 2026-05-18
```

**Output**: `data/bronze/binance_btc_1m.parquet` (single file, all dates).

**Note**: `bronze-binance` must run before `bronze-kalshi` because the Kalshi bronze ETL reads `binance_btc_1m.parquet` to perform ATM strike filtering during ingest.

---

### `silver`

```
uv run kvol silver [OPTIONS]
```

Joins all bronze parquet files, computes implied vol per contract row, and writes the silver parquet.

| Flag | Default | Description |
|------|---------|-------------|
| `--bronze-dir` | `data/bronze` | Source directory |
| `--silver-dir` | `data/silver` | Output directory |

Runs `SilverETL(...).run()`. DuckDB reads all `kalshi_markets_*.parquet` and `kalshi_candles_*.parquet` via glob.

**Output**: `data/silver/contracts.parquet`.

---

### `pipeline`

```
uv run kvol pipeline [OPTIONS]
```

Runs all four stages in sequence: Binance bronze → Kalshi bronze → silver → gold.

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--start-date` | 90 days ago | — | First trade date |
| `--end-date` | Yesterday | — | Last trade date |
| `--api-key` | — | `KALSHI_API_KEY` | RSA-2048 PEM private key |
| `--data-dir` | `data` | — | Root data directory; `bronze/`, `silver/`, `gold/` sub-dirs created automatically |

**Example:**
```bash
# Using .env for credentials
uv run kvol pipeline --start-date 2026-03-21 --end-date 2026-05-18

# With explicit key (CI usage)
uv run kvol pipeline \
  --start-date 2026-03-21 \
  --end-date 2026-05-18 \
  --api-key "$KALSHI_API_KEY"
```

**Order within `pipeline`**:
1. `BinanceBronzeETL.run()` — must precede Kalshi bronze
2. `KalshiBronzeETL.run()` — reads Binance parquet for ATM filtering
3. `SilverETL.run()` — reads all bronze parquets
4. `gold_run()` — reads silver parquet

## Alternative Entry Points

- `run_pipeline.py` at the repo root is a thin shim that calls `uv run kvol pipeline`. It exists for convenience but carries no logic.
- `main.py` at the repo root (if present) delegates identically.

## Gotchas

- The `--api-key` flag accepts the raw PEM string, not a file path. In `.env`, the value must be the full multi-line PEM with literal `\n` sequences, or use `KALSHI_API_KEY="$(cat key.pem)"` in the shell.
- `KEY_ID` (the UUID identifying which key to use) must also be set in `.env`; it is read directly in `KalshiClient.__init__` via `os.environ.get("KEY_ID", "")` and is not a CLI flag.
- Date defaults are computed at call time (`_default_start`, `_default_end`), so the help text shows `90 days ago` / `yesterday` rather than a static date.
