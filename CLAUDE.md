# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Research project testing whether ATM implied vol and 25-delta skew on Kalshi BTC daily binary markets shift around the Asian market open (00:00 UTC). See `PLAN.md` for the full research design, hypothesis, and success criteria.

## Commands

```bash
# Create environment and install dependencies
uv sync

# Run a full pipeline stage (uses .env for KALSHI_API_KEY)
uv run kvol bronze-kalshi --start-date 2026-03-19 --end-date 2026-05-18
uv run kvol bronze-binance --start-date 2026-03-19 --end-date 2026-05-18
uv run kvol silver
uv run kvol pipeline  # runs all stages

# Run tests
uv run pytest tests/ -v

# Run a single test file
uv run pytest tests/test_implied_vol.py -v

# Launch the Streamlit UI (after pipeline has run)
uv run streamlit run src/ui/app.py
```

The `.gitignore` includes `.ruff_cache/`, so Ruff is the intended linter when added.

## Architecture

The pipeline follows a bronze/silver/gold medallion pattern with strict layer contracts — each layer may only read from the layer below, never skip layers or access raw data from gold.

```
src/clients/         # API wrappers — Kalshi and Binance, return polars DataFrames
src/cli/main.py      # Click CLI: bronze-kalshi, bronze-binance, silver, vol-surface, pipeline
src/etl/base.py      # Abstract BaseETL with async extract/transform/load
src/etl/bronze/      # Extract API → parquet (raw, minimal transformation)
src/etl/silver/      # Join + feature engineering using DuckDB
src/etl/gold/        # Stats and plots only — consumes silver parquet
data/{bronze,silver,gold}/
run_pipeline.py      # Thin shim delegating to `kvol pipeline`
```

**Kalshi API (as of 2026)**: Base URL `https://api.elections.kalshi.com/trade-api/v2`. Authentication is RSA-PSS (not Bearer token) — `.env` must contain `KEY_ID` (UUID) and `KALSHI_API_KEY` (RSA-2048 PEM private key). Signing message: `timestamp_ms + METHOD + /trade-api/v2 + path`. Markets are now **hourly** binary options (e.g., `KXBTCD-26MAY1901-T85799.99`) settling each UTC hour with ~188 strikes. Old daily format (`KXBTCD-25MAY24-B95000`) no longer active.

**Clients** (`src/clients/`): Both clients are async (`aiohttp`) and accept `session: aiohttp.ClientSession | None = None` in their constructor — the CLI creates a session and passes it in. `KalshiClient._limiter` and `BinanceClient._limiter` are **class variables** (`ClassVar[AsyncLimiter]`) so all instances share one rate-limit budget. Kalshi uses 10 req/s, `asyncio.Semaphore(5)` for parallel candle batches, and 3-attempt retry logic. `KalshiClient.historical_cutoff` is fetched lazily on the first `fetch_markets` call (not in `__init__`). Binance uses `api.binance.us` (`.com` is geo-blocked). The Kalshi client routes between `/historical/markets` and `/markets` based on the cutoff from `GET /historical/cutoff`.

**Bronze** (`src/etl/bronze/`): Kalshi candle window starts at 23:00 UTC day-before (to capture T-1 snapshots) and ends at 23:59 UTC on the last date. Zero-volume candle rows are dropped at ingest. Binance window starts 2h before the first trade date and ends 2h after. All writes are zstd level 3 parquet. `KalshiBronzeETL.transform()` annotates candles with `trade_date` (joined from markets); `load()` only partitions by date and writes.

**Candle price format**: New API returns `yes_ask.close_dollars` and `yes_bid.close_dollars` (USD string, e.g., "0.23"). Mid-price is computed and stored as cents (UInt8, 0–100). Old API returned `price.close` as integer cents directly — both formats are handled.

**Silver** (`src/etl/silver/`): DuckDB join with `SET TimeZone='UTC'`. Snapshots are T-1 (23:00 UTC prior day), T0 (00:00 UTC), T+1 (01:00 UTC). Filter `expiry_time > snapshot_ts` ensures only open markets are sampled. Time-to-expiry T is computed dynamically as `(expiry_time - snapshot_ts).total_seconds() / (365.25 * 24 * 3600)` — not hardcoded.

**Implied vol inversion** (`src/etl/silver/implied_vol.py`): Kalshi binary markets are cash-or-nothing digital calls. A closed-form solution: substituting `u = σ√T` into `N⁻¹(p) = ln(S/K)/u − u/2` yields a quadratic in `u`. Root selection: ITM → `u = −d2* + √disc`; OTM → `u = −d2* − √disc`; ATM → `scipy.optimize.brentq`. `compute_t` is kept for tests but not used by the silver ETL.

**25-delta skew**: +25Δ strike is where `N(d2) ≈ 0.75`; −25Δ is where `N(d2) ≈ 0.25`. Kalshi strikes are discrete — use the closest available strike to each delta target.

**Primary DataFrame library**: `polars` everywhere. DuckDB is used only for silver-layer joins.
