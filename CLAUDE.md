# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Research pipeline and backtesting framework for BTC digital options on Kalshi. The project has established that a systematic vol premium exists (ATM IV > 5-min RV in 73% of observations, p ≈ 0) and that a delta-hedged short-OTM strategy achieves Sharpe 7.15 across all hours (10.53 Asia-hours only) over the 61-day sample (March–May 2026). See `docs/investment_research.md` for the full findings and `README.md` for a quick summary.

## Commands

```bash
# Create environment and install dependencies
uv sync

# Run a full pipeline stage (uses .env for KALSHI_API_KEY)
uv run kvol bronze-kalshi --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol bronze-binance --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol silver
uv run kvol vol-surface
uv run kvol gold
uv run kvol rv-iv       # computes RV vs IV premium → data/gold/rv_iv*.parquet
uv run kvol pipeline    # runs all stages end-to-end

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
src/cli/main.py      # Click CLI: bronze-kalshi, bronze-binance, silver, vol-surface, gold, rv-iv, pipeline
src/etl/base.py      # Abstract BaseETL with async extract/transform/load
src/etl/bronze/      # Extract API → parquet (raw, minimal transformation)
src/etl/silver/      # Join + IV inversion using DuckDB → contracts.parquet + vol_surface.parquet
src/etl/gold/        # Stats, plots, RV-IV analysis — consumes silver parquets
  gold_etl.py        # ATM IV, 25Δ skew, t-tests, Wilcoxon, Cohen's d → summary_stats.csv
  rv_iv_analysis.py  # 5-min and hourly RV vs IV premium → rv_iv*.parquet
data/{bronze,silver,gold}/
dags/kalshi_etl_dag.py  # Airflow DAG for daily scheduled pipeline
```

**Kalshi API (as of 2026)**: Base URL `https://api.elections.kalshi.com/trade-api/v2`. Authentication is RSA-PSS (not Bearer token) — `.env` must contain `KEY_ID` (UUID) and `KALSHI_API_KEY` (RSA-2048 PEM private key). Signing message: `timestamp_ms + METHOD + /trade-api/v2 + path`. Markets are now **hourly** binary options (e.g., `KXBTCD-26MAY1901-T85799.99`) settling each UTC hour with ~188 strikes. Old daily format (`KXBTCD-25MAY24-B95000`) no longer active.

**Clients** (`src/clients/`): Both clients are async (`aiohttp`) and accept `session: aiohttp.ClientSession | None = None` in their constructor — the CLI creates a session and passes it in. `KalshiClient._limiter` and `BinanceClient._limiter` are **class variables** (`ClassVar[AsyncLimiter]`) so all instances share one rate-limit budget. Kalshi uses 10 req/s, `asyncio.Semaphore(5)` for parallel candle batches, and 3-attempt retry logic. `KalshiClient.historical_cutoff` is fetched lazily on the first `fetch_markets` call (not in `__init__`). Binance uses `api.binance.us` (`.com` is geo-blocked). The Kalshi client routes between `/historical/markets` and `/markets` based on the cutoff from `GET /historical/cutoff`.

**Bronze** (`src/etl/bronze/`): Kalshi candle window starts at 23:00 UTC day-before (to capture T-1 snapshots) and ends at 23:59 UTC on the last date. Zero-volume candle rows are dropped at ingest. Binance window starts 2h before the first trade date and ends 2h after. All writes are zstd level 3 parquet. `KalshiBronzeETL.transform()` annotates candles with `trade_date` (joined from markets); `load()` only partitions by date and writes.

**Candle price format**: New API returns `yes_ask.close_dollars` and `yes_bid.close_dollars` (USD string, e.g., "0.23"). Mid-price is computed and stored as cents (UInt8, 0–100). Old API returned `price.close` as integer cents directly — both formats are handled.

**Silver** (`src/etl/silver/`): Two ETLs share the same DuckDB join logic (`SET TimeZone='UTC'`). `SilverETL` produces `contracts.parquet` with snapshot-level IV per (trade_date, UTC hour, ticker). `VolSurfaceETL` produces `vol_surface.parquet` with IV at every traded minute across all 24 UTC expiry hours. Filter `expiry_time > snapshot_ts` ensures only open markets are sampled. Time-to-expiry T is computed dynamically — not hardcoded. Column `prob_itm` = `digi_px / 100` in both output parquets.

**Implied vol inversion** (`src/etl/silver/implied_vol.py`): Kalshi binary markets are cash-or-nothing digital calls. A closed-form solution: substituting `u = σ√T` into `N⁻¹(p) = ln(S/K)/u − u/2` yields a quadratic in `u`. Root selection: ITM → `u = −d2* + √disc`; OTM → `u = −d2* − √disc`; ATM → `scipy.optimize.brentq`. `compute_t` is kept for tests but not used by the silver ETL.

**25-delta skew**: +25Δ strike is where `N(d2) ≈ 0.75`; −25Δ is where `N(d2) ≈ 0.25`. Kalshi strikes are discrete — use the closest available strike to each delta target.

**Primary DataFrame library**: `polars` everywhere. DuckDB is used only for silver-layer joins.
