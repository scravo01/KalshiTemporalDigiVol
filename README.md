# KalshiTemporalDigiVol

Research pipeline and backtesting framework for BTC digital options on Kalshi.

## Overview

This project tests whether there is a systematic vol premium in Kalshi hourly BTC binary
options and whether that premium varies by time of day (with focus on the Asian trading
session). It includes:

- A production-quality bronze/silver/gold ETL pipeline pulling from Kalshi and Binance APIs
- An implied vol inversion engine for digital cash-or-nothing calls (closed-form quadratic
  with Brent's method fallback for ATM)
- A full vol surface across all 24 UTC expiry hours
- Statistical analysis: vol premium vs realized vol, skew structure, and intraday vol patterns
- Two delta-hedged backtests: all 24 hours vs Asia session only (00:00–11:59 UTC)

## Quick Start

```bash
uv sync

# Run the full pipeline (requires KALSHI_API_KEY in .env)
uv run kvol pipeline

# Or step by step:
uv run kvol bronze-kalshi --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol bronze-binance --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol silver
uv run kvol vol-surface

# Launch the Streamlit dashboard
uv run streamlit run src/ui/app.py

# Open research notebooks
uv run jupyter lab notebooks/

# Run tests
uv run pytest tests/ -v
```

## Repository Structure

```
src/
  clients/           # Async API clients: Kalshi (RSA-PSS auth) + Binance
  etl/
    bronze/          # Raw API extraction → partitioned parquet by date
    silver/          # Join + IV inversion → contracts.parquet + vol_surface.parquet
    gold/            # Feature engineering + statistical tests → features.parquet
  cli/main.py        # Click CLI entrypoints
  ui/app.py          # Streamlit dashboard
data/
  bronze/            # Raw API data, partitioned by date
  silver/            # contracts.parquet, vol_surface.parquet
  gold/              # features.parquet, summary_stats.csv, rv_iv.parquet, plots/
notebooks/
  vol_research.ipynb            # Research: vol premium, skew, Asia open hypothesis
  backtest_01_all_hours.ipynb   # Delta-hedged short-vol, all 24 UTC hours
  backtest_02_asia_hours.ipynb  # Same strategy, Asia session only (00:00–11:59 UTC)
tests/
docs/                # Component-level documentation
```

## Key Findings (58-day sample, March–May 2026)

- **Vol premium**: ATM IV exceeds 5-min realized vol by ~20 annualized vol points on
  average (p ≈ 0, persistent across all three calendar months in sample). Roughly 73% of
  5-min windows have IV > RV.
- **Asia open hypothesis (Q3)**: ATM IV at 01:00 UTC is *not* significantly higher than
  other expiry hours (one-sided t-test: p = 0.29; Tukey HSD: no pairwise significance).
  Peak ATM IV hour is 10 UTC. Realized vol is elevated at 01:00 UTC relative to 14 UTC,
  but hours 03–12 have higher realized vol than hour 01. The original hypothesis is not
  supported at this sample size.
- **Backtest**: A delta-hedged short-25Δ strategy with IV/RVol ≥ 1.20 filter, 10¢ minimum
  premium, and 50% early-exit generates 78 trades over 58 days. Parameters were set on
  first principles; out-of-sample validation is included in the notebooks.

## Limitations

- 58-day sample covers a single BTC regime; results are not regime-generalizable.
- Thin order books (median 4 strikes per expiry); IV estimates are noisy.
- Kalshi market capacity constrains the backtest to ~$250–$500 notional per trade before
  market impact materially erodes the edge.
- See `notebooks/vol_research.ipynb` for full methodology and caveats.

## Environment

Create a `.env` file with:
```
KEY_ID=<your-kalshi-key-uuid>
KALSHI_API_KEY=<your-rsa-2048-pem-private-key>
```
