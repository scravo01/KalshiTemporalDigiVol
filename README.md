# KalshiTemporalDigiVol

Research pipeline and backtesting framework for BTC digital options on Kalshi. The project measures whether a systematic implied vol premium exists in Kalshi hourly BTC binary markets and whether that premium is concentrated in the Asia trading session — then quantifies it via delta-hedged backtests.

## Key Findings (61-day sample, March–May 2026)

| Metric | Value |
|--------|-------|
| Mean IV − RV | +0.161 annualized (~16 vol points) |
| % observations IV > RV | 73% |
| t-test significance | p ≈ 0 |
| Hour-specific cointegration F-test | Significant — per-hour α, β differ materially |

**All-hours backtest** (short +25Δ OTM, IV/RV ≥ 1.20 filter, 5-min delta hedge):

| Period | Trades | Sharpe | Win Rate | Max DD |
|--------|--------|--------|----------|--------|
| Full (Mar–May) | 82 | 7.15 | 79.3% | −$3,318 |
| In-sample (Mar–Apr) | 64 | 4.42 | 73.4% | — |
| Out-of-sample (May) | 18 | 21.98 | 100% | $0 |

**Asia-hours filter** (01:00–10:59 UTC only):

| Period | Trades | Sharpe | Win Rate | Max DD |
|--------|--------|--------|----------|--------|
| Full (Mar–May) | 31 | 10.53 | 90.3% | −$761 |
| In-sample (Mar–Apr) | 23 | 8.02 | 87.0% | −$761 |
| Out-of-sample (May) | 8 | 24.90 | 100% | $0 |

Bootstrap validation (1,000 × 50% subsamples): P5 Sharpe 3.99, 100% of subsamples profitable.

See [`investment_research.md`](investment_research.md) for the full research memo.

---

## Quick Start

```bash
# Install dependencies
uv sync

# Run the full pipeline (requires KALSHI_API_KEY in .env)
uv run kvol pipeline --start-date 2026-03-21 --end-date 2026-05-18

# Or stage by stage:
uv run kvol bronze-binance --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol bronze-kalshi  --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol silver
uv run kvol vol-surface
uv run kvol gold
uv run kvol rv-iv

# Launch the Streamlit dashboard
uv run streamlit run src/ui/app.py

# Open research notebooks
uv run jupyter lab notebooks/

# Run tests
uv run pytest tests/ -v
```

---

## Repository Structure

```
src/
  clients/           # Async API clients: Kalshi (RSA-PSS auth) + Binance
  etl/
    bronze/          # Raw API extraction → partitioned parquet by date
    silver/          # DuckDB join + IV inversion → contracts.parquet + vol_surface.parquet
    gold/            # Feature engineering, stats, RV-IV analysis → parquets + plots
  cli/main.py        # Click CLI entrypoints (kvol)
  ui/app.py          # Streamlit dashboard
dags/
  kalshi_etl_dag.py  # Airflow DAG: daily scheduled pipeline
data/
  bronze/            # Raw API data, partitioned by date
  silver/            # contracts.parquet, vol_surface.parquet
  gold/              # features.parquet, rv_iv.parquet, rv_iv_hourly.parquet, plots/
notebooks/
  vol_research.ipynb            # Vol premium analysis, cointegration, hour effects
  backtest_01_all_hours.ipynb   # Delta-hedged short-vol, all 24 UTC hours
  backtest_02_asia_hours.ipynb  # Same strategy, Asia session only (01:00–10:59 UTC)
tests/
docs/               # Component-level documentation
Dockerfile          # Custom Airflow image with project deps
docker-compose.airflow.yml  # Postgres + Airflow stack for local scheduling
```

---

## Architecture

```
Binance API ──► BinanceBronzeETL ──► data/bronze/binance_btc_1m.parquet
                                              │
Kalshi API ───► KalshiBronzeETL  ──► data/bronze/kalshi_{markets,candles}_*.parquet
                                              │
                              SilverETL (DuckDB join + IV inversion)
                                     │                  │
                        contracts.parquet        vol_surface.parquet
                                     │                  │
                               GoldETL             RV-IV Analysis
                                     │                  │
                          features.parquet      rv_iv_hourly.parquet
                          summary_stats.csv
```

Each layer reads only from the layer directly below it. All timestamps are UTC. Polars is the primary DataFrame library; DuckDB is used only in silver-layer joins.

---

## Airflow Deployment

A full Airflow + Docker stack is included for automated daily scheduling.

```bash
# One-time setup: generate a Fernet key and add to .env
python -c "from cryptography.fernet import Fernet; print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())" >> .env

# Create the Airflow admin password file (plaintext, gitignored)
echo '{"admin": "admin"}' > .airflow_passwords.json

# Build image and start stack
docker compose -f docker-compose.airflow.yml up -d

# Open Airflow UI
open http://localhost:8080   # admin / admin
```

The DAG `kalshi_etl_pipeline` runs daily at 22:30 UTC. For manual triggers with custom date ranges use **Trigger DAG w/ config** in the UI. Data is written directly to `data/` on the host via bind mount.

See [`docs/airflow.md`](docs/airflow.md) for full setup instructions.

---

## Environment Setup

Create a `.env` file at the repo root:

```bash
# Kalshi API credentials
KEY_ID=<your-kalshi-key-uuid>
KALSHI_API_KEY="-----BEGIN RSA PRIVATE KEY-----
...your RSA-2048 PEM private key...
-----END RSA PRIVATE KEY-----"

# Airflow (only needed for Docker deployment)
AIRFLOW_FERNET_KEY=<base64-fernet-key>
```

The Kalshi API uses RSA-PSS authentication. `KEY_ID` is the UUID shown in your Kalshi API key settings; `KALSHI_API_KEY` is the full RSA-2048 PEM private key (multi-line, quoted).

---

## Limitations

- 61-day sample covers a single BTC regime; results are not regime-generalizable.
- Kalshi market capacity constrains each trade to ~$250–$500 notional before market impact erodes the edge; practical AUM ceiling is ~$50–100k.
- Modeled transaction costs (10 bps slippage) are optimistic — actual Kalshi bid-ask spreads are 12–20 bps.
- Out-of-sample hold-out periods are 8–18 trades; statistically thin.

---

## Documentation

Component-level documentation lives in [`docs/`](docs/). Key references:

| Doc | Description |
|-----|-------------|
| [`investment_research.md`](investment_research.md) | Full strategy research memo with tables and conclusions |
| [`docs/airflow.md`](docs/airflow.md) | Airflow + Docker setup and DAG overview |
| [`docs/architecture.md`](docs/architecture.md) | Medallion pipeline data flow and layer contracts |
| [`docs/implied_vol.md`](docs/implied_vol.md) | Digital Black-Scholes IV inversion derivation |
