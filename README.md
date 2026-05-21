# KalshiTemporalDigiVol

## Mission

This project pulls short-dated binary option data from [Kalshi](https://kalshi.com) to research and implement a systematic short-vol strategy on hourly BTC digital option event contracts. The core thesis: Kalshi's market-maker pricing systematically overcharges for implied volatility relative to realized vol, and that premium can be harvested via a delta-hedged short-OTM position. The project is end-to-end — from authenticated API ingestion through IV inversion, statistical testing, backtesting, and automated daily scheduling via Airflow.

**What this project does:**
- Ingests live and historical BTC binary option data from Kalshi's REST API (RSA-PSS authenticated) and 1-minute BTC spot data from Binance
- Inverts digital Black-Scholes implied vol for every traded minute across all 24 UTC expiry hours, building a full intraday vol surface
- Tests whether ATM IV systematically exceeds 5-minute realized vol and whether the premium is hour-structured
- Implements and backtests a delta-hedged short-OTM strategy with walk-forward validation and bootstrap robustness checks
- Runs the full pipeline on a daily cron schedule via Apache Airflow on Docker

---

## Research Conclusions

The vol premium is real, statistically significant, and extractable. Over 61 trading days (March–May 2026):

| Metric | Value |
|--------|-------|
| Mean IV − RV | +0.161 annualized (~16 vol points) |
| % observations IV > RV | 73% |
| t-test significance | p ≈ 0 |
| Hour-specific cointegration F-test | Significant — per-hour α, β differ materially from pooled |

**All-hours backtest** (short +25Δ OTM, IV/RV ≥ 1.20 filter, 5-min delta hedge):

| Period | Trades | Sharpe | Win Rate | Max Drawdown |
|--------|--------|--------|----------|--------------|
| Full (Mar–May) | 82 | 7.15 | 79.3% | −$3,318 |
| In-sample (Mar–Apr) | 64 | 4.42 | 73.4% | — |
| Out-of-sample (May) | 18 | 21.98 | 100% | $0 |

**Asia-hours filter** (01:00–10:59 UTC only):

| Period | Trades | Sharpe | Win Rate | Max Drawdown |
|--------|--------|--------|----------|--------------|
| Full (Mar–May) | 31 | 10.53 | 90.3% | −$761 |
| In-sample (Mar–Apr) | 23 | 8.02 | 87.0% | −$761 |
| Out-of-sample (May) | 8 | 24.90 | 100% | $0 |

Bootstrap validation (1,000 × 50% subsamples): **P5 Sharpe 3.99**, 100% of subsamples profitable. The Asia-session filter improves Sharpe by 47% and cuts max drawdown by 77% vs all-hours.

> **Full research memo:** [`investment_research.md`](investment_research.md) — covers vol premium analysis, hour-specific cointegration model, complete backtest tables with walk-forward and bootstrap results, capacity analysis, risk factors, and recommended next steps.

---

## How It Works

The pipeline follows a **bronze / silver / gold medallion architecture**. Each layer reads only from the layer directly below it — no cross-layer shortcuts.

```
Binance API ──► BinanceBronzeETL ──► data/bronze/binance_btc_1m.parquet
                                               │
Kalshi API ───► KalshiBronzeETL  ──► data/bronze/kalshi_{markets,candles}_YYYY-MM-DD.parquet
                                               │
                               SilverETL (DuckDB join + IV inversion)
                                      │                    │
                         contracts.parquet          vol_surface.parquet
                                      │                    │
                                GoldETL              RV-IV Analysis
                                      │                    │
                           features.parquet       rv_iv_hourly.parquet
                           summary_stats.csv
```

- **Bronze** — Faithful API copies, minimal transformation. Kalshi data is partitioned per trade date; Binance data is a single file. Zero-volume candle bars are dropped at ingest. All timestamps are UTC.
- **Silver** — DuckDB joins Kalshi candles to Binance spot prices and inverts digital Black-Scholes IV for every traded minute × all 24 UTC expiry hours. Produces `contracts.parquet` (snapshot-level IV) and `vol_surface.parquet` (~2.5M rows, the full minute-by-minute surface).
- **Gold** — Statistical analysis layer. `GoldETL` runs paired t-tests, Wilcoxon signed-rank tests, and Cohen's d on the vol surface features. `rv_iv_analysis` joins realized vol (computed from Binance 1m log returns) against ATM IV to quantify the vol premium at 5-min and hourly resolution.

All timestamps are UTC throughout. [Polars](https://pola.rs) is the primary DataFrame library; DuckDB is used only for silver-layer joins.

---

## Data Pipeline

### Sources

| Source | Data | Auth |
|--------|------|------|
| Kalshi REST API | BTC hourly binary options — cash-or-nothing digital calls settling each UTC hour, ~188 strikes per expiry | RSA-PSS (2048-bit key, not Bearer token) |
| Binance US REST API | BTCUSDT 1-minute klines | None (public) |

Binance routes to `api.binance.us` — the `.com` domain is geo-blocked in the US.

### What Gets Stored

```
data/bronze/
  binance_btc_1m.parquet                  BTC close price, 1-min bars
  kalshi_markets_YYYY-MM-DD.parquet       Contract metadata: ticker, strike, expiry, settlement price
  kalshi_candles_YYYY-MM-DD.parquet       1-min mid-price per contract (cents, UInt8)

data/silver/
  contracts.parquet                       Snapshot-level IV per (date, UTC hour, ticker)
  vol_surface.parquet                     IV at every traded minute × all 24 expiry hours

data/gold/
  rv_iv.parquet                           5-min RV vs ATM IV, vol_premium column
  rv_iv_hourly.parquet                    Same at hourly resolution
  features.parquet                        ATM IV + 25Δ skew features
  summary_stats.csv                       Statistical test results (t-stat, p-value, Cohen's d)
  plots/                                  Boxplots and vol surface visualizations
```

All parquet files use **zstd level 3** compression. Kalshi candle prices are stored as `UInt8` (integer cents 0–100) — 8× smaller than Float64 across ~2M rows per month. Tickers use `Categorical` encoding for efficient parquet dictionary compression.

---

## Launching Airflow (Recommended)

The full pipeline runs as a scheduled Airflow DAG inside Docker. Data is written to `data/` on the host via bind mount — no copy step needed.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.13 + [uv](https://docs.astral.sh/uv/) (for local CLI use outside Docker)
- Port 8080 available

### Required Secrets

You need **two gitignored files** before starting:

**File 1: `.env`** (at repo root)

```bash
# Kalshi API — get from kalshi.com → Account → API Keys → Create Key
KEY_ID=<your-key-uuid>
KALSHI_API_KEY="-----BEGIN RSA PRIVATE KEY-----
...your RSA-2048 PEM private key (multi-line, keep the quotes)...
-----END RSA PRIVATE KEY-----"

# Airflow — generate once, keep stable
AIRFLOW_FERNET_KEY=<base64-fernet-key>
```

Generate the Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())" >> .env
```

**File 2: `.airflow_passwords.json`** (at repo root)

```bash
echo '{"admin": "admin"}' > .airflow_passwords.json
```

This sets the Airflow UI password. Change `"admin"` to any password you prefer.

### Build and Start

```bash
# Build the custom image (installs polars, scipy, etc. — ~5–10 min first time)
docker compose -f docker-compose.airflow.yml build

# Start the stack
docker compose -f docker-compose.airflow.yml up -d

# Open the Airflow UI
open http://localhost:8080   # username: admin  password: admin

# Stop when done (data/ on the host persists)
docker compose -f docker-compose.airflow.yml down
```

### The DAG: `kalshi_etl_pipeline`

The DAG runs automatically at **22:30 UTC daily** (after Kalshi hourly markets settle at ~22:00 UTC). Task dependency graph:

```
bronze_binance
      │
      ▼
bronze_kalshi          ← needs BTC spot to select ATM strike ladder
      │
      ├──────────────────┐
      ▼                  ▼
   silver           vol_surface
      │                  │
      ▼                  ▼
    gold              rv_iv
```

**Manual trigger with a custom date range:**
Airflow UI → DAGs → `kalshi_etl_pipeline` → **Trigger DAG ▶** → **Trigger DAG w/ config**

The UI form exposes two date pickers (`start_date`, `end_date`). Use these to backfill historical data for any date range. The bronze tasks skip dates where parquet files already exist on disk, so re-triggering an existing range is safe and fast.

> Full setup details, log visibility configuration, and Airflow 3.x notes: [`docs/airflow.md`](docs/airflow.md)

---

## CLI Quick Start (Without Docker)

If you prefer to run stages directly without Docker:

```bash
# Install dependencies
uv sync

# Pull data (requires .env with Kalshi credentials)
uv run kvol bronze-binance --start-date 2026-03-21 --end-date 2026-05-18
uv run kvol bronze-kalshi  --start-date 2026-03-21 --end-date 2026-05-18

# Transform
uv run kvol silver         # DuckDB join + IV inversion → contracts.parquet
uv run kvol vol-surface    # Full minute-by-minute vol surface → vol_surface.parquet
uv run kvol gold           # Statistical tests → features.parquet, summary_stats.csv
uv run kvol rv-iv          # RV vs IV premium → rv_iv.parquet, rv_iv_hourly.parquet

# All stages in one command
uv run kvol pipeline --start-date 2026-03-21 --end-date 2026-05-18

# Dashboard
uv run streamlit run src/ui/app.py
```

---

## Replaying the Research

Once data is pulled (via Airflow or the CLI above), open Jupyter and work through the notebooks in order:

```bash
uv run jupyter lab notebooks/
```

| Step | Notebook | What it covers |
|------|----------|----------------|
| 1 | `vol_research.ipynb` | Does ATM IV exceed RV? Hour-specific cointegration (log-log OLS), F-test and LR test, skew structure, intraday vol patterns |
| 2 | `backtest_01_all_hours.ipynb` | Delta-hedged short +25Δ OTM, all 24 UTC expiry hours, walk-forward validation (Mar–Apr in-sample / May out-of-sample) |
| 3 | `backtest_02_asia_hours.ipynb` | Same strategy restricted to Asia session (01:00–10:59 UTC), bootstrap validation (1,000 × 50% subsamples) |

After running the notebooks, read [`investment_research.md`](investment_research.md) for the consolidated findings, tables, risk factors, and recommended next steps.

---

## Technical Highlights

A summary of non-trivial engineering and research decisions, for those who want to dig into the implementation:

### Data Engineering

- **Medallion lakehouse with enforced layer contracts** — bronze reads from APIs only, silver from bronze only, gold from silver only. No cross-layer shortcuts anywhere in the codebase.
- **Incremental ingest** — `KalshiBronzeETL` checks for existing per-date parquet files before requesting data; re-running for an existing date range is a no-op. Only truly new dates hit the API.
- **Async API clients with shared class-level rate limiters** (`src/clients/`) — both `KalshiClient` and `BinanceClient` use `aiohttp` with `ClassVar[AsyncLimiter]`, meaning all instances share one rate-limit budget. Kalshi candle batches run under `asyncio.Semaphore(5)` for controlled concurrency.
- **RSA-PSS request signing from first principles** — no Kalshi SDK. The signing message is `timestamp_ms + METHOD + /trade-api/v2 + path`, signed with the 2048-bit private key from `.env`.
- **Kalshi batch candle optimization** — the batch API caps at 10,000 candlesticks per call. With ~1,440 bars/contract/day, the max tickers per call is `floor(10,000 / 1,440) = 7`, reducing ~1,440 serial calls to ~180 batched calls per day.
- **Historical/live endpoint routing** — `KalshiClient.historical_cutoff` is fetched lazily on the first `fetch_markets` call, then used to route every subsequent request to `/historical/markets` vs `/markets` transparently.
- **Storage-efficient parquet** — all files use zstd level 3. Kalshi prices stored as `UInt8` (exact for integer cent values 0–100; 8× smaller than Float64). Tickers as `Categorical` (parquet dictionary encoding, ~2–4 bytes effective vs 30+ bytes raw string).

### Quantitative Research

- **Closed-form digital Black-Scholes IV inversion** (`src/etl/silver/implied_vol.py`) — substituting `u = σ√T` into the digital call pricing equation yields a quadratic in `u`. Two roots; selection depends on moneyness (ITM takes the larger root, OTM the smaller, ATM falls back to `scipy.optimize.brentq`). O(1) per contract, no iteration for non-ATM strikes.
- **Full vol surface** — IV computed at every traded minute × all 24 UTC expiry hours (~2.5M rows over 61 days). This enables both snapshot-level analysis and the continuous RV-IV comparison.
- **Hour-specific cointegration model** — OLS regression of `log(IV)` on `log(RV)` pooled vs per-hour. F-test and likelihood-ratio test both confirm that the IV/RV relationship is structurally different by UTC hour, motivating the Asia-session filter.
- **Walk-forward validation** — backtest parameters set on first principles (not optimized); March–April used as in-sample, May held out completely. Out-of-sample Sharpe 21.98 with 100% win rate (18 trades).
- **Bootstrap robustness** — 1,000 iterations sampling 50% of trades. P5 Sharpe of 3.99 with 100% of subsamples profitable confirms the result is not driven by a handful of lucky trades.
- **Delta hedge** — BTC position rebalanced every 5 minutes against the option delta (`prob_itm`), with 1 bps transaction cost per rebalance leg.

### Infrastructure

- **Airflow 3.2.1 on Docker** with `LocalExecutor` + postgres metadata DB. Uses `standalone` mode (scheduler + API server in one process). `SimpleAuthManager` with plaintext password file rather than FAB.
- **Secrets via python-dotenv, not docker-compose environment** — `.env` is bind-mounted into the container and read by `load_dotenv()` at CLI startup. Avoids injecting credentials into `docker inspect` output.
- **tqdm in Airflow logs** — tqdm writes `\r`-separated progress updates; Airflow captures them as one log event. Fixed by piping all BashOperator commands through `tr '\r' '\n'` with `set -o pipefail` to preserve exit codes.
- **Test suite** (`tests/`) — async client tests use `aioresponses` to mock HTTP responses without spinning up real servers. Silver ETL tests build fixture DataFrames and run the full DuckDB join + IV inversion pipeline end-to-end.

---

## Repository Structure

```
src/
  clients/           # Async API clients: Kalshi (RSA-PSS) + Binance (public)
  etl/
    bronze/          # Raw API extraction → per-date partitioned parquet
    silver/          # DuckDB join + IV inversion → contracts + vol_surface
    gold/            # Statistical analysis + RV-IV comparison
  cli/main.py        # Click CLI: bronze-kalshi, bronze-binance, silver,
                     #            vol-surface, gold, rv-iv, pipeline
  ui/app.py          # Streamlit dashboard
dags/
  kalshi_etl_dag.py  # Airflow DAG (daily, 22:30 UTC)
data/
  bronze/            # Raw parquets, partitioned by date
  silver/            # contracts.parquet, vol_surface.parquet
  gold/              # rv_iv*.parquet, features.parquet, summary_stats.csv, plots/
notebooks/
  vol_research.ipynb            # Vol premium, cointegration, hour effects
  backtest_01_all_hours.ipynb   # Full backtest + walk-forward
  backtest_02_asia_hours.ipynb  # Asia-session filter + bootstrap
tests/               # pytest suite with aioresponses mocks
docs/                # Component-level documentation
investment_research.md          # Consolidated research memo
Dockerfile                      # Custom Airflow image
docker-compose.airflow.yml      # Postgres + Airflow stack
```

---

## Documentation

| Doc | Description |
|-----|-------------|
| [`investment_research.md`](investment_research.md) | Full strategy memo: methodology, findings tables, bootstrap results, risk factors |
| [`docs/airflow.md`](docs/airflow.md) | Airflow + Docker setup, DAG details, Airflow 3.x notes |
| [`docs/architecture.md`](docs/architecture.md) | Medallion pipeline data flow and layer contracts |
| [`docs/implied_vol.md`](docs/implied_vol.md) | Digital Black-Scholes IV inversion derivation |
| [`docs/data.md`](docs/data.md) | Full schema reference for all bronze, silver, and gold parquets |
| [`docs/README.md`](docs/README.md) | Complete documentation index |
