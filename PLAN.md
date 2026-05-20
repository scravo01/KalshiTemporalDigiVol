# BTC Kalshi Asia Vol Shift — Project Plan

## Hypothesis

When Asian traders wake up (~00:00 UTC), there is a pickup in realized volatility in BTC spot that is reflected in the implied volatility of Kalshi daily BTC binary markets. We test whether ATM implied vol and 25-delta skew shift meaningfully around the Asian open.

## Research Design

We extract two features at three fixed UTC snapshots for every available trading day:

- **T-1:** 23:00 UTC (one hour before Asia open)
- **T0:** 00:00 UTC (Asia open)
- **T+1:** 01:00 UTC (one hour after Asia open)

**Feature 1 — ATM Implied Vol:** Implied vol of the Kalshi contract whose strike is closest to BTC spot at snapshot time.

**Feature 2 — 25-Delta Skew:** `(IV(+25Δ strike) − IV(−25Δ strike)) / ATM IV`

Where +25Δ strike is the contract where N(d2) ≈ 0.75 and −25Δ strike is where N(d2) ≈ 0.25. Kalshi strikes are discrete so we take the closest available strike to each delta target.

The statistical test is a paired t-test and Wilcoxon signed-rank test on the daily shift (T0 − T-1) and (T+1 − T0) for both features.

## Data Sources

**Kalshi** — BTC daily binary markets (`KXBTCD` series). These are cash-or-nothing digital calls: "Will BTC be above $X at 4pm ET?". Contract prices (0–100 cents) are risk-neutral probabilities which we invert via Black-Scholes digital pricing to recover implied vol. Pull via Kalshi REST API — historical markets endpoint for settled contracts older than 3 months, live markets endpoint for recent.

**Binance** — `BTCUSDT` 1-minute klines as the BTC spot/underlier price series. Timestamps must be UTC-aligned to match Kalshi candlestick data precisely.

## Implied Vol Method

A Kalshi daily binary is economically equivalent to a cash-or-nothing digital call. We invert the digital Black-Scholes formula to recover implied vol:

```
d2 = (log(S/K) + (r - 0.5σ²)T) / (σ√T)
Digital call price = N(d2)
```

Invert numerically using Brent's method (`scipy.optimize.brentq`). Filter out contracts priced below 2 or above 98 cents — too deep ITM/OTM to produce reliable vol estimates. Require at least 3 valid strikes per snapshot to produce a skew estimate.

## Tech Stack

| Tool | Role |
|---|---|
| `polars` | Primary DataFrame library throughout — all clients and ETL layers return polars DataFrames |
| `duckdb` | SQL-style joins and windowing in the silver layer — joining Kalshi candles to Binance spot across millions of rows |
| `scipy` | `brentq` for implied vol inversion |
| `matplotlib` / `seaborn` | Output plots in gold layer |

## Repository Structure

```
KalshiTemporalDigiVol/
├── data/
│   ├── bronze/                              # raw API data, partitioned by date
│   │   ├── kalshi_markets_<date>.parquet    # market metadata — ticker, strike, expiry
│   │   ├── kalshi_candles_<date>.parquet    # 1min candlesticks for all tickers
│   │   └── binance_btc_1m.parquet           # BTC spot 1min OHLCV
│   ├── silver/                              # analysis-ready features
│   │   ├── contracts.parquet                # (date, snapshot, strike, implied_vol, prob_itm, ...)
│   │   └── vol_surface.parquet              # full 60-min window per expiry (123k rows)
│   └── gold/                               # presentation-ready outputs
│       ├── features.parquet                 # (trade_date, expiry_hour, atm_iv, skew_25d, ...)
│       ├── rv_iv.parquet                    # 5-min realized vol vs ATM IV
│       ├── summary_stats.csv                # statistical test results
│       └── plots/                           # saved chart images
├── src/
│   ├── clients/
│   │   ├── kalshi_client.py                 # Kalshi API — RSA-PSS auth, rate-limited
│   │   └── binance_client.py                # Binance API — returns polars DataFrames
│   └── etl/
│       ├── bronze/
│       │   ├── kalshi_bronze.py             # extract Kalshi → data/bronze/
│       │   └── binance_bronze.py            # extract Binance → data/bronze/
│       ├── silver/
│       │   ├── implied_vol.py               # digital BS IV inversion
│       │   ├── silver_etl.py                # bronze → snapshot features → contracts.parquet
│       │   └── vol_surface_etl.py           # bronze → full vol surface → vol_surface.parquet
│       └── gold/
│           ├── gold_etl.py                  # silver → ATM IV features + stats → data/gold/
│           └── rv_iv_analysis.py            # realized vol vs implied vol analysis
├── notebooks/
│   ├── vol_research.ipynb                   # research narrative: vol premium, skew, Asia open
│   ├── backtest_01_all_hours.ipynb          # delta-hedged short-vol, all 24 hours
│   └── backtest_02_asia_hours.ipynb         # same strategy, Asia session only
├── run_pipeline.py                          # thin shim → `kvol pipeline`
├── pyproject.toml
└── README.md
```

## ETL Layer Contracts

### Bronze — "Raw but Reliable"
- Faithful copy of API response with minimal transformation
- Standardize column names and dtypes only
- Add `ingested_at` UTC timestamp to every row
- Never drop rows — if it came from the API it lives in bronze
- Both Kalshi and Binance data must share a common UTC timestamp column for downstream joins

### Silver — "Analysis Ready"
- Join Kalshi 1min candles to Binance spot on UTC timestamp using DuckDB
- Filter to the three snapshot windows per day (23:00, 00:00, 01:00 UTC)
- Compute implied vol per (ticker, snapshot) using digital BS inversion
- Identify ATM, +25Δ, and −25Δ strikes using delta targeting against discrete available strikes
- Output one row per `(trade_date, snapshot)` with `atm_iv` and `skew_25d`
- Drop rows where fewer than 3 valid strikes are available — log the drop reason, do not crash

### Gold — "Presentation Ready"
- Consume silver only — no raw data access
- Compute Δatm_iv and Δskew: (T0 − T-1) and (T+1 − T0) for each trading day
- Run paired t-test and Wilcoxon signed-rank test on each delta series
- Compute Cohen's d for effect size
- Output `summary_stats.csv` with: feature, mean shift, std, t-stat, p-value, Cohen's d
- Generate and save two boxplots: ATM IV at T-1/T0/T+1 and 25Δ skew at T-1/T0/T+1

## Success Criteria

| Metric | Threshold |
|---|---|
| ATM IV shift p-value | < 0.05 |
| Effect size (Cohen's d) | > 0.3 |
| 25Δ skew shift consistency | > 55% of days move in same direction |
| Sample size | > 60 valid trading days |
| IV range sanity check | All ATM IVs between 20% and 500% annualized |

A negative result (no significant shift) is a valid and acceptable outcome if the methodology is sound. Document it honestly.

## Pipeline Entrypoint

`run_pipeline.py` is the single command to reproduce all results:

```
python run_pipeline.py
```

Runs bronze → silver → gold in sequence. Each stage logs progress and row counts. The pipeline should be runnable by anyone with a Kalshi API key and internet access.

## Key Implementation Notes

- All timestamps must be UTC throughout — no local time anywhere in the codebase
- Kalshi contract prices are in cents (0–100); normalize to (0–1) before IV inversion
- Kalshi's live endpoints cover the last ~3 months; anything older requires the `/historical/` endpoints — the client must route correctly based on the cutoff returned by `GET /historical/cutoff`
- Binance returns up to 1000 bars per API call — paginate accordingly to cover the full date range
- Kalshi fees are ~7 cents per contract — relevant if a backtest is added later
- BTC daily markets settle at 4pm ET (~21:00 UTC) — expiry time T must reflect this in the IV calculation
