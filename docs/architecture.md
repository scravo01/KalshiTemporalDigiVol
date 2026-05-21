# Architecture

## Purpose

End-to-end research pipeline that tests whether ATM implied volatility and 25-delta skew on Kalshi BTC binary markets shift around the Asian market open (00:00 UTC). Follows a bronze/silver/gold medallion pattern where each layer may only read from the layer directly below it.

## File Location

Top-level layout; no single source file — see `src/` for all modules.

## Data Flow

```
                  ┌──────────────────────────────────────────────────┐
                  │                  ENTRY POINTS                    │
                  │            uv run kvol pipeline                   │
                  └───────────────────┬──────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                   ▼
         ┌──────────────┐  ┌──────────────────┐  (silver after bronze)
         │ BinanceBronze│  │  KalshiBronze     │
         │    ETL       │  │     ETL           │
         └──────┬───────┘  └────────┬──────────┘
                │                   │
                ▼                   ▼
   data/bronze/binance_btc_1m.parquet
   data/bronze/kalshi_markets_<date>.parquet
   data/bronze/kalshi_candles_<date>.parquet
                        │
                        ▼
               ┌─────────────────┐
               │    SilverETL    │  (DuckDB join + IV inversion)
               └────────┬────────┘
                        │
                        ▼
           data/silver/contracts.parquet
                        │
                        ▼
               ┌─────────────────┐
               │   gold_etl.run  │  (stats + plots)
               └────────┬────────┘
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
  data/gold/summary_stats.csv   data/gold/plots/
```

## Layer Contracts

### Bronze — "Raw but Reliable"
- Writes faithful API responses as Polars DataFrames to zstd-compressed parquet.
- Normalizes column names and dtypes only; adds `ingested_at` UTC timestamp per row.
- Zero-volume Kalshi candle rows are dropped at ingest (they carry no price information).
- **Input**: API calls via `KalshiClient` and `BinanceClient`.
- **Reads from**: nothing (APIs only).
- **Writes to**: `data/bronze/`.

### Silver — "Analysis Ready"
- Joins Kalshi candles to Binance klines using DuckDB (`SET TimeZone='UTC'` enforced).
- Assigns snapshot label per expiry UTC hour (T-11 through T+12, with T0 = expiry at 01:00 UTC = Asian open window open at 00:00 UTC).
- Inverts digital Black-Scholes to compute `implied_vol` per contract row.
- Drops rows with null IV or volume = 0; drops `(trade_date, snapshot)` groups with fewer than 3 valid strikes.
- **Reads from**: `data/bronze/` only.
- **Writes to**: `data/silver/contracts.parquet`.

### Gold — "Presentation Ready"
- Computes ATM IV and 25-delta skew per `(trade_date, snapshot)` pair.
- Runs paired t-test and Wilcoxon signed-rank test on consecutive snapshot shifts.
- Computes Cohen's d for effect size.
- **Reads from**: `data/silver/` only.
- **Writes to**: `data/gold/summary_stats.csv` and `data/gold/plots/`.

## Module Map

```
src/
├── cli/
│   └── main.py              Click CLI; entry point for all pipeline stages
├── clients/
│   ├── kalshi_client.py     Async Kalshi API wrapper (RSA-PSS auth, rate-limited)
│   └── binance_client.py    Async Binance API wrapper (api.binance.us)
├── etl/
│   ├── base.py              Abstract BaseETL (extract / transform / load)
│   ├── bronze/
│   │   ├── kalshi_bronze.py  KalshiBronzeETL
│   │   └── binance_bronze.py BinanceBronzeETL
│   ├── silver/
│   │   ├── silver_etl.py    SilverETL (DuckDB join, snapshot labelling, IV)
│   │   ├── vol_surface_etl.py VolSurfaceETL (all bars in window, not first-bar only)
│   │   └── implied_vol.py   invert_iv() closed-form + brentq fallback
│   └── gold/
│       └── gold_etl.py      run() — features, stats, plots
```

## Key Technology Choices

| Component | Technology | Reason |
|-----------|-----------|--------|
| DataFrames | Polars | All ETL layers; columnar, zero-copy, no pandas |
| SQL joins | DuckDB | Silver layer only; handles cross-parquet glob scans with UTC timezone enforcement |
| Async I/O | aiohttp | Both API clients; concurrent candle batch fetches |
| Rate limiting | aiolimiter | Shared `AsyncLimiter` across all Kalshi requests |
| IV inversion | scipy (brentq + norm) | ATM fallback and normal CDF/PPF |
| CLI | Click | `kvol` entrypoint; validates date params |
| Compression | zstd level 3 | All parquet writes; good ratio/speed tradeoff |

## Snapshot Labelling

All 24 UTC settlement hours are assigned a snapshot label relative to T0 (Asian open, 00:00 UTC):

| Expiry UTC hour | Snapshot label | Interpretation |
|----------------|----------------|----------------|
| 01:00 | T0 | Window opened at 00:00 UTC (Asian open) |
| 00:00 | T-1 | Window opened 23:00 UTC prior day |
| 02:00 | T+1 | Window opened 01:00 UTC |
| 03:00–13:00 | T+2 through T+12 | Post-Asian-open hours |
| 23:00–14:00 | T-2 through T-11 | Pre-Asian-open hours |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KALSHI_API_KEY` | Yes (bronze-kalshi, pipeline) | RSA-2048 PEM private key |
| `KEY_ID` | Yes | Kalshi key UUID (used in `KALSHI-ACCESS-KEY` header) |

Both are loaded from `.env` via `python-dotenv` in `cli/main.py:main()`.

## Known Limitations

- `vol_surface_etl.py` (`VolSurfaceETL`) is not wired into the CLI or `pipeline` command — it must be invoked manually.
- Gold ETL hardcodes `IV_MIN = 0.20` and `IV_MAX = 5.00` as IV sanity bounds.
