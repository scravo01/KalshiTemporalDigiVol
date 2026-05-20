# ETL: RV vs IV Analysis (`rv_iv_analysis.py`)

Gold-layer module that quantifies the vol premium between realized volatility and ATM implied volatility at both 5-minute and hourly resolutions. This is the analytical foundation for the vol premium findings in the research notebooks.

## CLI Command

```bash
uv run kvol rv-iv
```

Runs automatically as the `rv_iv` task in the Airflow DAG (after `vol_surface` completes). Can also be run standalone after `vol-surface` has produced `data/silver/vol_surface.parquet`.

## Inputs

| File | Source | Required |
|------|--------|----------|
| `data/bronze/binance_btc_1m.parquet` | BinanceBronzeETL | Yes |
| `data/silver/vol_surface.parquet` | VolSurfaceETL | Yes |

The vol surface is filtered to IV ∈ [20%, 500%] annualized on load. Rows outside this range are logged and dropped.

## Outputs

| File | Resolution | Rows (61-day sample) |
|------|-----------|----------------------|
| `data/gold/rv_iv.parquet` | 5-minute buckets | ~17,500 |
| `data/gold/rv_iv_hourly.parquet` | 1-hour buckets | ~1,460 |

Both files use zstd level 3 compression.

## Output Schema

Both parquets share the same schema:

| Column | Type | Description |
|--------|------|-------------|
| `bucket` | `Datetime(us, UTC)` | Period start timestamp (truncated to 5m or 1h) |
| `rv_ann` | `Float64` | Annualized realized vol (log-return method) |
| `spot` | `Float32` | BTC spot price at end of period |
| `atm_iv_mean` | `Float32` | Mean ATM IV across contracts active during the period |
| `atm_iv_last` | `Float32` | ATM IV at the last observation in the period |
| `minutes_to_expiry_mean` | `Float32` | Mean time-to-expiry of the ATM contract used |
| `vol_premium` | `Float64` | `rv_ann − atm_iv_mean` (positive = RV > IV) |
| `var_premium` | `Float64` | `rv_ann² − atm_iv_mean²` |
| `hour_utc` | `Int8` | UTC hour of the bucket (0–23) |
| `date` | `Date` | Calendar date of the bucket |

## Methodology

### Realized Volatility

Log returns are computed from consecutive 1-minute Binance BTCUSDT close prices:

```
r_t = log(close_t / close_{t-1})
```

Squared log returns are summed within each bucket and annualized:

```
RV_bucket = sqrt( sum(r_t²) × annualization_factor )
```

Annualization factors (crypto trades 24/7):
- 5-minute: `365 × 24 × 60 / 5 = 105,120`
- Hourly: `365 × 24 = 8,760`

### ATM Implied Volatility

ATM IV is selected from `vol_surface.parquet` as the strike with `|prob_itm − 0.50|` minimized within each `(bar_ts, expiry_time)` pair. The front contract (smallest positive `minutes_to_expiry`) is used.

For 5-minute buckets, multiple ATM IV readings (one per minute bar) are aggregated by mean within each 5-minute window. For hourly buckets, the same aggregation applies at 1-hour resolution.

### Vol Premium

`vol_premium = rv_ann − atm_iv_mean`

- Positive values indicate RV > IV (option seller lost money)
- Negative values indicate IV > RV (option seller earned the premium)

A negative mean vol premium across observations indicates a persistent vol risk premium favoring option sellers.

## Summary Logging

When `rv-iv` runs it logs a summary for both resolutions:

```
=== [5-min] RV-IV Analysis Summary ===
Observations:         17,534
Mean vol premium:     -0.1615  (RV − ATM IV, annualised)
Std vol premium:       0.3821
% obs with RV > IV:   27.1%
Sharpe of premium:    -0.423
Mean RV (ann.):        0.7842
Mean ATM IV (ann.):    0.9457
```

Note: `vol_premium = RV − IV`, so a **negative** mean premium means IV > RV — consistent with a short-vol edge.

## Pipeline Position

```
vol_surface.parquet ──► rv_iv_analysis.run() ──► rv_iv.parquet
                                              └──► rv_iv_hourly.parquet
```

In the Airflow DAG the `rv_iv` task depends on `vol_surface` (not `silver`), so it runs in parallel with `gold` after the surface is computed.
