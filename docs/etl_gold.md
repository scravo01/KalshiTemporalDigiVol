# Gold ETL / Analysis

## Purpose

Consumes the silver-layer `contracts.parquet`, computes ATM IV and 25-delta skew per `(trade_date, snapshot)` pair, runs statistical tests on consecutive snapshot shifts, and writes a summary CSV and boxplot PNGs. This is the terminal layer — it reads silver only and produces no parquet files.

## File Location

`src/etl/gold/gold_etl.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `polars` | Loading silver parquet and feature computation |
| `scipy.stats` | `ttest_1samp`, `wilcoxon` |
| `numpy` | Array operations in stat tests |
| `matplotlib`, `seaborn` | Boxplot generation |
| `scipy.interpolate.griddata` | Imported for vol surface plots (3D) |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `SILVER_PATH` | `data/silver/contracts.parquet` | Input |
| `SURFACE_PATH` | `data/silver/vol_surface.parquet` | Optional input for surface plots |
| `GOLD_DIR` | `data/gold` | Output root |
| `PLOTS_DIR` | `data/gold/plots` | PNG output directory |
| `STATS_PATH` | `data/gold/summary_stats.csv` | Statistical test results |
| `IV_MIN` | `0.20` | IV sanity filter lower bound (20% annualised) |
| `IV_MAX` | `5.00` | IV sanity filter upper bound (500% annualised) |
| `_SNAPSHOT_ORDER` | `["T-11", ..., "T-1", "T0", "T+1", ..., "T+12"]` | Ordered list of all 24 snapshot labels for plot axes |

## Entry Point

```python
from src.etl.gold.gold_etl import run
run()
```

Called by `kvol pipeline` and directly by `kvol silver` → gold step. Not a `BaseETL` subclass — synchronous function, no `asyncio`.

## Functions

### `run() -> None`

Orchestrates the full gold pipeline:

1. `_load_silver()` — load and sanity-filter silver parquet.
2. `_compute_features()` — derive ATM IV, 25Δ skew, and strike counts per `(trade_date, snapshot)`.
3. `_compute_stats()` — run statistical tests across consecutive snapshot pairs.
4. `_save_stats()` — write `summary_stats.csv`.
5. `_save_plots()` — write boxplot PNGs.
6. Optionally `_save_vol_surface_plots()` if `data/silver/vol_surface.parquet` exists.

### `_load_silver() -> pl.DataFrame`

Reads `SILVER_PATH`. Filters rows where `IV_MIN <= implied_vol <= IV_MAX`. Logs count dropped. Raises `FileNotFoundError` if file is absent.

### `_compute_features(df) -> pl.DataFrame`

Produces one row per `(trade_date, snapshot)` with:

| Output column | Derivation |
|--------------|-----------|
| `atm_iv` | `implied_vol` of the row with `delta` closest to `0.5` per group |
| `atm_strike` | Corresponding strike |
| `btc_close` | BTC spot at that row |
| `iv_plus25` | `implied_vol` of row with `delta` closest to `0.75` |
| `iv_minus25` | `implied_vol` of row with `delta` closest to `0.25` |
| `n_valid_strikes` | Count of rows per group |
| `skew_25d` | `(iv_plus25 - iv_minus25) / atm_iv` |

Delta targeting uses nearest-neighbour on the `delta` column (`digi_px / 100`). Since Kalshi strikes are discrete, this is an approximation — delta may not be exactly 0.25/0.50/0.75.

**Note on delta vs. traditional delta**: In this codebase, `delta = digi_px / 100` (the digital option price as a probability). This is not the same as the Black-Scholes delta of the digital (which would be the PDF term). It is the risk-neutral probability `N(d2)`, which is how this codebase defines "25-delta": `N(d2) ≈ 0.25`.

### `_compute_stats(features_df) -> pl.DataFrame`

For each of `atm_iv` and `skew_25d`:
1. Pivots the features DataFrame wide by `snapshot` (columns = snapshot labels, index = `trade_date`).
2. Builds a list of consecutive snapshot pairs from `_SNAPSHOT_ORDER` that are present in the data.
3. For each pair `(a, b)`: computes `delta = pair[b] - pair[a]`, drops dates where either column is null, and calls `_run_tests`.
4. Skips pairs with fewer than 5 valid days (logs a WARNING).

### `_run_tests(delta, feature, shift) -> dict`

Runs two tests on the daily shift array:

| Test | Function | Description |
|------|----------|-------------|
| Paired t-test | `scipy.stats.ttest_1samp(delta, 0.0)` | Tests whether mean shift is zero |
| Wilcoxon signed-rank | `scipy.stats.wilcoxon(nonzero, alternative="two-sided")` | Non-parametric; skips zero-shift days |

Also computes:
- `mean_shift`: arithmetic mean of `delta`
- `std`: sample standard deviation (`ddof=1`)
- `cohens_d`: `mean_shift / std` (effect size; `nan` if `std == 0`)
- `n_days`: sample size

Returns a `dict` for assembly into the stats DataFrame.

## Output Files

### `data/gold/summary_stats.csv`

| Column | Type | Notes |
|--------|------|-------|
| `feature` | str | `atm_iv` or `skew_25d` |
| `shift` | str | e.g. `T0-T-1`, `T+1-T0` |
| `mean_shift` | float | Rounded to 6 decimal places |
| `std` | float | Sample std, rounded to 6 decimal places |
| `t_stat` | float | Rounded to 4 decimal places |
| `p_value` | float | Rounded to 6 decimal places |
| `p_wilcoxon` | float | Rounded to 6 decimal places |
| `cohens_d` | float | Rounded to 4 decimal places |
| `n_days` | int | Valid trading days in the paired sample |

Plain CSV (not parquet) — small enough that the overhead of parquet is unnecessary.

### `data/gold/plots/atm_iv_boxplot.png`

Seaborn boxplot of `atm_iv` across all snapshot labels in `_SNAPSHOT_ORDER`. X-axis: snapshot; Y-axis: ATM IV (annualised). 150 DPI, 11×5 inches.

### `data/gold/plots/skew_25d_boxplot.png`

Same structure for `skew_25d`. Y-axis label: "Skew (IV+25Δ − IV−25Δ) / ATM IV".

## Hypothesis Test Interpretation

The research question is whether ATM IV or 25Δ skew shifts at the Asian open (00:00 UTC). The relevant shifts are:

- `T0-T-1`: change from the hour before Asian open to the Asian open hour
- `T+1-T0`: change in the first hour after Asian open

**Success criteria**:
- ATM IV shift p-value < 0.05
- Effect size (Cohen's d) > 0.3
- 25Δ skew shift consistency: > 55% of days move in same direction

As of 2026-05-19, the 25Δ skew shift has a borderline p-value of ~0.083.

## Known Limitations

- `_SNAPSHOT_ORDER` covers all 24 labels but the UI's `_SNAPSHOT_ORDER` is still hardcoded to `["T-1", "T0", "T+1"]`. The gold ETL correctly handles all 24 snapshot labels; only the Streamlit UI shows three.
- `_save_vol_surface_plots()` is imported via `mpl_toolkits.mplot3d` but the function body and 3D rendering logic are not fully implemented in the current source (the `_save_vol_surface_plots` function reference in `run()` is conditional on `SURFACE_PATH.exists()`). The function is not present in the visible source — if it is absent, this call will raise `AttributeError`.
- The consecutive-pair approach means each snapshot shift is tested independently. No multiple-testing correction (e.g. Bonferroni) is applied across the 23 pairs × 2 features = 46 tests.
- `pd.DataFrame` conversion happens inside `_save_plots` (via `features_df.to_pandas()`). This is the only point in the codebase where pandas is used.
