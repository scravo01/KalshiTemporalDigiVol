# Streamlit UI

## Purpose

Interactive dashboard for exploring the silver-layer contract data and gold-layer statistical results. Provides time-series charts, boxplot distributions, IV smile cross-sections, and the hypothesis test output table — all driven by sidebar filters.

## File Location

`src/ui/app.py`

## How to Launch

```bash
# From the repo root, after running the pipeline at least through silver
uv run streamlit run src/ui/app.py
```

The app reads from `data/silver/contracts.parquet` and `data/gold/summary_stats.csv`. If the silver file does not exist, the app displays a warning and stops.

## Path Constants

| Constant | Path | Notes |
|----------|------|-------|
| `_SILVER_PATH` | `data/silver/contracts.parquet` | Required; app halts if missing |
| `_STATS_PATH` | `data/gold/summary_stats.csv` | Optional; section shows info message if missing |
| `_SNAPSHOT_ORDER` | `["T-1", "T0", "T+1"]` | **Hardcoded to three snapshots**; the silver layer contains all 24 but the UI only shows these three in ordered contexts |

## Cached Data Loaders

All three loaders are decorated with `@st.cache_data` — they run once per session and are not re-evaluated on widget interaction.

| Function | Source | Returns |
|----------|--------|---------|
| `load_contracts()` | `_SILVER_PATH` | Raw silver parquet as `pl.DataFrame` |
| `load_features()` | Calls `gold_etl._compute_features(_load_silver())` | Features DataFrame (atm_iv, skew_25d per date+snapshot) |
| `load_stats()` | `_STATS_PATH` | Summary stats CSV as `pl.DataFrame` |

`load_features()` catches all exceptions and returns an empty DataFrame — the app degrades gracefully if gold code raises.

## Sidebar Controls

| Control | Type | Description |
|---------|------|-------------|
| Date range | `st.date_input` | Filters `trade_date` across all sections except the stats table |
| Snapshots | `st.multiselect` | Filters to selected snapshot labels (default: all three) |
| IV range | `st.slider` | Filters `implied_vol` in the raw contracts table only (20–500% annualised) |
| Show raw contracts table | `st.checkbox` | Toggles Section 6 |

Filters are applied to `feat_filtered` (the features DataFrame). The raw `contracts` DataFrame is filtered separately in Section 5 and 6.

## Sections

### Section 1: Statistical Test Results

Displays `summary_stats.csv` as a full-width dataframe table (`st.dataframe`). Shows an info message if the stats file is missing. No filtering — always shows the full test results.

### Section 2: ATM Implied Vol Over Time

Line chart of `atm_iv` pivoted wide by snapshot, indexed by `trade_date`. One line per snapshot label present in `feat_filtered`. Uses `st.line_chart` (Altair-backed). Shows an info message if no data matches the current filters.

### Section 3: 25Δ Skew Over Time

Same structure as Section 2 but for `skew_25d`.

### Section 4: Distribution by Snapshot

Two-column layout:
- Left column: `sns.boxplot` of `atm_iv` vs. snapshot (order: `_SNAPSHOT_ORDER`).
- Right column: `sns.boxplot` of `skew_25d` vs. snapshot.

Renders via `st.pyplot`. Both plots use the date-and-snapshot-filtered `feat_filtered`.

### Section 5: IV Smile Cross-Section

Renders the full IV smile (IV vs. strike) for a single selected `(trade_date, snapshot)` pair. Controlled by two widgets:
- `st.date_input("Trade date")` — defaults to the latest date in the filtered range.
- `st.selectbox("Snapshot")` — `_SNAPSHOT_ORDER` options.

Data source: raw `contracts` DataFrame (not `feat_filtered`). Plots each strike's IV with delta annotated as text above each point (font size 7, grey). Shows an info message if no data exists for the selection.

### Section 6: Raw Contracts Table (optional)

Shown only when "Show raw contracts table" is checked. Filters `contracts` by date range and IV range from the sidebar slider. Displays row count below the table.

## Assumptions & Gotchas

- `_SNAPSHOT_ORDER` is hardcoded to `["T-1", "T0", "T+1"]`. The silver layer now contains all 24 snapshot labels. Sections 2, 3, and 4 will silently omit all other snapshots from ordered display (though they may appear unordered in `feat_filtered` if the multiselect is modified).
- `load_features()` recomputes gold-layer features live (calls `_compute_features` and `_load_silver`). This applies the gold-layer `IV_MIN`/`IV_MAX` sanity filter — the feature data may differ from the raw contracts shown in Section 6, which has no such filter applied at load time.
- The Streamlit app must be launched from the repo root (`/Users/samuelcravo/Documents/GitHub/KalshiTemporalDigiVol`) so that relative paths `data/silver/...` resolve correctly. Running from a different directory will cause `FileNotFoundError`.
- There is no authentication on the Streamlit app — it is intended for local research use only.
- Delta annotations in Section 5 use `smile_data["delta"]` directly. In the silver schema, `delta = digi_px / 100`, which is the risk-neutral probability, not the traditional option delta.
