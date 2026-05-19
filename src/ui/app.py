from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
import streamlit as st

# ── path constants ────────────────────────────────────────────────────────────
_SILVER_PATH = Path("data/silver/contracts.parquet")
_STATS_PATH = Path("data/gold/summary_stats.csv")
_SNAPSHOT_ORDER = ["T-1", "T0", "T+1"]


# ── cached loaders ────────────────────────────────────────────────────────────

@st.cache_data
def load_contracts() -> pl.DataFrame:
    if not _SILVER_PATH.exists():
        return pl.DataFrame()
    return pl.read_parquet(_SILVER_PATH)


@st.cache_data
def load_features() -> pl.DataFrame:
    from src.etl.gold.gold_etl import _compute_features, _load_silver
    try:
        return _compute_features(_load_silver())
    except Exception:
        return pl.DataFrame()


@st.cache_data
def load_stats() -> pl.DataFrame:
    if not _STATS_PATH.exists():
        return pl.DataFrame()
    return pl.read_csv(_STATS_PATH)


# ── app ───────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="BTC Kalshi Asia Vol Shift", layout="wide")
st.title("BTC Kalshi — Asia Open Vol Shift")

contracts = load_contracts()
features = load_features()
stats_df = load_stats()

if contracts.is_empty():
    st.warning("No silver data found. Run `python run_pipeline.py` first.")
    st.stop()

# ── sidebar controls ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    all_dates = features["trade_date"].unique().sort().to_list() if not features.is_empty() else []
    min_date = all_dates[0] if all_dates else None
    max_date = all_dates[-1] if all_dates else None

    date_range = st.date_input("Date range", value=(min_date, max_date))
    start_date = date_range[0] if len(date_range) > 0 else min_date
    end_date = date_range[1] if len(date_range) > 1 else max_date

    selected_snapshots = st.multiselect("Snapshots", _SNAPSHOT_ORDER, default=_SNAPSHOT_ORDER)
    iv_range = st.slider("IV range (% annualised)", 20, 500, (20, 500))
    show_raw = st.checkbox("Show raw contracts table")

# apply filters
feat_filtered = features
if not features.is_empty():
    feat_filtered = (
        features
        .filter(
            pl.col("trade_date").is_between(start_date, end_date)
            & pl.col("snapshot").cast(pl.Utf8).is_in(selected_snapshots)
        )
    )

# ── section 1: stats ──────────────────────────────────────────────────────────
st.header("Statistical Test Results")
if stats_df.is_empty():
    st.info("Run the full pipeline to generate stats.")
else:
    st.dataframe(stats_df.to_pandas(), use_container_width=True)

# ── section 2: ATM IV over time ───────────────────────────────────────────────
st.header("ATM Implied Vol Over Time")
if not feat_filtered.is_empty() and "atm_iv" in feat_filtered.columns:
    wide_iv = feat_filtered.pivot(index="trade_date", on="snapshot", values="atm_iv")
    cols_present = [c for c in _SNAPSHOT_ORDER if c in wide_iv.columns]
    st.line_chart(wide_iv.select(["trade_date"] + cols_present).to_pandas().set_index("trade_date"))
else:
    st.info("No ATM IV data for selected filters.")

# ── section 3: 25Δ skew over time ────────────────────────────────────────────
st.header("25Δ Skew Over Time")
if not feat_filtered.is_empty() and "skew_25d" in feat_filtered.columns:
    wide_skew = feat_filtered.pivot(index="trade_date", on="snapshot", values="skew_25d")
    cols_present = [c for c in _SNAPSHOT_ORDER if c in wide_skew.columns]
    st.line_chart(wide_skew.select(["trade_date"] + cols_present).to_pandas().set_index("trade_date"))
else:
    st.info("No skew data for selected filters.")

# ── section 4: boxplots ───────────────────────────────────────────────────────
st.header("Distribution by Snapshot")
if not feat_filtered.is_empty():
    data_pd = feat_filtered.to_pandas()
    col1, col2 = st.columns(2)

    with col1:
        fig, ax = plt.subplots(figsize=(5, 4))
        sns.boxplot(data=data_pd, x="snapshot", y="atm_iv", order=_SNAPSHOT_ORDER, ax=ax)
        ax.set_title("ATM IV")
        ax.set_xlabel("")
        ax.set_ylabel("IV (annualised)")
        fig.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

    with col2:
        fig, ax = plt.subplots(figsize=(5, 4))
        sns.boxplot(data=data_pd, x="snapshot", y="skew_25d", order=_SNAPSHOT_ORDER, ax=ax)
        ax.set_title("25Δ Skew")
        ax.set_xlabel("")
        ax.set_ylabel("Skew ratio")
        fig.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

# ── section 5: IV smile for a single date ────────────────────────────────────
st.header("IV Smile Cross-Section")
smile_col1, smile_col2 = st.columns(2)
with smile_col1:
    smile_date = st.date_input("Trade date", value=max_date, key="smile_date")
with smile_col2:
    smile_snap = st.selectbox("Snapshot", _SNAPSHOT_ORDER, key="smile_snap")

smile_data = (
    contracts
    .filter(
        (pl.col("trade_date") == smile_date)
        & (pl.col("snapshot").cast(pl.Utf8) == smile_snap)
    )
    .sort("strike")
)

if len(smile_data) > 0:
    fig, ax = plt.subplots(figsize=(8, 4))
    strikes = smile_data["strike"].to_list()
    ivs = (smile_data["implied_vol"] * 100).to_list()
    deltas = smile_data["delta"].to_list()
    ax.plot(strikes, ivs, marker="o", linewidth=1.5)
    ax.set_xlabel("Strike ($)")
    ax.set_ylabel("IV (%)")
    ax.set_title(f"IV Smile — {smile_date}  {smile_snap}")
    # annotate delta on each point
    for s, iv, d in zip(strikes, ivs, deltas):
        ax.annotate(f"{d:.2f}", (s, iv), textcoords="offset points", xytext=(0, 6),
                    fontsize=7, ha="center", color="grey")
    fig.tight_layout()
    st.pyplot(fig)
    plt.close(fig)
else:
    st.info("No data for selected date / snapshot.")

# ── section 6: raw contracts table ───────────────────────────────────────────
if show_raw:
    st.header("Raw Contracts (Silver Layer)")
    iv_lo, iv_hi = iv_range
    raw_filtered = (
        contracts
        .filter(
            pl.col("trade_date").is_between(start_date, end_date)
            & (pl.col("implied_vol") >= iv_lo / 100)
            & (pl.col("implied_vol") <= iv_hi / 100)
        )
    )
    st.dataframe(raw_filtered.to_pandas(), use_container_width=True)
    st.caption(f"{len(raw_filtered):,} rows")
