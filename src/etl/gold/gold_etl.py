import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns
from scipy import stats

logger = logging.getLogger(__name__)

SILVER_PATH = Path("data/silver/contracts.parquet")
GOLD_DIR = Path("data/gold")
PLOTS_DIR = GOLD_DIR / "plots"
STATS_PATH = GOLD_DIR / "summary_stats.csv"

IV_MIN = 0.20
IV_MAX = 5.00
_SNAPSHOT_ORDER = ["T-3", "T-2", "T-1", "T0", "T+1", "T+2"]


def run() -> None:
    df = _load_silver()
    features_df = _compute_features(df)
    stats_df = _compute_stats(features_df)
    _save_stats(stats_df)
    _save_plots(features_df)
    logger.info("Gold ETL complete")


def _load_silver() -> pl.DataFrame:
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Silver file missing: {SILVER_PATH}")
    df = pl.read_parquet(SILVER_PATH)
    n_before = len(df)
    df = df.filter((pl.col("implied_vol") >= IV_MIN) & (pl.col("implied_vol") <= IV_MAX))
    logger.info(f"Loaded {len(df)} silver rows after IV sanity filter ({n_before - len(df)} dropped)")
    return df


def _compute_features(df: pl.DataFrame) -> pl.DataFrame:
    # ATM: row with delta closest to 0.5 per (trade_date, snapshot)
    atm_df = (
        df.with_columns((pl.col("delta") - 0.5).abs().alias("_dist_atm"))
        .sort(["trade_date", "snapshot", "_dist_atm"])
        .group_by(["trade_date", "snapshot"], maintain_order=True)
        .first()
        .select([
            "trade_date", "snapshot", "btc_close",
            pl.col("strike").alias("atm_strike"),
            pl.col("implied_vol").alias("atm_iv"),
        ])
    )

    # +25Δ: delta closest to 0.75
    plus25_df = (
        df.with_columns((pl.col("delta") - 0.75).abs().alias("_dist"))
        .sort(["trade_date", "snapshot", "_dist"])
        .group_by(["trade_date", "snapshot"], maintain_order=True)
        .first()
        .select(["trade_date", "snapshot", pl.col("implied_vol").alias("iv_plus25")])
    )

    # −25Δ: delta closest to 0.25
    minus25_df = (
        df.with_columns((pl.col("delta") - 0.25).abs().alias("_dist"))
        .sort(["trade_date", "snapshot", "_dist"])
        .group_by(["trade_date", "snapshot"], maintain_order=True)
        .first()
        .select(["trade_date", "snapshot", pl.col("implied_vol").alias("iv_minus25")])
    )

    n_strikes = (
        df.group_by(["trade_date", "snapshot"])
        .agg(pl.len().alias("n_valid_strikes"))
    )

    features_df = (
        atm_df
        .join(plus25_df, on=["trade_date", "snapshot"])
        .join(minus25_df, on=["trade_date", "snapshot"])
        .join(n_strikes, on=["trade_date", "snapshot"])
        .with_columns(
            ((pl.col("iv_plus25") - pl.col("iv_minus25")) / pl.col("atm_iv"))
            .alias("skew_25d")
        )
        .select([
            "trade_date", "snapshot", "atm_iv", "skew_25d",
            "atm_strike", "n_valid_strikes", "btc_close",
        ])
        .sort(["trade_date", pl.col("snapshot").cast(pl.Utf8)])
    )
    logger.info(f"Computed features for {len(features_df)} (trade_date, snapshot) pairs")
    return features_df


def _compute_stats(features_df: pl.DataFrame) -> pl.DataFrame:
    results = []

    for feature in ("atm_iv", "skew_25d"):
        wide = features_df.pivot(index="trade_date", on="snapshot", values=feature)
        present = [s for s in _SNAPSHOT_ORDER if s in wide.columns]

        consecutive_pairs = [
            (a, b) for a, b in zip(present, present[1:])
        ]
        for a, b in consecutive_pairs:
            # Use only dates where both snapshots are non-null
            pair = wide.select(["trade_date", a, b]).drop_nulls()
            if len(pair) < 5:
                logger.warning(
                    f"Only {len(pair)} days for {feature} {b}-{a} — skipping"
                )
                continue
            delta = (pair[b] - pair[a]).to_numpy()
            results.append(_run_tests(delta, feature, f"{b}-{a}"))

    return pl.DataFrame(results)


def _run_tests(delta: np.ndarray, feature: str, shift: str) -> dict:
    t_stat, p_value = stats.ttest_1samp(delta, 0.0)
    nonzero = delta[delta != 0]
    if len(nonzero) < 2:
        p_wilcox = float("nan")
    else:
        _, p_wilcox = stats.wilcoxon(nonzero, alternative="two-sided")
    mean_shift = float(np.mean(delta))
    std = float(np.std(delta, ddof=1))
    cohens_d = mean_shift / std if std > 0 else float("nan")
    return {
        "feature": feature,
        "shift": shift,
        "mean_shift": round(mean_shift, 6),
        "std": round(std, 6),
        "t_stat": round(float(t_stat), 4),
        "p_value": round(float(p_value), 6),
        "p_wilcoxon": round(float(p_wilcox), 6),
        "cohens_d": round(cohens_d, 4),
        "n_days": len(delta),
    }


def _save_stats(stats_df: pl.DataFrame) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    stats_df.write_csv(STATS_PATH)
    logger.info(f"Saved summary stats → {STATS_PATH}")


def _save_plots(features_df: pl.DataFrame) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    data = features_df.to_pandas()

    for col, title, ylabel in [
        ("atm_iv", "ATM Implied Vol at Three Snapshots", "ATM IV (annualised)"),
        ("skew_25d", "25Δ Skew at Three Snapshots", "Skew (IV+25Δ − IV−25Δ) / ATM IV"),
    ]:
        fig, ax = plt.subplots(figsize=(11, 5))
        sns.boxplot(data=data, x="snapshot", y=col, order=_SNAPSHOT_ORDER, ax=ax)
        ax.set_title(title)
        ax.set_xlabel("Snapshot")
        ax.set_ylabel(ylabel)
        fig.tight_layout()
        out = PLOTS_DIR / f"{col}_boxplot.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        logger.info(f"Saved plot → {out}")
