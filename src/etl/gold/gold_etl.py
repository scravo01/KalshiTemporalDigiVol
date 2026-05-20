import logging
from pathlib import Path

import numpy as np
import polars as pl
from scipy import stats

logger = logging.getLogger(__name__)

SILVER_PATH   = Path("data/silver/contracts.parquet")
SURFACE_PATH  = Path("data/silver/vol_surface.parquet")
GOLD_DIR      = Path("data/gold")
FEATURES_PATH = GOLD_DIR / "features.parquet"
STATS_PATH    = GOLD_DIR / "summary_stats.csv"

IV_MIN = 0.20
IV_MAX = 5.00
_SNAPSHOT_ORDER = (
    [f"T-{i}" for i in range(11, 0, -1)] + ["T0"] + [f"T+{i}" for i in range(1, 13)]
)
_SNAP_TO_IDX = {s: i for i, s in enumerate(_SNAPSHOT_ORDER)}


def run() -> None:
    df          = _load_silver()
    features_df = _compute_features(df)
    stats_df    = _compute_stats(features_df)
    _save_stats(stats_df)
    _save_features(features_df)
    logger.info("Gold ETL complete — features and stats written to %s", GOLD_DIR)


def _load_silver() -> pl.DataFrame:
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Silver file missing: {SILVER_PATH}")
    df = pl.read_parquet(SILVER_PATH)
    n_before = len(df)
    df = df.filter(
        (pl.col("implied_vol") >= IV_MIN) & (pl.col("implied_vol") <= IV_MAX)
    )
    logger.info(
        "Loaded %d silver rows after IV sanity filter (%d dropped)",
        len(df), n_before - len(df),
    )
    return df


def _compute_features(df: pl.DataFrame) -> pl.DataFrame:
    # Snapshot-level metadata (snapshot_ts = earliest bar; expiry_time is constant per group)
    meta_df = (
        df.group_by(["trade_date", "snapshot"])
        .agg([
            pl.col("snapshot_ts").min().alias("snapshot_ts"),
            pl.col("expiry_time").first().alias("expiry_time"),
        ])
    )

    # ATM: delta closest to 0.5
    atm_df = (
        df.with_columns((pl.col("prob_itm") - 0.5).abs().alias("_dist_atm"))
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
        df.with_columns((pl.col("prob_itm") - 0.75).abs().alias("_dist"))
        .sort(["trade_date", "snapshot", "_dist"])
        .group_by(["trade_date", "snapshot"], maintain_order=True)
        .first()
        .select(["trade_date", "snapshot", pl.col("implied_vol").alias("iv_plus25")])
    )

    # −25Δ: delta closest to 0.25
    minus25_df = (
        df.with_columns((pl.col("prob_itm") - 0.25).abs().alias("_dist"))
        .sort(["trade_date", "snapshot", "_dist"])
        .group_by(["trade_date", "snapshot"], maintain_order=True)
        .first()
        .select(["trade_date", "snapshot", pl.col("implied_vol").alias("iv_minus25")])
    )

    n_strikes = df.group_by(["trade_date", "snapshot"]).agg(
        pl.len().alias("n_valid_strikes")
    )

    features_df = (
        atm_df
        .join(plus25_df,  on=["trade_date", "snapshot"])
        .join(minus25_df, on=["trade_date", "snapshot"])
        .join(n_strikes,  on=["trade_date", "snapshot"])
        .join(meta_df,    on=["trade_date", "snapshot"])
        .with_columns([
            ((pl.col("iv_plus25") - pl.col("iv_minus25")) / pl.col("atm_iv")).alias("skew_25d"),
            pl.col("expiry_time").dt.hour().cast(pl.Int8).alias("expiry_hour"),
        ])
        .select([
            "trade_date", "snapshot", "snapshot_ts", "expiry_time", "expiry_hour",
            "atm_iv", "iv_plus25", "iv_minus25", "skew_25d",
            "atm_strike", "n_valid_strikes", "btc_close",
        ])
        .sort(["trade_date", "expiry_hour"])
    )
    logger.info(
        "Computed features for %d (trade_date, snapshot) pairs", len(features_df)
    )
    return features_df


def _compute_stats(features_df: pl.DataFrame) -> pl.DataFrame:
    results = []
    for feature in ("atm_iv", "skew_25d"):
        wide    = features_df.pivot(index="trade_date", on="snapshot", values=feature)
        present = [s for s in _SNAPSHOT_ORDER if s in wide.columns]
        for a, b in zip(present, present[1:]):
            pair = wide.select(["trade_date", a, b]).drop_nulls()
            if len(pair) < 5:
                logger.warning(
                    "Only %d days for %s %s-%s — skipping", len(pair), feature, b, a
                )
                continue
            delta = (pair[b] - pair[a]).to_numpy()
            results.append(_run_tests(delta, feature, f"{b}-{a}"))
    return pl.DataFrame(results)


def _run_tests(delta: np.ndarray, feature: str, shift: str) -> dict:
    t_stat, p_value = stats.ttest_1samp(delta, 0.0)
    nonzero = delta[delta != 0]
    p_wilcox = float("nan") if len(nonzero) < 2 else float(
        stats.wilcoxon(nonzero, alternative="two-sided")[1]
    )
    mean_shift = float(np.mean(delta))
    std        = float(np.std(delta, ddof=1))
    cohens_d   = mean_shift / std if std > 0 else float("nan")
    return {
        "feature":    feature,
        "shift":      shift,
        "mean_shift": round(mean_shift, 6),
        "std":        round(std, 6),
        "t_stat":     round(float(t_stat), 4),
        "p_value":    round(float(p_value), 6),
        "p_wilcoxon": round(float(p_wilcox), 6),
        "cohens_d":   round(cohens_d, 4),
        "n_days":     len(delta),
    }


def _save_stats(stats_df: pl.DataFrame) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    stats_df.write_csv(STATS_PATH)
    logger.info("Saved summary stats → %s", STATS_PATH)


def _save_features(features_df: pl.DataFrame) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    features_df.write_parquet(FEATURES_PATH, compression="zstd", compression_level=3)
    logger.info(
        "Saved %d feature rows → %s", len(features_df), FEATURES_PATH
    )
