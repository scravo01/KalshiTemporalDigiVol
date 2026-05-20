import logging
from pathlib import Path

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

BRONZE_PATH        = Path("data/bronze/binance_btc_1m.parquet")
SURFACE_PATH       = Path("data/silver/vol_surface.parquet")
GOLD_DIR           = Path("data/gold")
RV_IV_PATH         = GOLD_DIR / "rv_iv.parquet"
RV_IV_HOURLY_PATH  = GOLD_DIR / "rv_iv_hourly.parquet"

IV_MIN        = 0.20
IV_MAX        = 5.00
_ANNUALIZE_5M = 105_120  # 365 * 24 * 60 / 5  (crypto 24/7)
_ANNUALIZE_1H =   8_760  # 365 * 24


def run() -> None:
    binance_df     = _load_binance()
    vol_surface_df = _load_vol_surface()

    # 5-min dataset (legacy)
    rv_df  = _compute_rv_5min(binance_df)
    iv_df  = _compute_atm_iv_5min(vol_surface_df)
    df     = _join_rv_iv(rv_df, iv_df)
    _print_summary(df, label="5-min")
    _save_data(df, RV_IV_PATH)

    # Hourly dataset — IV and RV both at 1-hour resolution
    rv_h   = _compute_rv_hourly(binance_df)
    iv_h   = _compute_atm_iv_hourly(vol_surface_df)
    df_h   = _join_rv_iv(rv_h, iv_h)
    _print_summary(df_h, label="hourly")
    _save_data(df_h, RV_IV_HOURLY_PATH)

    logger.info("RV-IV analysis complete — wrote %s and %s", RV_IV_PATH, RV_IV_HOURLY_PATH)


# ── data loaders ──────────────────────────────────────────────────────────────

def _load_binance() -> pl.DataFrame:
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(f"Bronze file missing: {BRONZE_PATH}")
    df = pl.read_parquet(BRONZE_PATH)
    logger.info("Loaded %d Binance 1m bars", len(df))
    return df.sort("timestamp")


def _load_vol_surface() -> pl.DataFrame:
    if not SURFACE_PATH.exists():
        raise FileNotFoundError(f"Silver vol surface missing: {SURFACE_PATH}")
    df = pl.read_parquet(SURFACE_PATH)
    n_before = len(df)
    df = df.filter(
        (pl.col("implied_vol") >= IV_MIN) & (pl.col("implied_vol") <= IV_MAX)
    )
    logger.info(
        "Loaded %d vol surface rows after IV filter (%d dropped)",
        len(df), n_before - len(df),
    )
    return df


# ── core computations ─────────────────────────────────────────────────────────

def _compute_rv_5min(binance_df: pl.DataFrame) -> pl.DataFrame:
    return (
        binance_df
        .with_columns(
            (pl.col("close").cast(pl.Float64) / pl.col("close").cast(pl.Float64).shift(1))
            .log()
            .alias("log_ret")
        )
        .drop_nulls("log_ret")
        .with_columns(pl.col("timestamp").dt.truncate("5m").alias("bucket"))
        .group_by("bucket")
        .agg([
            (pl.col("log_ret").pow(2).sum() * _ANNUALIZE_5M).sqrt().alias("rv_ann"),
            pl.col("close").last().alias("spot"),
        ])
        .sort("bucket")
    )


def _compute_rv_hourly(binance_df: pl.DataFrame) -> pl.DataFrame:
    return (
        binance_df
        .with_columns(
            (pl.col("close").cast(pl.Float64) / pl.col("close").cast(pl.Float64).shift(1))
            .log()
            .alias("log_ret")
        )
        .drop_nulls("log_ret")
        .with_columns(pl.col("timestamp").dt.truncate("1h").alias("bucket"))
        .group_by("bucket")
        .agg([
            (pl.col("log_ret").pow(2).sum() * _ANNUALIZE_1H).sqrt().alias("rv_ann"),
            pl.col("close").last().alias("spot"),
        ])
        .sort("bucket")
    )


def _compute_atm_iv_5min(vol_surface_df: pl.DataFrame) -> pl.DataFrame:
    atm_per_expiry = (
        vol_surface_df
        .with_columns((pl.col("prob_itm") - 0.5).abs().alias("_dist_atm"))
        .sort(["bar_ts", "expiry_time", "minutes_to_expiry", "_dist_atm"])
        .group_by(["bar_ts", "expiry_time"], maintain_order=True)
        .first()
        .select(["bar_ts", "expiry_time", "implied_vol", "minutes_to_expiry"])
    )

    atm_front = (
        atm_per_expiry
        .filter(pl.col("minutes_to_expiry") > 0)
        .sort(["bar_ts", "minutes_to_expiry"])
        .group_by("bar_ts", maintain_order=True)
        .first()
        .select(["bar_ts", "implied_vol", "minutes_to_expiry"])
    )

    return (
        atm_front
        .with_columns(pl.col("bar_ts").dt.truncate("5m").alias("bucket"))
        .group_by("bucket")
        .agg([
            pl.col("implied_vol").mean().alias("atm_iv_mean"),
            pl.col("implied_vol").last().alias("atm_iv_last"),
            pl.col("minutes_to_expiry").mean().alias("minutes_to_expiry_mean"),
        ])
        .sort("bucket")
    )


def _compute_atm_iv_hourly(vol_surface_df: pl.DataFrame) -> pl.DataFrame:
    atm_per_expiry = (
        vol_surface_df
        .with_columns((pl.col("prob_itm") - 0.5).abs().alias("_dist_atm"))
        .sort(["bar_ts", "expiry_time", "minutes_to_expiry", "_dist_atm"])
        .group_by(["bar_ts", "expiry_time"], maintain_order=True)
        .first()
        .select(["bar_ts", "expiry_time", "implied_vol", "minutes_to_expiry"])
    )

    atm_front = (
        atm_per_expiry
        .filter(pl.col("minutes_to_expiry") > 0)
        .sort(["bar_ts", "minutes_to_expiry"])
        .group_by("bar_ts", maintain_order=True)
        .first()
        .select(["bar_ts", "implied_vol", "minutes_to_expiry"])
    )

    return (
        atm_front
        .with_columns(pl.col("bar_ts").dt.truncate("1h").alias("bucket"))
        .group_by("bucket")
        .agg([
            pl.col("implied_vol").mean().alias("atm_iv_mean"),
            pl.col("implied_vol").last().alias("atm_iv_last"),
            pl.col("minutes_to_expiry").mean().alias("minutes_to_expiry_mean"),
        ])
        .sort("bucket")
    )


def _join_rv_iv(rv_df: pl.DataFrame, iv_df: pl.DataFrame) -> pl.DataFrame:
    joined = (
        rv_df.join(iv_df, on="bucket", how="inner")
        .with_columns([
            (pl.col("rv_ann") - pl.col("atm_iv_mean")).alias("vol_premium"),
            (pl.col("rv_ann").pow(2) - pl.col("atm_iv_mean").pow(2)).alias("var_premium"),
            pl.col("bucket").dt.hour().cast(pl.Int8).alias("hour_utc"),
            pl.col("bucket").dt.date().alias("date"),
        ])
        .sort("bucket")
    )
    logger.info(
        "Joined RV+IV: %d observations, %s → %s",
        len(joined), joined["bucket"].min(), joined["bucket"].max(),
    )
    return joined


# ── summary ───────────────────────────────────────────────────────────────────

def _print_summary(df: pl.DataFrame, label: str = "") -> None:
    prem   = df["vol_premium"].to_numpy()
    mean_p = float(np.mean(prem))
    std_p  = float(np.std(prem, ddof=1))
    sharpe = mean_p / std_p if std_p > 0 else float("nan")
    tag = f"[{label}] " if label else ""
    logger.info("=== %sRV-IV Analysis Summary ===", tag)
    logger.info("Observations:         %d", len(df))
    logger.info("Mean vol premium:     %.4f  (RV − ATM IV, annualised)", mean_p)
    logger.info("Std vol premium:      %.4f", std_p)
    logger.info("%% obs with RV > IV:  %.1f%%", float(np.mean(prem > 0)) * 100)
    logger.info("Sharpe of premium:    %.3f", sharpe)
    logger.info("Mean RV (ann.):       %.4f", float(df["rv_ann"].mean()))
    logger.info("Mean ATM IV (ann.):   %.4f", float(df["atm_iv_mean"].mean()))


# ── data output ───────────────────────────────────────────────────────────────

def _save_data(df: pl.DataFrame, path: Path) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3)
    logger.info("Saved %d RV-IV rows → %s", len(df), path)
