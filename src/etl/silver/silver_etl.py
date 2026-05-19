import logging
from pathlib import Path

import duckdb
import polars as pl

from src.etl.silver.implied_vol import compute_t, invert_iv

logger = logging.getLogger(__name__)

# Module-level path vars — tests override these directly
BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
SILVER_PATH = SILVER_DIR / "contracts.parquet"

_MIN_VALID_STRIKES = 3

_JOIN_SQL = """
WITH snapshot_candles AS (
    SELECT
        m.trade_date,
        m.ticker,
        m.strike,
        m.expiry_time,
        CASE
            WHEN extract('hour' FROM c.timestamp) = 23
             AND CAST(date_trunc('day', c.timestamp) AS DATE) = CAST(m.trade_date AS DATE) - INTERVAL '1 day'
            THEN 'T-1'
            WHEN extract('hour' FROM c.timestamp) = 0
             AND CAST(date_trunc('day', c.timestamp) AS DATE) = CAST(m.trade_date AS DATE)
            THEN 'T0'
            WHEN extract('hour' FROM c.timestamp) = 1
             AND CAST(date_trunc('day', c.timestamp) AS DATE) = CAST(m.trade_date AS DATE)
            THEN 'T+1'
        END AS snapshot,
        c.timestamp AS snapshot_ts,
        c.close     AS digi_px,
        c.volume
    FROM kalshi_candles c
    JOIN kalshi_markets m ON c.ticker = m.ticker
    WHERE extract('minute' FROM c.timestamp) = 0
      AND c.volume > 0
      AND (
            (extract('hour' FROM c.timestamp) = 23
             AND CAST(date_trunc('day', c.timestamp) AS DATE) = CAST(m.trade_date AS DATE) - INTERVAL '1 day')
         OR (extract('hour' FROM c.timestamp) IN (0, 1)
             AND CAST(date_trunc('day', c.timestamp) AS DATE) = CAST(m.trade_date AS DATE))
      )
)
SELECT
    sc.trade_date,
    sc.snapshot,
    sc.snapshot_ts,
    sc.ticker,
    sc.strike,
    sc.expiry_time,
    sc.digi_px,
    sc.volume,
    b.close AS btc_close
FROM snapshot_candles sc
JOIN binance_klines b ON b.timestamp = sc.snapshot_ts
WHERE sc.snapshot IS NOT NULL
ORDER BY sc.trade_date, sc.snapshot, sc.strike
"""


def run() -> None:
    logger.info("Silver ETL: starting")

    markets_path = BRONZE_DIR / "kalshi_markets.parquet"
    candles_path = BRONZE_DIR / "kalshi_candles.parquet"
    klines_path = BRONZE_DIR / "binance_btc_1m.parquet"

    for p in (markets_path, candles_path, klines_path):
        if not p.exists():
            raise FileNotFoundError(f"Bronze file missing: {p}")

    # ── DuckDB join ───────────────────────────────────────────────────────────
    conn = duckdb.connect()
    conn.execute("SET TimeZone='UTC'")
    conn.execute(f"CREATE VIEW kalshi_markets AS SELECT * FROM read_parquet('{markets_path}')")
    conn.execute(f"CREATE VIEW kalshi_candles AS SELECT * FROM read_parquet('{candles_path}')")
    conn.execute(f"CREATE VIEW binance_klines  AS SELECT * FROM read_parquet('{klines_path}')")

    raw_df: pl.DataFrame = conn.execute(_JOIN_SQL).pl()
    conn.close()

    logger.info(f"DuckDB join produced {len(raw_df)} rows")
    if raw_df.is_empty():
        logger.warning("Silver ETL produced no rows — check bronze data coverage")
        return

    # ── compute delta and implied vol ─────────────────────────────────────────
    result_df = raw_df.with_columns([
        (pl.col("digi_px").cast(pl.Float32) / 100.0).alias("delta"),
        pl.struct(["digi_px", "btc_close", "strike", "snapshot"])
        .map_elements(
            lambda s: invert_iv(
                float(s["digi_px"]),
                float(s["btc_close"]),
                float(s["strike"]),
                compute_t(s["snapshot"]),
            ),
            return_dtype=pl.Float64,
        )
        .alias("implied_vol"),
    ])

    # ── cast to final schema ──────────────────────────────────────────────────
    final_df = result_df.rename({"ticker": "digi_contract_name"}).select([
        pl.col("trade_date").cast(pl.Date),
        pl.col("snapshot").cast(pl.Categorical),
        pl.col("snapshot_ts").cast(pl.Datetime("us", "UTC")),
        pl.col("digi_contract_name").cast(pl.Categorical),
        pl.col("strike").cast(pl.UInt32),
        pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
        pl.col("digi_px").cast(pl.UInt8),
        pl.col("delta").cast(pl.Float32),
        pl.col("implied_vol").cast(pl.Float32),
        pl.col("btc_close").cast(pl.Float32),
        pl.col("volume").cast(pl.UInt32),
    ])

    # ── validate and filter ───────────────────────────────────────────────────
    n_before = len(final_df)
    valid_df = final_df.filter(pl.col("volume") > 0).drop_nulls(subset=["implied_vol"])
    n_null_iv = n_before - len(valid_df)
    if n_null_iv:
        logger.info(f"Dropped {n_null_iv} rows with null/invalid IV")

    # Drop (trade_date, snapshot) groups with fewer than 3 valid strikes
    group_counts = (
        valid_df.group_by(["trade_date", "snapshot"])
        .agg(pl.len().alias("n_valid"))
    )
    thin_groups = group_counts.filter(pl.col("n_valid") < _MIN_VALID_STRIKES)
    if len(thin_groups):
        logger.info(f"Dropping {len(thin_groups)} (trade_date, snapshot) groups with < {_MIN_VALID_STRIKES} strikes")
        sufficient = group_counts.filter(pl.col("n_valid") >= _MIN_VALID_STRIKES).select(["trade_date", "snapshot"])
        valid_df = valid_df.join(sufficient, on=["trade_date", "snapshot"], how="inner")

    # ── write ─────────────────────────────────────────────────────────────────
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    valid_df.write_parquet(SILVER_PATH, compression="zstd", compression_level=3)
    logger.info(f"Silver ETL complete: {len(valid_df)} rows → {SILVER_PATH}")
