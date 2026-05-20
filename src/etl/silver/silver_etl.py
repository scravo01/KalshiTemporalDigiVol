import logging
from pathlib import Path

import duckdb
import polars as pl
from tqdm import tqdm

from src.etl.base import BaseETL
from src.etl.silver.implied_vol import invert_iv

_SECONDS_PER_YEAR = 365.25 * 24 * 3600

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
_MIN_VALID_STRIKES = 3

_JOIN_SQL = """
WITH first_bar AS (
    -- All 24 hourly expiry windows (UTC hours 0-23).
    -- Snapshot labels are relative to T0 = window-open at 00:00 UTC (Asian open):
    --   expiry 01:00 UTC → window opens 00:00 UTC → T0
    --   expiry 02:00 UTC → T+1, ..., expiry 13:00 UTC → T+12
    --   expiry 00:00 UTC → T-1, expiry 23:00 UTC → T-2, ..., expiry 14:00 UTC → T-11
    SELECT
        c.ticker,
        MIN(c.timestamp) AS first_ts
    FROM kalshi_candles c
    JOIN kalshi_markets m ON c.ticker = m.ticker
    WHERE c.volume > 0
      AND c.timestamp > m.expiry_time - INTERVAL '60 minutes'
      AND c.timestamp <= m.expiry_time
    GROUP BY c.ticker
),
snapshot_candles AS (
    SELECT
        m.trade_date,
        m.ticker,
        m.strike,
        m.expiry_time,
        CASE CAST(extract('hour' FROM m.expiry_time) AS INTEGER)
            WHEN 0  THEN 'T-1'
            WHEN 1  THEN 'T0'
            WHEN 2  THEN 'T+1'
            WHEN 3  THEN 'T+2'
            WHEN 4  THEN 'T+3'
            WHEN 5  THEN 'T+4'
            WHEN 6  THEN 'T+5'
            WHEN 7  THEN 'T+6'
            WHEN 8  THEN 'T+7'
            WHEN 9  THEN 'T+8'
            WHEN 10 THEN 'T+9'
            WHEN 11 THEN 'T+10'
            WHEN 12 THEN 'T+11'
            WHEN 13 THEN 'T+12'
            WHEN 14 THEN 'T-11'
            WHEN 15 THEN 'T-10'
            WHEN 16 THEN 'T-9'
            WHEN 17 THEN 'T-8'
            WHEN 18 THEN 'T-7'
            WHEN 19 THEN 'T-6'
            WHEN 20 THEN 'T-5'
            WHEN 21 THEN 'T-4'
            WHEN 22 THEN 'T-3'
            WHEN 23 THEN 'T-2'
        END AS snapshot,
        c.timestamp AS snapshot_ts,
        c.close     AS digi_px,
        c.volume
    FROM kalshi_candles c
    JOIN kalshi_markets m ON c.ticker = m.ticker
    JOIN first_bar fb ON c.ticker = fb.ticker AND c.timestamp = fb.first_ts
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
-- Binance uses open-period timestamps; the bar at (snapshot_ts - 1 min) covers
-- the same 1-minute window as the Kalshi end-period bar at snapshot_ts.
JOIN binance_klines b ON b.timestamp = sc.snapshot_ts - INTERVAL '1 minute'
WHERE sc.snapshot IS NOT NULL
ORDER BY sc.trade_date, sc.snapshot, sc.strike
"""


class SilverETL(BaseETL):
    def __init__(
        self,
        bronze_dir: Path = BRONZE_DIR,
        silver_dir: Path = SILVER_DIR,
    ) -> None:
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.silver_path = silver_dir / "contracts.parquet"

    async def extract(self) -> pl.DataFrame:
        markets_files = sorted(self.bronze_dir.glob("kalshi_markets_*.parquet"))
        candles_files = sorted(self.bronze_dir.glob("kalshi_candles_*.parquet"))
        klines_path = self.bronze_dir / "binance_btc_1m.parquet"

        if not markets_files:
            raise FileNotFoundError(
                f"No kalshi_markets_*.parquet files found in {self.bronze_dir}"
            )
        if not candles_files:
            raise FileNotFoundError(
                f"No kalshi_candles_*.parquet files found in {self.bronze_dir}"
            )
        if not klines_path.exists():
            raise FileNotFoundError(f"Bronze file missing: {klines_path}")

        markets_glob = str(self.bronze_dir / "kalshi_markets_*.parquet")
        candles_glob = str(self.bronze_dir / "kalshi_candles_*.parquet")

        conn = duckdb.connect()
        conn.execute("SET TimeZone='UTC'")
        conn.execute(
            f"CREATE VIEW kalshi_markets AS SELECT * FROM read_parquet('{markets_glob}')"
        )
        conn.execute(
            f"CREATE VIEW kalshi_candles AS SELECT * FROM read_parquet('{candles_glob}')"
        )
        conn.execute(
            f"CREATE VIEW binance_klines  AS SELECT * FROM read_parquet('{klines_path}')"
        )
        raw_df: pl.DataFrame = conn.execute(_JOIN_SQL).pl()
        conn.close()

        logger.info("DuckDB join produced %d rows", len(raw_df))
        if raw_df.is_empty():
            logger.warning("Silver ETL produced no rows — check bronze data coverage")
        return raw_df

    async def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        if raw.is_empty():
            return raw

        rows = raw.to_dicts()
        iv_vals: list[float | None] = []
        for r in tqdm(rows, desc="Computing implied vol", unit=" rows"):
            expiry = r["expiry_time"]
            snap = r["snapshot_ts"]
            T = (
                (expiry - snap).total_seconds() / _SECONDS_PER_YEAR
                if expiry > snap
                else None
            )
            iv_vals.append(
                invert_iv(
                    float(r["digi_px"]),
                    float(r["btc_close"]),
                    float(r["strike"]),
                    T if T is not None else 0.0,
                )
                if T is not None
                else None
            )

        result_df = raw.with_columns(
            [
                (pl.col("digi_px").cast(pl.Float32) / 100.0).alias("prob_itm"),
                pl.Series("implied_vol", iv_vals, dtype=pl.Float64).alias(
                    "implied_vol"
                ),
            ]
        )

        final_df = result_df.rename({"ticker": "digi_contract_name"}).select(
            [
                pl.col("trade_date").cast(pl.Date),
                pl.col("snapshot").cast(pl.Categorical),
                pl.col("snapshot_ts").cast(pl.Datetime("us", "UTC")),
                pl.col("digi_contract_name").cast(pl.Categorical),
                pl.col("strike").cast(pl.UInt32),
                pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
                pl.col("digi_px").cast(pl.UInt8),
                pl.col("prob_itm").cast(pl.Float32),
                pl.col("implied_vol").cast(pl.Float32),
                pl.col("btc_close").cast(pl.Float32),
                pl.col("volume").cast(pl.UInt32),
            ]
        )

        n_before = len(final_df)
        valid_df = final_df.filter(pl.col("volume") > 0).drop_nulls(
            subset=["implied_vol"]
        )
        n_dropped = n_before - len(valid_df)
        if n_dropped:
            logger.info("Dropped %d rows with null/invalid IV", n_dropped)

        group_counts = valid_df.group_by(["trade_date", "snapshot"]).agg(
            pl.len().alias("n_valid")
        )
        thin = group_counts.filter(pl.col("n_valid") < _MIN_VALID_STRIKES)
        if len(thin):
            logger.info(
                "Dropping %d (trade_date, snapshot) groups with < %d strikes",
                len(thin),
                _MIN_VALID_STRIKES,
            )
            sufficient = group_counts.filter(
                pl.col("n_valid") >= _MIN_VALID_STRIKES
            ).select(["trade_date", "snapshot"])
            valid_df = valid_df.join(
                sufficient, on=["trade_date", "snapshot"], how="inner"
            )

        return valid_df

    async def load(self, data: pl.DataFrame) -> None:
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        data.write_parquet(self.silver_path, compression="zstd", compression_level=3)
        logger.info("Silver ETL complete: %d rows → %s", len(data), self.silver_path)
