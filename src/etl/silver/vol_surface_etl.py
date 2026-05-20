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

# All volume-positive candles in each contract's 60-minute window (no first-bar filter).
# LEFT JOIN on Binance so candles during Binance gaps are retained and dropped in Python.
_SURFACE_SQL = """
SELECT
    m.trade_date,
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
    m.ticker,
    m.strike,
    m.expiry_time,
    c.timestamp AS bar_ts,
    c.close     AS digi_px,
    c.volume,
    b.close     AS btc_close
FROM kalshi_candles c
JOIN      kalshi_markets  m ON c.ticker     = m.ticker
LEFT JOIN binance_klines  b ON b.timestamp  = c.timestamp - INTERVAL '1 minute'
WHERE c.volume > 0
  AND c.timestamp >  m.expiry_time - INTERVAL '60 minutes'
  AND c.timestamp <= m.expiry_time
ORDER BY m.trade_date, m.expiry_time, m.strike, c.timestamp
"""


class VolSurfaceETL(BaseETL):
    def __init__(
        self,
        bronze_dir: Path = BRONZE_DIR,
        silver_dir: Path = SILVER_DIR,
    ) -> None:
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.surface_path = silver_dir / "vol_surface.parquet"

    async def extract(self) -> pl.DataFrame:
        markets_files = sorted(self.bronze_dir.glob("kalshi_markets_*.parquet"))
        candles_files = sorted(self.bronze_dir.glob("kalshi_candles_*.parquet"))
        klines_path = self.bronze_dir / "binance_btc_1m.parquet"

        if not markets_files:
            raise FileNotFoundError(f"No kalshi_markets_*.parquet in {self.bronze_dir}")
        if not candles_files:
            raise FileNotFoundError(f"No kalshi_candles_*.parquet in {self.bronze_dir}")
        if not klines_path.exists():
            raise FileNotFoundError(f"Bronze file missing: {klines_path}")

        markets_glob = str(self.bronze_dir / "kalshi_markets_*.parquet")
        candles_glob = str(self.bronze_dir / "kalshi_candles_*.parquet")

        conn = duckdb.connect()
        conn.execute("SET TimeZone='UTC'")
        conn.execute(
            f"CREATE VIEW kalshi_markets  AS SELECT * FROM read_parquet('{markets_glob}')"
        )
        conn.execute(
            f"CREATE VIEW kalshi_candles  AS SELECT * FROM read_parquet('{candles_glob}')"
        )
        conn.execute(
            f"CREATE VIEW binance_klines  AS SELECT * FROM read_parquet('{klines_path}')"
        )
        raw_df: pl.DataFrame = conn.execute(_SURFACE_SQL).pl()
        conn.close()

        logger.info("Vol surface DuckDB query produced %d rows", len(raw_df))
        return raw_df

    async def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        if raw.is_empty():
            return raw

        # Drop rows where Binance had a gap (LEFT JOIN produced NULL btc_close)
        n_before = len(raw)
        raw = raw.filter(pl.col("btc_close").is_not_null())
        if n_before - len(raw):
            logger.info(
                "Dropped %d rows with null btc_close (Binance gap)", n_before - len(raw)
            )

        rows = raw.to_dicts()
        iv_vals: list[float | None] = []
        mins_to_expiry: list[int] = []

        for r in tqdm(rows, desc="Computing vol surface IV", unit=" rows"):
            expiry = r["expiry_time"]
            bar_ts = r["bar_ts"]
            diff_s = (expiry - bar_ts).total_seconds()
            T = diff_s / _SECONDS_PER_YEAR if diff_s > 0 else None
            iv_vals.append(
                invert_iv(
                    float(r["digi_px"]),
                    float(r["btc_close"]),
                    float(r["strike"]),
                    T,
                )
                if T is not None
                else None
            )
            mins_to_expiry.append(max(0, round(diff_s / 60)) if diff_s >= 0 else 0)

        result = raw.with_columns(
            [
                pl.Series("minutes_to_expiry", mins_to_expiry, dtype=pl.UInt8),
                (pl.col("digi_px").cast(pl.Float32) / 100.0).alias("delta"),
                pl.Series("implied_vol", iv_vals, dtype=pl.Float64).cast(pl.Float32),
            ]
        )

        final = result.rename({"ticker": "digi_contract_name"}).select(
            [
                pl.col("trade_date").cast(pl.Date),
                pl.col("snapshot").cast(pl.Categorical),
                pl.col("bar_ts").cast(pl.Datetime("us", "UTC")),
                pl.col("minutes_to_expiry"),
                pl.col("digi_contract_name").cast(pl.Categorical),
                pl.col("strike").cast(pl.UInt32),
                pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
                pl.col("digi_px").cast(pl.UInt8),
                pl.col("delta").cast(pl.Float32),
                pl.col("implied_vol").cast(pl.Float32),
                pl.col("btc_close").cast(pl.Float32),
            ]
        )

        n_null_iv = final["implied_vol"].is_null().sum()
        if n_null_iv:
            logger.info("Dropping %d rows with null implied_vol", n_null_iv)
        final = final.filter(pl.col("implied_vol").is_not_null())

        logger.info("Vol surface transform complete: %d rows", len(final))
        return final

    async def load(self, data: pl.DataFrame) -> None:
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        data.write_parquet(self.surface_path, compression="zstd", compression_level=3)
        logger.info("Vol surface written: %d rows → %s", len(data), self.surface_path)
