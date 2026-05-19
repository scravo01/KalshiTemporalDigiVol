import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from src.etl.base import BaseETL

if TYPE_CHECKING:
    from src.clients.kalshi_client import KalshiClient

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze")


class KalshiBronzeETL(BaseETL):
    def __init__(
        self,
        client: "KalshiClient",
        start_date: date,
        end_date: date,
        bronze_dir: Path = BRONZE_DIR,
    ) -> None:
        self.client = client
        self.start_date = start_date
        self.end_date = end_date
        self.bronze_dir = bronze_dir
        self.markets_path = bronze_dir / "kalshi_markets.parquet"
        self.candles_path = bronze_dir / "kalshi_candles.parquet"

    async def extract(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        logger.info("Kalshi bronze extract: %s → %s", self.start_date, self.end_date)
        markets_df = await self.client.fetch_markets(self.start_date, self.end_date)

        cutoff_date = self.client.historical_cutoff.date()
        hist_df = markets_df.filter(pl.col("trade_date") < cutoff_date)
        live_df = markets_df.filter(pl.col("trade_date") >= cutoff_date)

        candle_dfs: list[pl.DataFrame] = []
        for group_df, is_historical in [(hist_df, True), (live_df, False)]:
            if group_df.is_empty():
                continue
            tickers = group_df["ticker"].cast(pl.Utf8).to_list()
            dates = group_df["trade_date"]
            # Start at 23:00 UTC the day before to capture T-1 snapshots
            min_d = dates.min()
            prev_day = date(min_d.year, min_d.month, min_d.day) - timedelta(days=1)
            start_ts = datetime(prev_day.year, prev_day.month, prev_day.day, 23, 0, 0, tzinfo=timezone.utc)
            # End at 23:59 UTC to capture all intraday settlements + T+1 snapshot
            max_d = dates.max()
            end_ts = datetime(max_d.year, max_d.month, max_d.day, 23, 59, 59, tzinfo=timezone.utc)
            df = await self.client.fetch_candles(tickers, start_ts, end_ts, is_historical)
            candle_dfs.append(df)

        if candle_dfs:
            candles_df = pl.concat(candle_dfs)
        else:
            from src.clients.kalshi_client import _empty_candles_df
            candles_df = _empty_candles_df()

        return markets_df, candles_df

    async def transform(
        self, raw: tuple[pl.DataFrame, pl.DataFrame]
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        return raw  # bronze is raw-preserving; client already normalises dtypes

    async def load(self, data: tuple[pl.DataFrame, pl.DataFrame]) -> None:
        markets_df, candles_df = data
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        markets_df.write_parquet(self.markets_path, compression="zstd", compression_level=3)
        logger.info("Wrote %d market rows → %s", len(markets_df), self.markets_path)
        if candles_df.is_empty():
            logger.warning("No candle data — skipping candles parquet write")
            return
        candles_df.write_parquet(self.candles_path, compression="zstd", compression_level=3)
        logger.info("Wrote %d candle rows → %s", len(candles_df), self.candles_path)
