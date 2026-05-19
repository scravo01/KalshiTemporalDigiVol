import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from src.etl.base import BaseETL

if TYPE_CHECKING:
    from src.clients.binance_client import BinanceClient

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze")


class BinanceBronzeETL(BaseETL):
    def __init__(
        self,
        client: "BinanceClient",
        start_date: date,
        end_date: date,
        bronze_dir: Path = BRONZE_DIR,
    ) -> None:
        self.client = client
        self.start_date = start_date
        self.end_date = end_date
        self.bronze_dir = bronze_dir
        self.klines_path = bronze_dir / "binance_btc_1m.parquet"

    async def extract(self) -> pl.DataFrame:
        # Extend window to cover T-1 (23:00 prior day) and T+1 (01:00 next day)
        start_dt = (
            datetime(self.start_date.year, self.start_date.month, self.start_date.day,
                     22, 0, 0, tzinfo=timezone.utc)
            - timedelta(days=1)
        )
        end_dt = (
            datetime(self.end_date.year, self.end_date.month, self.end_date.day,
                     2, 0, 0, tzinfo=timezone.utc)
            + timedelta(days=1)
        )
        logger.info("Binance bronze extract: %s → %s", start_dt, end_dt)
        return await self.client.fetch_klines(start_dt, end_dt)

    async def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        return raw

    async def load(self, data: pl.DataFrame) -> None:
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        data.write_parquet(self.klines_path, compression="zstd", compression_level=3)
        logger.info("Wrote %d kline rows → %s", len(data), self.klines_path)
