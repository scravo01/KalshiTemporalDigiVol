import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.clients.binance_client import BinanceClient

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze")
KLINES_PATH = BRONZE_DIR / "binance_btc_1m.parquet"


def run(client: BinanceClient, start_date: date, end_date: date) -> None:
    logger.info(f"Binance bronze ETL: {start_date} → {end_date}")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Extend window to cover T-1 (23:00 prior day) and T+1 (01:00 next day)
    start_dt = datetime(start_date.year, start_date.month, start_date.day, 22, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 2, 0, 0, tzinfo=timezone.utc) + timedelta(days=1)

    klines_df = client.fetch_klines(start_dt, end_dt)
    klines_df.write_parquet(KLINES_PATH, compression="zstd", compression_level=3)
    logger.info(f"Wrote {len(klines_df)} kline rows → {KLINES_PATH}")
