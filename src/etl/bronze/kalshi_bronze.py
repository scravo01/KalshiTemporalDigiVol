import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from src.clients.kalshi_client import KalshiClient

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze")
MARKETS_PATH = BRONZE_DIR / "kalshi_markets.parquet"
CANDLES_PATH = BRONZE_DIR / "kalshi_candles.parquet"
BATCH_SIZE = 7  # floor(10_000 candles / 1_260 bars per contract-day)
_BATCH_SLEEP_S = 0.05  # 50 ms between batch calls — stays well under 20 req/s


def run(client: KalshiClient, start_date: date, end_date: date) -> None:
    logger.info(f"Kalshi bronze ETL: {start_date} → {end_date}")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # ── markets ──────────────────────────────────────────────────────────────
    markets_df = client.fetch_markets(start_date, end_date)
    markets_df.write_parquet(MARKETS_PATH, compression="zstd", compression_level=3)
    logger.info(f"Wrote {len(markets_df)} market rows → {MARKETS_PATH}")

    if markets_df.is_empty():
        logger.warning("No markets fetched — skipping candle ETL")
        return

    # ── split tickers by historical cutoff ───────────────────────────────────
    cutoff_date = client.historical_cutoff.date()
    hist_df = markets_df.filter(pl.col("trade_date") < cutoff_date)
    live_df = markets_df.filter(pl.col("trade_date") >= cutoff_date)

    all_candle_dfs: list[pl.DataFrame] = []

    for group_df, is_historical in [(hist_df, True), (live_df, False)]:
        if group_df.is_empty():
            continue
        tickers = group_df["ticker"].cast(pl.Utf8).to_list()
        batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
        label = "historical" if is_historical else "live"
        logger.info(f"Fetching {label} candles: {len(tickers)} tickers in {len(batches)} batches")

        for i, batch in enumerate(batches, 1):
            # Use the date range of this specific batch for the time window
            batch_dates = (
                group_df.filter(pl.col("ticker").cast(pl.Utf8).is_in(batch))["trade_date"]
            )
            min_date = batch_dates.min()
            max_date = batch_dates.max()

            start_ts = datetime(min_date.year, min_date.month, min_date.day, 0, 0, 0, tzinfo=timezone.utc)
            end_ts = datetime(max_date.year, max_date.month, max_date.day, 21, 0, 0, tzinfo=timezone.utc)

            candles = client.fetch_candles(batch, start_ts, end_ts, is_historical)
            all_candle_dfs.append(candles)

            if i % 10 == 0:
                logger.info(f"  {label} batch {i}/{len(batches)} done")
            time.sleep(_BATCH_SLEEP_S)

    if not all_candle_dfs:
        logger.warning("No candle data fetched")
        return

    candles_df = pl.concat(all_candle_dfs)
    candles_df.write_parquet(CANDLES_PATH, compression="zstd", compression_level=3)
    logger.info(f"Wrote {len(candles_df)} candle rows → {CANDLES_PATH}")
