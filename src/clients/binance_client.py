import logging
from datetime import datetime, timezone

import polars as pl
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.binance.com"
_KLINES_LIMIT = 1000


class BinanceClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    def fetch_klines(self, start_dt: datetime, end_dt: datetime) -> pl.DataFrame:
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
        rows: list[dict] = []
        current_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        while current_ms < end_ms:
            resp = self.session.get(
                f"{BASE_URL}/api/v3/klines",
                params={
                    "symbol": "BTCUSDT",
                    "interval": "1m",
                    "startTime": current_ms,
                    "limit": _KLINES_LIMIT,
                },
                timeout=30,
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break

            for bar in batch:
                ts_s = int(bar[0]) // 1000  # open time ms → seconds
                close = float(bar[4])
                rows.append({"timestamp": ts_s, "close": close, "ingested_at": ingested_at})

            last_ts_ms = int(batch[-1][0])
            if len(batch) < _KLINES_LIMIT or last_ts_ms >= end_ms:
                break
            current_ms = last_ts_ms + 60_000  # advance by one bar

        if not rows:
            logger.warning("fetch_klines returned 0 rows")
            return pl.DataFrame(schema={
                "timestamp": pl.Datetime("us", "UTC"),
                "close": pl.Float32,
                "ingested_at": pl.Datetime("us", "UTC"),
            })

        df = (
            pl.DataFrame(rows)
            .with_columns([
                (pl.col("timestamp").cast(pl.Int64) * 1_000_000)
                  .cast(pl.Datetime("us"))
                  .dt.replace_time_zone("UTC")
                  .alias("timestamp"),
                pl.col("close").cast(pl.Float32),
                pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
            ])
        )
        logger.info(f"fetch_klines: {len(df)} rows ({start_dt.date()} → {end_dt.date()})")
        return df
