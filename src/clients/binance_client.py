import logging
from datetime import datetime, timezone
from typing import ClassVar, Optional

import aiohttp
import polars as pl
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm as atqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://api.binance.us"
_KLINES_LIMIT = 1000
_RATE_LIMIT = 20  # requests per second (well within Binance's 1 200/min)
_TIMEOUT = aiohttp.ClientTimeout(total=30)


class BinanceClient:
    _limiter: ClassVar[AsyncLimiter] = AsyncLimiter(_RATE_LIMIT, 1.0)

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=_TIMEOUT)
        return self._session

    async def fetch_klines(self, start_dt: datetime, end_dt: datetime) -> pl.DataFrame:
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
        rows: list[dict] = []
        current_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        session = self._get_session()

        with atqdm(desc="Binance klines", unit=" pages") as pbar:
            while current_ms < end_ms:
                async with self._limiter:
                    async with session.get(
                        f"{BASE_URL}/api/v3/klines",
                        params={
                            "symbol": "BTCUSDT",
                            "interval": "1m",
                            "startTime": current_ms,
                            "limit": _KLINES_LIMIT,
                        },
                    ) as resp:
                        resp.raise_for_status()
                        batch = await resp.json()

                if not batch:
                    break
                for bar in batch:
                    ts_s = int(bar[0]) // 1000
                    rows.append(
                        {
                            "timestamp": ts_s,
                            "close": float(bar[4]),
                            "ingested_at": ingested_at,
                        }
                    )
                pbar.update(1)
                last_ts_ms = int(batch[-1][0])
                if len(batch) < _KLINES_LIMIT or last_ts_ms >= end_ms:
                    break
                current_ms = last_ts_ms + 60_000

        if not rows:
            logger.warning("fetch_klines returned 0 rows")
            return pl.DataFrame(
                schema={
                    "timestamp": pl.Datetime("us", "UTC"),
                    "close": pl.Float32,
                    "ingested_at": pl.Datetime("us", "UTC"),
                }
            )

        df = pl.DataFrame(rows).with_columns(
            [
                (pl.col("timestamp").cast(pl.Int64) * 1_000_000)
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("UTC")
                .alias("timestamp"),
                pl.col("close").cast(pl.Float32),
                pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
            ]
        )
        logger.info(
            "fetch_klines: %d rows (%s → %s)", len(df), start_dt.date(), end_dt.date()
        )
        return df
