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

# All 24 UTC settlement hours — every hourly Kalshi binary contract in the ET day
SETTLEMENT_HOURS: list[int] = list(range(24))


def markets_path(bronze_dir: Path, trade_date: date) -> Path:
    return bronze_dir / f"kalshi_markets_{trade_date}.parquet"


def candles_path(bronze_dir: Path, trade_date: date) -> Path:
    return bronze_dir / f"kalshi_candles_{trade_date}.parquet"


def _strike_ladder(
    spot: float, available_strikes: list[int], n_steps: int = 4, step: int = 500
) -> set[int]:
    """ATM + n_steps above + n_steps below at $step increments (ATM included)."""
    atm = min(available_strikes, key=lambda k: abs(k - spot))
    targets = {atm + i * step for i in range(-n_steps, n_steps + 1)}
    return {min(available_strikes, key=lambda k: abs(k - t)) for t in targets}


def _filter_strike_ladder(
    markets_df: pl.DataFrame,
    binance_df: pl.DataFrame,
    n_steps: int = 4,
    step: int = 500,
) -> pl.DataFrame:
    """For each expiry window, select ATM ± n_steps at $step increments.

    Uses BTC spot at window_open (expiry_ts - 1h) as the ATM reference.
    """
    if markets_df.is_empty():
        return markets_df

    keep_rows: list[tuple] = []  # (expiry_time, strike_i64)

    for expiry_ts in markets_df["expiry_time"].unique().to_list():
        window_start = expiry_ts - timedelta(hours=1)
        row = binance_df.filter(pl.col("timestamp") <= window_start).tail(1)
        if row.is_empty():
            logger.warning(
                "No Binance spot at or before %s — skipping ATM filter for this window",
                window_start,
            )
            continue
        spot = float(row["close"][0])
        group = markets_df.filter(pl.col("expiry_time") == expiry_ts)
        available = group["strike"].cast(pl.Int64).to_list()
        ladder = _strike_ladder(spot, available, n_steps, step)
        for strike in ladder:
            keep_rows.append((expiry_ts, int(strike)))

    if not keep_rows:
        logger.warning(
            "_filter_strike_ladder: no strikes selected — returning empty markets_df"
        )
        return markets_df.clear()

    keep_df = pl.DataFrame(
        {
            "expiry_time": [r[0] for r in keep_rows],
            "strike_keep": [r[1] for r in keep_rows],
        },
        schema={"expiry_time": pl.Datetime("us", "UTC"), "strike_keep": pl.Int64},
    )
    return (
        markets_df.with_columns(pl.col("strike").cast(pl.Int64).alias("strike_i64"))
        .join(
            keep_df,
            left_on=["expiry_time", "strike_i64"],
            right_on=["expiry_time", "strike_keep"],
            how="inner",
        )
        .drop("strike_i64")
    )


class KalshiBronzeETL(BaseETL):
    def __init__(
        self,
        client: "KalshiClient",
        start_date: date,
        end_date: date,
        bronze_dir: Path = BRONZE_DIR,
        settlement_hours: list[int] = SETTLEMENT_HOURS,
    ) -> None:
        self.client = client
        self.start_date = start_date
        self.end_date = end_date
        self.bronze_dir = bronze_dir
        self.settlement_hours = settlement_hours

    async def extract(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        logger.info(
            "Kalshi bronze extract: %s → %s  hours=%s",
            self.start_date,
            self.end_date,
            self.settlement_hours,
        )

        all_dates = [
            self.start_date + timedelta(days=i)
            for i in range((self.end_date - self.start_date).days + 1)
        ]
        missing_dates = [
            d for d in all_dates if not markets_path(self.bronze_dir, d).exists()
        ]

        if not missing_dates:
            logger.info(
                "All %d dates already on disk — nothing to fetch", len(all_dates)
            )
            from src.clients.kalshi_client import _empty_candles_df

            return pl.DataFrame(), _empty_candles_df()

        logger.info(
            "%d / %d dates need downloading", len(missing_dates), len(all_dates)
        )
        fetch_start = min(missing_dates)
        fetch_end = max(missing_dates)

        markets_df = await self.client.fetch_markets(fetch_start, fetch_end)
        missing_set = set(missing_dates)
        markets_df = markets_df.filter(pl.col("trade_date").is_in(missing_set))
        markets_df = markets_df.filter(
            pl.col("expiry_time").dt.hour().is_in(self.settlement_hours)
        )
        logger.info(
            "After settlement-hours filter %s: %d markets",
            self.settlement_hours,
            len(markets_df),
        )

        if markets_df.is_empty():
            from src.clients.kalshi_client import _empty_candles_df

            return markets_df, _empty_candles_df()

        binance_path = self.bronze_dir / "binance_btc_1m.parquet"
        if not binance_path.exists():
            raise FileNotFoundError(
                f"Binance parquet not found at {binance_path}. Run 'kvol bronze-binance' first."
            )
        binance_df = pl.read_parquet(binance_path)
        markets_df = _filter_strike_ladder(markets_df, binance_df)
        logger.info("After strike ladder filter: %d markets", len(markets_df))

        if markets_df.is_empty():
            from src.clients.kalshi_client import _empty_candles_df

            return markets_df, _empty_candles_df()

        # Fetch candles per expiry window (each window = 1 hour before settlement)
        candle_dfs: list[pl.DataFrame] = []
        expiry_times = sorted(markets_df["expiry_time"].unique().to_list())
        for expiry_ts in expiry_times:
            group = markets_df.filter(pl.col("expiry_time") == expiry_ts)
            tickers = group["ticker"].cast(pl.Utf8).to_list()
            window_start = expiry_ts - timedelta(hours=1)
            df = await self.client.fetch_candles(
                tickers, window_start, expiry_ts, period_interval=1
            )
            if not df.is_empty():
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
        markets_df, candles_df = raw
        if markets_df.is_empty() or candles_df.is_empty():
            return markets_df, candles_df
        ticker_to_date = markets_df.select(["ticker", "trade_date"]).with_columns(
            pl.col("ticker").cast(pl.Utf8)
        )
        candles_df = candles_df.with_columns(pl.col("ticker").cast(pl.Utf8)).join(
            ticker_to_date, on="ticker", how="left"
        )
        return markets_df, candles_df

    async def load(self, data: tuple[pl.DataFrame, pl.DataFrame]) -> None:
        markets_df, candles_df = data
        if markets_df.is_empty():
            return

        self.bronze_dir.mkdir(parents=True, exist_ok=True)

        for trade_date in sorted(markets_df["trade_date"].unique().to_list()):
            mpath = markets_path(self.bronze_dir, trade_date)
            date_markets = markets_df.filter(pl.col("trade_date") == trade_date)
            date_markets.write_parquet(mpath, compression="zstd", compression_level=3)
            logger.info("Wrote %d market rows → %s", len(date_markets), mpath)

            if not candles_df.is_empty():
                cpath = candles_path(self.bronze_dir, trade_date)
                date_candles = candles_df.filter(
                    pl.col("trade_date") == trade_date
                ).drop("trade_date")
                if date_candles.is_empty():
                    logger.warning(
                        "No candle data for %s — skipping candles write", trade_date
                    )
                else:
                    date_candles.write_parquet(
                        cpath, compression="zstd", compression_level=3
                    )
                    logger.info("Wrote %d candle rows → %s", len(date_candles), cpath)
