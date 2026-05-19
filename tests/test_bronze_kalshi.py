"""Tests for Kalshi bronze ETL strike-ladder, hour filters, and candle output."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from src.etl.bronze.kalshi_bronze import (
    SETTLEMENT_HOURS,
    KalshiBronzeETL,
    _filter_strike_ladder,
    _strike_ladder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_markets(trade_date: date, strikes: list[int], expiry_hour: int = 0) -> pl.DataFrame:
    expiry = datetime(trade_date.year, trade_date.month, trade_date.day, expiry_hour, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame({
        "ticker": [f"KXBTCD-T{s}" for s in strikes],
        "trade_date": [trade_date] * len(strikes),
        "strike": strikes,
        "expiry_time": [expiry] * len(strikes),
        "status": ["open"] * len(strikes),
        "settlement_price": [None] * len(strikes),
        "ingested_at": [datetime(2026, 5, 1, tzinfo=timezone.utc)] * len(strikes),
    }).with_columns([
        pl.col("ticker").cast(pl.Categorical),
        pl.col("trade_date").cast(pl.Date),
        pl.col("strike").cast(pl.UInt32),
        pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
        pl.col("status").cast(pl.Categorical),
        pl.col("settlement_price").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])


def _make_binance_df(trade_date: date, spot: float) -> pl.DataFrame:
    """Binance rows at window-open times for all 6 settlement hours."""
    rows = []
    for hour in SETTLEMENT_HOURS:
        expiry = datetime(trade_date.year, trade_date.month, trade_date.day, hour, 0, 0, tzinfo=timezone.utc)
        window_start = expiry - timedelta(hours=1)
        rows.append({"timestamp": window_start, "close": spot,
                     "ingested_at": datetime(2026, 5, 1, tzinfo=timezone.utc)})
    return pl.DataFrame(rows).with_columns([
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("close").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])


def _make_binance_parquet(trade_date: date, spot: float, tmp_path: Path) -> Path:
    path = tmp_path / "binance_btc_1m.parquet"
    _make_binance_df(trade_date, spot).write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_hour_filter_keeps_only_settlement_hours():
    """After the hour filter, only contracts whose expiry_time.hour is in SETTLEMENT_HOURS survive."""
    trade_date = date(2026, 5, 1)
    markets_target = pl.concat([
        _make_markets(trade_date, [95_000, 95_500], expiry_hour=h)
        for h in SETTLEMENT_HOURS
    ])
    markets_other = _make_markets(trade_date, [90_000], expiry_hour=12)
    all_markets = pl.concat([markets_target, markets_other])

    filtered = all_markets.filter(pl.col("expiry_time").dt.hour().is_in(SETTLEMENT_HOURS))

    assert len(filtered) == len(SETTLEMENT_HOURS) * 2
    assert set(filtered["expiry_time"].dt.hour().unique().to_list()) == set(SETTLEMENT_HOURS)


def test_strike_ladder_selects_9():
    """_filter_strike_ladder returns exactly 9 strikes (ATM + 4 above + 4 below)."""
    trade_date = date(2026, 5, 2)
    spot = 95_000.0
    strikes = list(range(90_000, 100_100, 100))
    markets = _make_markets(trade_date, strikes, expiry_hour=0)
    binance_df = _make_binance_df(trade_date, spot)

    result = _filter_strike_ladder(markets, binance_df)

    assert len(result) == 9
    result_strikes = set(result["strike"].cast(pl.Int64).to_list())
    # ATM = 95000; expected: ATM ± 500, 1000, 1500, 2000 (inclusive of ATM)
    expected = {
        95_000,
        95_500, 96_000, 96_500, 97_000,
        94_500, 94_000, 93_500, 93_000,
    }
    assert result_strikes == expected


def test_strike_ladder_missing_binance_raises(tmp_path: Path):
    """KalshiBronzeETL.extract raises FileNotFoundError when binance parquet is absent."""
    import asyncio
    trade_date = date(2026, 5, 2)
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    # No binance parquet in bronze dir

    markets_df = _make_markets(trade_date, [95_000, 95_500], expiry_hour=0)
    client = MagicMock()
    client.historical_cutoff = datetime(2317, 1, 1, tzinfo=timezone.utc)
    client.fetch_markets = AsyncMock(return_value=markets_df)

    etl = KalshiBronzeETL(client=client, start_date=trade_date, end_date=trade_date,
                           bronze_dir=bronze)
    with pytest.raises(FileNotFoundError, match="bronze-binance"):
        asyncio.run(etl.extract())


def test_strike_ladder_pure():
    """_strike_ladder returns 2×n_steps + 1 distinct strikes (ATM included)."""
    available = list(range(90_000, 100_100, 100))
    spot = 95_000.0
    result = _strike_ladder(spot, available, n_steps=4, step=500)

    assert len(result) == 9
    assert 95_000 in result


# ---------------------------------------------------------------------------
# Candle output contract
# ---------------------------------------------------------------------------

class TestCandleOutput:
    """
    ETL integration tests: candles parquet contains 1-minute close/volume bars
    for all instruments across all 6 settlement hour windows.
    """

    TRADE_DATE = date(2026, 5, 2)
    SETTLEMENT_HOURS = [22, 23, 0, 1, 2, 3]
    SPOT = 95_000.0
    # 9 strikes per expiry hour (ATM + 4 each side at $500)
    STRIKES_PER_HOUR = [93_000, 93_500, 94_000, 94_500, 95_000, 95_500, 96_000, 96_500, 97_000]

    def _make_markets(self) -> pl.DataFrame:
        dfs = []
        for hour in self.SETTLEMENT_HOURS:
            expiry = datetime(
                self.TRADE_DATE.year, self.TRADE_DATE.month, self.TRADE_DATE.day,
                hour, 0, 0, tzinfo=timezone.utc,
            )
            ingested = datetime(2026, 5, 3, tzinfo=timezone.utc)
            dfs.append(pl.DataFrame({
                "ticker": [f"KXBTCD-H{hour:02d}-T{s}" for s in self.STRIKES_PER_HOUR],
                "trade_date": [self.TRADE_DATE] * len(self.STRIKES_PER_HOUR),
                "strike": self.STRIKES_PER_HOUR,
                "expiry_time": [expiry] * len(self.STRIKES_PER_HOUR),
                "status": ["open"] * len(self.STRIKES_PER_HOUR),
                "settlement_price": [None] * len(self.STRIKES_PER_HOUR),
                "ingested_at": [ingested] * len(self.STRIKES_PER_HOUR),
            }).with_columns([
                pl.col("ticker").cast(pl.Categorical),
                pl.col("trade_date").cast(pl.Date),
                pl.col("strike").cast(pl.UInt32),
                pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
                pl.col("status").cast(pl.Categorical),
                pl.col("settlement_price").cast(pl.Float32),
                pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
            ]))
        return pl.concat(dfs)

    def _make_minute_candles(self, markets_df: pl.DataFrame) -> pl.DataFrame:
        """One row per (ticker, minute) for each contract's 1-hour trading window."""
        ingested = datetime(2026, 5, 3, tzinfo=timezone.utc)
        rows_ticker, rows_ts, rows_close, rows_vol = [], [], [], []
        for row in markets_df.iter_rows(named=True):
            expiry_ts = row["expiry_time"]
            window_start = expiry_ts - timedelta(hours=1)
            t = window_start
            while t <= expiry_ts:
                rows_ticker.append(row["ticker"])
                rows_ts.append(t)
                rows_close.append(50)
                rows_vol.append(10)
                t += timedelta(minutes=1)
        return pl.DataFrame({
            "ticker": rows_ticker,
            "timestamp": rows_ts,
            "close": rows_close,
            "volume": rows_vol,
            "ingested_at": [ingested] * len(rows_ticker),
        }).with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("close").cast(pl.UInt8),
            pl.col("volume").cast(pl.UInt32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])

    def _run(self, tmp_path: Path) -> pl.DataFrame:
        bronze = tmp_path / "bronze"
        bronze.mkdir()

        markets_df = self._make_markets()
        candles_df = self._make_minute_candles(markets_df)

        _make_binance_parquet(self.TRADE_DATE, self.SPOT, bronze)

        def _candles_for(tickers, *args, **kwargs):
            return candles_df.filter(pl.col("ticker").cast(pl.Utf8).is_in(tickers))

        client = MagicMock()
        client.historical_cutoff = datetime(2317, 1, 1, tzinfo=timezone.utc)
        client.fetch_markets = AsyncMock(return_value=markets_df)
        client.fetch_candles = AsyncMock(side_effect=_candles_for)

        KalshiBronzeETL(
            client=client,
            start_date=self.TRADE_DATE,
            end_date=self.TRADE_DATE,
            bronze_dir=bronze,
        ).run()

        return pl.read_parquet(bronze / f"kalshi_candles_{self.TRADE_DATE}.parquet")

    def test_schema(self, tmp_path):
        df = self._run(tmp_path)
        assert df["ticker"].dtype in (pl.Categorical, pl.String)
        assert df["timestamp"].dtype == pl.Datetime("us", "UTC")
        assert df["close"].dtype == pl.UInt8
        assert df["volume"].dtype == pl.UInt32

    def test_no_duplicate_ticker_timestamp(self, tmp_path):
        df = self._run(tmp_path)
        assert df.select(["ticker", "timestamp"]).is_duplicated().sum() == 0

    def test_close_in_valid_range(self, tmp_path):
        df = self._run(tmp_path)
        assert (df["close"] >= 0).all()
        assert (df["close"] <= 100).all()

    def test_all_market_tickers_have_candles(self, tmp_path):
        bronze = tmp_path / "bronze"
        if not bronze.exists():
            bronze.mkdir()
            markets_df = self._make_markets()
            candles_df = self._make_minute_candles(markets_df)
            _make_binance_parquet(self.TRADE_DATE, self.SPOT, bronze)
            def _candles_for(tickers, *args, **kwargs):
                return candles_df.filter(pl.col("ticker").cast(pl.Utf8).is_in(tickers))
            client = MagicMock()
            client.historical_cutoff = datetime(2317, 1, 1, tzinfo=timezone.utc)
            client.fetch_markets = AsyncMock(return_value=markets_df)
            client.fetch_candles = AsyncMock(side_effect=_candles_for)
            KalshiBronzeETL(client=client, start_date=self.TRADE_DATE, end_date=self.TRADE_DATE,
                             bronze_dir=bronze).run()

        markets = pl.read_parquet(bronze / f"kalshi_markets_{self.TRADE_DATE}.parquet")
        candles = pl.read_parquet(bronze / f"kalshi_candles_{self.TRADE_DATE}.parquet")
        market_tickers = set(markets["ticker"].cast(pl.Utf8).to_list())
        candle_tickers = set(candles["ticker"].cast(pl.Utf8).to_list())
        assert candle_tickers == market_tickers

    def test_candles_within_trading_windows(self, tmp_path):
        df = self._run(tmp_path)
        markets = pl.read_parquet(tmp_path / "bronze" / f"kalshi_markets_{self.TRADE_DATE}.parquet")
        # Every candle timestamp must be within [expiry_ts - 1h, expiry_ts] for its ticker
        joined = (
            df.with_columns(pl.col("ticker").cast(pl.Utf8))
            .join(
                markets.select(["ticker", "expiry_time"])
                .with_columns(pl.col("ticker").cast(pl.Utf8)),
                on="ticker", how="left",
            )
        )
        window_start = joined["expiry_time"] - timedelta(hours=1)
        assert (joined["timestamp"] >= window_start).all()
        assert (joined["timestamp"] <= joined["expiry_time"]).all()
