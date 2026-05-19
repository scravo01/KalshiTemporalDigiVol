"""Shared fixtures available to all test modules."""
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Synthetic bronze parquets for one trade-date (2024-05-25), three contracts, three snapshots."""
    trade_date = date(2024, 5, 25)
    ingested = datetime(2024, 5, 26, 0, 0, 0, tzinfo=timezone.utc)

    markets = pl.DataFrame({
        "ticker": ["KXBTCD-25MAY24-B95000", "KXBTCD-25MAY24-B95500", "KXBTCD-25MAY24-B94500"],
        "trade_date": [trade_date] * 3,
        "strike": [95_000, 95_500, 94_500],
        "expiry_time": [datetime(2024, 5, 25, 21, 0, 0, tzinfo=timezone.utc)] * 3,
        "status": ["settled"] * 3,
        "settlement_price": [None, None, None],
        "ingested_at": [ingested] * 3,
    }).with_columns([
        pl.col("ticker").cast(pl.Categorical),
        pl.col("trade_date").cast(pl.Date),
        pl.col("strike").cast(pl.UInt32),
        pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
        pl.col("status").cast(pl.Categorical),
        pl.col("settlement_price").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    snap_ts = [
        datetime(2024, 5, 24, 23, 0, 0, tzinfo=timezone.utc),  # T-1
        datetime(2024, 5, 25, 0, 0, 0, tzinfo=timezone.utc),   # T0
        datetime(2024, 5, 25, 1, 0, 0, tzinfo=timezone.utc),   # T+1
    ]
    ticker_list, ts_list, close_list, vol_list = [], [], [], []
    for ticker, close_base in [
        ("KXBTCD-25MAY24-B95000", 50),
        ("KXBTCD-25MAY24-B95500", 38),
        ("KXBTCD-25MAY24-B94500", 63),
    ]:
        for i, ts in enumerate(snap_ts):
            ticker_list.append(ticker)
            ts_list.append(ts)
            close_list.append(close_base + i)
            vol_list.append(100 + i * 10)

    candles = pl.DataFrame({
        "ticker": ticker_list,
        "timestamp": ts_list,
        "close": close_list,
        "volume": vol_list,
        "ingested_at": [ingested] * len(ticker_list),
    }).with_columns([
        pl.col("ticker").cast(pl.Categorical),
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("close").cast(pl.UInt8),
        pl.col("volume").cast(pl.UInt32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    binance = pl.DataFrame({
        "timestamp": snap_ts,
        "close": [95_200.0, 95_100.0, 95_300.0],
        "ingested_at": [ingested] * 3,
    }).with_columns([
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("close").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    bronze = tmp_path / "bronze"
    bronze.mkdir()
    markets.write_parquet(bronze / "kalshi_markets.parquet")
    candles.write_parquet(bronze / "kalshi_candles.parquet")
    binance.write_parquet(bronze / "binance_btc_1m.parquet")
    return tmp_path
