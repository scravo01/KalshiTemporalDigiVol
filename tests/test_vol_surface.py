"""Tests for VolSurfaceETL — minute-by-minute implied vol surface."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from src.etl.silver.vol_surface_etl import VolSurfaceETL


# ---------------------------------------------------------------------------
# Local fixture: bronze with 3 candles per contract per window
# ---------------------------------------------------------------------------

@pytest.fixture
def bronze_surface(tmp_path: Path) -> Path:
    """3 expiry windows × 3 strikes × 3 candles each.

    Candle offsets from expiry: -59 min, -45 min, -30 min.
    Binance bar for each candle at (bar_ts - 1 min).
    Spot 95_150 avoids all strikes (94500, 95000, 95500).
    """
    trade_date = date(2024, 5, 25)
    ingested = datetime(2024, 5, 26, 4, 0, 0, tzinfo=timezone.utc)
    strikes = [94_500, 95_000, 95_500]
    close_by_strike = {94_500: 65, 95_000: 53, 95_500: 38}
    expiry_times = [
        datetime(2024, 5, 25, 22, 0, 0, tzinfo=timezone.utc),  # T-3
        datetime(2024, 5, 26, 1, 0, 0, tzinfo=timezone.utc),   # T0
        datetime(2024, 5, 26, 3, 0, 0, tzinfo=timezone.utc),   # T+2
    ]
    bar_offsets_min = [59, 45, 30]

    market_rows = []
    for exp in expiry_times:
        for s in strikes:
            market_rows.append({
                "ticker": f"KXBTCD-TEST-E{exp.hour}H-T{s}",
                "trade_date": trade_date,
                "strike": s,
                "expiry_time": exp,
                "status": "settled",
                "settlement_price": None,
                "ingested_at": ingested,
            })
    markets = pl.DataFrame(market_rows).with_columns([
        pl.col("ticker").cast(pl.Categorical),
        pl.col("trade_date").cast(pl.Date),
        pl.col("strike").cast(pl.UInt32),
        pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
        pl.col("status").cast(pl.Categorical),
        pl.col("settlement_price").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    candle_rows = []
    for exp in expiry_times:
        for offset in bar_offsets_min:
            bar_ts = exp - timedelta(minutes=offset)
            for s in strikes:
                candle_rows.append({
                    "ticker": f"KXBTCD-TEST-E{exp.hour}H-T{s}",
                    "timestamp": bar_ts,
                    "close": close_by_strike[s],
                    "volume": 100,
                    "ingested_at": ingested,
                })
    candles = pl.DataFrame(candle_rows).with_columns([
        pl.col("ticker").cast(pl.Categorical),
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("close").cast(pl.UInt8),
        pl.col("volume").cast(pl.UInt32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    # Binance bar at bar_ts - 1 min for each candle
    binance_rows = []
    seen = set()
    for exp in expiry_times:
        for offset in bar_offsets_min:
            bar_ts = exp - timedelta(minutes=offset)
            b_ts = bar_ts - timedelta(minutes=1)
            if b_ts not in seen:
                seen.add(b_ts)
                binance_rows.append({
                    "timestamp": b_ts,
                    "close": 95_150.0,
                    "ingested_at": ingested,
                })
    binance = pl.DataFrame(binance_rows).with_columns([
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("close").cast(pl.Float32),
        pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
    ])

    bronze = tmp_path / "bronze"
    bronze.mkdir()
    markets.write_parquet(bronze / f"kalshi_markets_{trade_date}.parquet")
    candles.write_parquet(bronze / f"kalshi_candles_{trade_date}.parquet")
    binance.write_parquet(bronze / "binance_btc_1m.parquet")
    return tmp_path


@pytest.fixture
def surface_df(bronze_surface: Path) -> pl.DataFrame:
    etl = VolSurfaceETL(
        bronze_dir=bronze_surface / "bronze",
        silver_dir=bronze_surface / "silver",
    )
    etl.run()
    return pl.read_parquet(bronze_surface / "silver" / "vol_surface.parquet")


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestVolSurfaceSchema:
    EXPECTED_COLS = {
        "trade_date", "snapshot", "bar_ts", "minutes_to_expiry",
        "digi_contract_name", "strike", "expiry_time",
        "digi_px", "prob_itm", "implied_vol", "btc_close",
    }

    def test_all_columns_present(self, surface_df):
        assert self.EXPECTED_COLS.issubset(set(surface_df.columns))

    def test_bar_ts_is_datetime(self, surface_df):
        assert surface_df["bar_ts"].dtype == pl.Datetime("us", "UTC")

    def test_minutes_to_expiry_is_uint8(self, surface_df):
        assert surface_df["minutes_to_expiry"].dtype == pl.UInt8

    def test_implied_vol_is_float32(self, surface_df):
        assert surface_df["implied_vol"].dtype == pl.Float32

    def test_snapshot_is_categorical(self, surface_df):
        assert surface_df["snapshot"].dtype == pl.Categorical


# ---------------------------------------------------------------------------
# Content tests
# ---------------------------------------------------------------------------

class TestVolSurfaceContent:
    def test_minutes_to_expiry_range(self, surface_df):
        assert surface_df["minutes_to_expiry"].min() >= 0
        assert surface_df["minutes_to_expiry"].max() <= 60

    def test_correct_minutes_to_expiry_values(self, surface_df):
        # Our fixture has bars at expiry-59, expiry-45, expiry-30 → MTX = 59, 45, 30
        mtx = set(surface_df["minutes_to_expiry"].unique().to_list())
        assert mtx == {59, 45, 30}

    def test_implied_vol_positive(self, surface_df):
        assert (surface_df["implied_vol"].drop_nulls() > 0).all()

    def test_no_null_btc_close(self, surface_df):
        assert surface_df["btc_close"].is_null().sum() == 0

    def test_multiple_bars_per_contract(self, surface_df):
        counts = (
            surface_df
            .group_by(["digi_contract_name", "expiry_time"])
            .agg(pl.len().alias("n"))
        )
        assert (counts["n"] >= 2).all()

    def test_three_snapshots_present(self, surface_df):
        snaps = set(surface_df["snapshot"].cast(pl.Utf8).unique().to_list())
        assert snaps == {"T-3", "T0", "T+2"}

    def test_output_file_written(self, surface_df, bronze_surface):
        assert (bronze_surface / "silver" / "vol_surface.parquet").exists()
