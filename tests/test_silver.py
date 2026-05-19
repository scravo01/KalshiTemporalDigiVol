from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

# ── fixture: synthetic bronze parquets ───────────────────────────────────────

@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    trade_date = date(2024, 5, 25)
    ingested = datetime(2024, 5, 26, 0, 0, 0, tzinfo=timezone.utc)

    # kalshi_markets
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

    # snapshot timestamps for all three contracts × three snapshots
    snap_ts = [
        datetime(2024, 5, 24, 23, 0, 0, tzinfo=timezone.utc),  # T-1
        datetime(2024, 5, 25, 0, 0, 0, tzinfo=timezone.utc),   # T0
        datetime(2024, 5, 25, 1, 0, 0, tzinfo=timezone.utc),   # T+1
    ]
    ticker_list, ts_list, close_list, vol_list = [], [], [], []
    for ticker, close_base in [("KXBTCD-25MAY24-B95000", 50),
                                ("KXBTCD-25MAY24-B95500", 38),
                                ("KXBTCD-25MAY24-B94500", 63)]:
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

    # binance klines — one row per snapshot timestamp
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


@pytest.fixture
def silver_df(bronze_dir: Path, monkeypatch) -> pl.DataFrame:
    import src.etl.silver.silver_etl as etl
    monkeypatch.setattr(etl, "BRONZE_DIR", bronze_dir / "bronze")
    monkeypatch.setattr(etl, "SILVER_DIR", bronze_dir / "silver")
    monkeypatch.setattr(etl, "SILVER_PATH", bronze_dir / "silver" / "contracts.parquet")
    etl.run()
    return pl.read_parquet(bronze_dir / "silver" / "contracts.parquet")


# ── tests ─────────────────────────────────────────────────────────────────────

class TestSilverSchema:
    EXPECTED_COLS = {
        "trade_date", "snapshot", "snapshot_ts", "digi_contract_name",
        "strike", "expiry_time", "digi_px", "delta", "implied_vol",
        "btc_close", "volume",
    }

    def test_all_columns_present(self, silver_df):
        assert self.EXPECTED_COLS.issubset(set(silver_df.columns))

    def test_snapshot_is_categorical(self, silver_df):
        assert silver_df["snapshot"].dtype == pl.Categorical

    def test_digi_contract_name_is_categorical(self, silver_df):
        assert silver_df["digi_contract_name"].dtype == pl.Categorical

    def test_strike_is_uint32(self, silver_df):
        assert silver_df["strike"].dtype == pl.UInt32

    def test_digi_px_is_uint8(self, silver_df):
        assert silver_df["digi_px"].dtype == pl.UInt8

    def test_expiry_time_is_datetime(self, silver_df):
        assert silver_df["expiry_time"].dtype == pl.Datetime("us", "UTC")

    def test_delta_is_float32(self, silver_df):
        assert silver_df["delta"].dtype == pl.Float32


class TestSilverContent:
    def test_three_snapshots_produced(self, silver_df):
        snaps = set(silver_df["snapshot"].cast(pl.Utf8).unique().to_list())
        assert snaps == {"T-1", "T0", "T+1"}

    def test_delta_values_in_range(self, silver_df):
        assert (silver_df["delta"] >= 0.02).all()
        assert (silver_df["delta"] <= 0.98).all()

    def test_delta_equals_digi_px_over_100(self, silver_df):
        expected = silver_df["digi_px"].cast(pl.Float32) / 100.0
        diff = (silver_df["delta"] - expected).abs()
        assert (diff < 1e-4).all()

    def test_btc_close_matches_binance(self, silver_df):
        t0 = silver_df.filter(pl.col("snapshot").cast(pl.Utf8) == "T0")
        assert (t0["btc_close"] - 95_100.0).abs().max() < 0.1

    def test_no_zero_volume_rows(self, silver_df):
        assert (silver_df["volume"] > 0).all()

    def test_implied_vol_positive(self, silver_df):
        non_null = silver_df["implied_vol"].drop_nulls()
        assert (non_null > 0).all()

    def test_three_contracts_per_snapshot(self, silver_df):
        counts = (
            silver_df.group_by("snapshot")
            .agg(pl.len().alias("n"))
        )
        assert (counts["n"] == 3).all()
