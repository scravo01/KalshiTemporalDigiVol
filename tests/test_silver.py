from pathlib import Path

import polars as pl
import pytest

from src.etl.silver.silver_etl import SilverETL


@pytest.fixture
def silver_df(bronze_dir: Path) -> pl.DataFrame:
    """Run SilverETL against synthetic bronze fixtures; return the output DataFrame."""
    etl = SilverETL(bronze_dir=bronze_dir / "bronze", silver_dir=bronze_dir / "silver")
    etl.run()
    return pl.read_parquet(bronze_dir / "silver" / "contracts.parquet")


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
        assert ((silver_df["delta"] - expected).abs() < 1e-4).all()

    def test_btc_close_matches_binance(self, silver_df):
        t0 = silver_df.filter(pl.col("snapshot").cast(pl.Utf8) == "T0")
        assert (t0["btc_close"] - 95_100.0).abs().max() < 0.1

    def test_no_zero_volume_rows(self, silver_df):
        assert (silver_df["volume"] > 0).all()

    def test_implied_vol_positive(self, silver_df):
        non_null = silver_df["implied_vol"].drop_nulls()
        assert (non_null > 0).all()

    def test_three_contracts_per_snapshot(self, silver_df):
        counts = silver_df.group_by("snapshot").agg(pl.len().alias("n"))
        assert (counts["n"] == 3).all()
