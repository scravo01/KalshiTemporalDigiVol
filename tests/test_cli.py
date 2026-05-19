"""Unit and integration tests for Click entrypoints in src/cli/main.py."""
import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from click.testing import CliRunner

from src.cli.main import cli

# ── mock helpers ──────────────────────────────────────────────────────────────

def _mock_kalshi_client(cutoff_ts: int = 9_999_999_999) -> MagicMock:
    client = MagicMock()
    client.historical_cutoff = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
    client.fetch_markets = AsyncMock(return_value=pl.DataFrame(schema={
        "ticker": pl.Categorical,
        "trade_date": pl.Date,
        "strike": pl.UInt32,
        "expiry_time": pl.Datetime("us", "UTC"),
        "status": pl.Categorical,
        "settlement_price": pl.Float32,
        "ingested_at": pl.Datetime("us", "UTC"),
    }))
    client.fetch_candles = AsyncMock(return_value=pl.DataFrame(schema={
        "ticker": pl.Categorical,
        "timestamp": pl.Datetime("us", "UTC"),
        "close": pl.UInt8,
        "volume": pl.UInt32,
        "ingested_at": pl.Datetime("us", "UTC"),
    }))
    return client


def _mock_binance_client() -> MagicMock:
    client = MagicMock()
    client.fetch_klines = AsyncMock(return_value=pl.DataFrame(schema={
        "timestamp": pl.Datetime("us", "UTC"),
        "close": pl.Float32,
        "ingested_at": pl.Datetime("us", "UTC"),
    }))
    return client


# ── Unit: bronze-kalshi ───────────────────────────────────────────────────────

class TestBronzeKalshiUnit:
    def test_missing_api_key_exits_nonzero(self):
        runner = CliRunner(env={})
        result = runner.invoke(cli, ["bronze-kalshi", "--start-date", "2024-01-01", "--end-date", "2024-01-07"])
        assert result.exit_code != 0

    def test_invalid_date_format_exits_nonzero(self):
        runner = CliRunner(env={"KALSHI_API_KEY": "key"})
        result = runner.invoke(cli, ["bronze-kalshi", "--start-date", "01/01/2024", "--end-date", "2024-01-07"])
        assert result.exit_code != 0

    def test_invokes_etl_with_parsed_dates(self, tmp_path):
        runner = CliRunner(env={"KALSHI_API_KEY": "test-key"})
        with patch("src.cli.main.KalshiClient", return_value=_mock_kalshi_client()), \
             patch("src.cli.main.KalshiBronzeETL") as mock_etl_cls:
            mock_etl = MagicMock()
            mock_etl_cls.return_value = mock_etl
            result = runner.invoke(cli, [
                "bronze-kalshi",
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-07",
                "--bronze-dir", str(tmp_path),
            ])
        assert result.exit_code == 0, result.output
        _, kwargs = mock_etl_cls.call_args
        assert kwargs["start_date"] == date(2024, 1, 1)
        assert kwargs["end_date"] == date(2024, 1, 7)
        mock_etl.run.assert_called_once()

    def test_client_receives_api_key(self):
        runner = CliRunner(env={"KALSHI_API_KEY": "secret-key"})
        with patch("src.cli.main.KalshiClient") as mock_cls, \
             patch("src.cli.main.KalshiBronzeETL") as mock_etl_cls:
            mock_cls.return_value = _mock_kalshi_client()
            mock_etl_cls.return_value = MagicMock()
            runner.invoke(cli, ["bronze-kalshi", "--start-date", "2024-01-01", "--end-date", "2024-01-07"])
        mock_cls.assert_called_once_with(api_key="secret-key")


# ── Unit: bronze-binance ──────────────────────────────────────────────────────

class TestBronzeBinanceUnit:
    def test_invokes_etl_with_parsed_dates(self, tmp_path):
        runner = CliRunner()
        with patch("src.cli.main.BinanceClient", return_value=_mock_binance_client()), \
             patch("src.cli.main.BinanceBronzeETL") as mock_etl_cls:
            mock_etl = MagicMock()
            mock_etl_cls.return_value = mock_etl
            result = runner.invoke(cli, [
                "bronze-binance",
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-07",
                "--bronze-dir", str(tmp_path),
            ])
        assert result.exit_code == 0, result.output
        _, kwargs = mock_etl_cls.call_args
        assert kwargs["start_date"] == date(2024, 1, 1)
        assert kwargs["end_date"] == date(2024, 1, 7)
        mock_etl.run.assert_called_once()

    def test_no_api_key_required(self, tmp_path):
        runner = CliRunner(env={})
        with patch("src.cli.main.BinanceClient", return_value=_mock_binance_client()), \
             patch("src.cli.main.BinanceBronzeETL") as mock_etl_cls:
            mock_etl_cls.return_value = MagicMock()
            result = runner.invoke(cli, [
                "bronze-binance", "--start-date", "2024-01-01", "--end-date", "2024-01-07",
                "--bronze-dir", str(tmp_path),
            ])
        assert result.exit_code == 0


# ── Unit: silver ──────────────────────────────────────────────────────────────

class TestSilverUnit:
    def test_invokes_silver_etl(self, tmp_path):
        runner = CliRunner()
        with patch("src.cli.main.SilverETL") as mock_etl_cls:
            mock_etl = MagicMock()
            mock_etl_cls.return_value = mock_etl
            result = runner.invoke(cli, [
                "silver",
                "--bronze-dir", str(tmp_path / "bronze"),
                "--silver-dir", str(tmp_path / "silver"),
            ])
        assert result.exit_code == 0, result.output
        mock_etl.run.assert_called_once()

    def test_dirs_passed_to_etl(self, tmp_path):
        runner = CliRunner()
        with patch("src.cli.main.SilverETL") as mock_etl_cls:
            mock_etl_cls.return_value = MagicMock()
            runner.invoke(cli, [
                "silver",
                "--bronze-dir", str(tmp_path / "bronze"),
                "--silver-dir", str(tmp_path / "silver"),
            ])
        _, kwargs = mock_etl_cls.call_args
        assert kwargs["bronze_dir"] == tmp_path / "bronze"
        assert kwargs["silver_dir"] == tmp_path / "silver"

    def test_silver_has_no_required_options(self, tmp_path):
        # Runs without any options (uses defaults) — should not exit with usage error
        runner = CliRunner()
        with patch("src.cli.main.SilverETL") as mock_etl_cls:
            mock_etl_cls.return_value = MagicMock()
            result = runner.invoke(cli, ["silver"])
        assert result.exit_code == 0


# ── Integration: bronze-kalshi ────────────────────────────────────────────────

class TestBronzeKalshiIntegration:
    """Real ETL logic with a mock client — verifies parquet output."""

    def _run(self, tmp_path: Path) -> Path:
        from src.etl.bronze.kalshi_bronze import KalshiBronzeETL
        bronze = tmp_path / "bronze"
        bronze.mkdir()

        snap_ts = [
            datetime(2024, 5, 24, 23, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 25, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 25, 1, 0, 0, tzinfo=timezone.utc),
        ]
        ingested = datetime.now(timezone.utc)
        markets_df = pl.DataFrame({
            "ticker": ["KXBTCD-25MAY24-B95000"],
            "trade_date": [date(2024, 5, 25)],
            "strike": [95_000],
            "expiry_time": [datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)],
            "status": ["settled"],
            "settlement_price": [None],
            "ingested_at": [ingested],
        }).with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("trade_date").cast(pl.Date),
            pl.col("strike").cast(pl.UInt32),
            pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
            pl.col("status").cast(pl.Categorical),
            pl.col("settlement_price").cast(pl.Float32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])
        candles_df = pl.DataFrame({
            "ticker": ["KXBTCD-25MAY24-B95000"] * 3,
            "timestamp": snap_ts,
            "close": [50, 51, 52],
            "volume": [100, 110, 120],
            "ingested_at": [ingested] * 3,
        }).with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("close").cast(pl.UInt8),
            pl.col("volume").cast(pl.UInt32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])

        client = MagicMock()
        client.historical_cutoff = datetime(2317, 1, 1, tzinfo=timezone.utc)
        client.fetch_markets = AsyncMock(return_value=markets_df)
        client.fetch_candles = AsyncMock(return_value=candles_df)

        etl = KalshiBronzeETL(client=client, start_date=date(2024, 5, 25),
                               end_date=date(2024, 5, 25), bronze_dir=bronze)
        etl.run()
        return bronze

    def test_writes_markets_parquet(self, tmp_path):
        bronze = self._run(tmp_path)
        assert (bronze / "kalshi_markets.parquet").exists()

    def test_writes_candles_parquet(self, tmp_path):
        bronze = self._run(tmp_path)
        assert (bronze / "kalshi_candles.parquet").exists()

    def test_markets_schema(self, tmp_path):
        bronze = self._run(tmp_path)
        df = pl.read_parquet(bronze / "kalshi_markets.parquet")
        assert df["trade_date"].dtype == pl.Date
        assert df["strike"].dtype == pl.UInt32
        assert df["expiry_time"].dtype == pl.Datetime("us", "UTC")

    def test_candles_no_zero_volume(self, tmp_path):
        bronze = self._run(tmp_path)
        df = pl.read_parquet(bronze / "kalshi_candles.parquet")
        assert (df["volume"] > 0).all()
        assert df["close"].dtype == pl.UInt8


# ── Integration: bronze-binance ───────────────────────────────────────────────

class TestBronzeBinanceIntegration:
    def _run(self, tmp_path: Path) -> Path:
        from src.etl.bronze.binance_bronze import BinanceBronzeETL
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        snap_ts = [
            datetime(2024, 5, 24, 23, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 25, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 25, 1, 0, 0, tzinfo=timezone.utc),
        ]
        klines_df = pl.DataFrame({
            "timestamp": snap_ts,
            "close": [95_000.0, 95_100.0, 95_200.0],
            "ingested_at": [datetime.now(timezone.utc)] * 3,
        }).with_columns([
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("close").cast(pl.Float32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])
        client = MagicMock()
        client.fetch_klines = AsyncMock(return_value=klines_df)
        etl = BinanceBronzeETL(client=client, start_date=date(2024, 5, 25),
                                end_date=date(2024, 5, 25), bronze_dir=bronze)
        etl.run()
        return bronze

    def test_writes_klines_parquet(self, tmp_path):
        bronze = self._run(tmp_path)
        assert (bronze / "binance_btc_1m.parquet").exists()

    def test_klines_schema(self, tmp_path):
        bronze = self._run(tmp_path)
        df = pl.read_parquet(bronze / "binance_btc_1m.parquet")
        assert df["timestamp"].dtype == pl.Datetime("us", "UTC")
        assert df["close"].dtype == pl.Float32
        assert len(df) == 3


# ── Integration: silver ───────────────────────────────────────────────────────

class TestSilverIntegration:
    """CLI invocation of silver command against synthetic bronze fixtures."""

    def test_writes_silver_parquet(self, bronze_dir):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "silver",
            "--bronze-dir", str(bronze_dir / "bronze"),
            "--silver-dir", str(bronze_dir / "silver"),
        ])
        assert result.exit_code == 0, result.output
        assert (bronze_dir / "silver" / "contracts.parquet").exists()

    def test_three_snapshots_present(self, bronze_dir):
        runner = CliRunner()
        runner.invoke(cli, [
            "silver",
            "--bronze-dir", str(bronze_dir / "bronze"),
            "--silver-dir", str(bronze_dir / "silver"),
        ])
        df = pl.read_parquet(bronze_dir / "silver" / "contracts.parquet")
        snaps = set(df["snapshot"].cast(pl.Utf8).unique().to_list())
        assert snaps == {"T-1", "T0", "T+1"}

    def test_implied_vol_all_positive(self, bronze_dir):
        runner = CliRunner()
        runner.invoke(cli, [
            "silver",
            "--bronze-dir", str(bronze_dir / "bronze"),
            "--silver-dir", str(bronze_dir / "silver"),
        ])
        df = pl.read_parquet(bronze_dir / "silver" / "contracts.parquet")
        assert (df["implied_vol"].drop_nulls() > 0).all()
