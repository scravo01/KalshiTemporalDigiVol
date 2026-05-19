"""Tests for async Kalshi and Binance clients (HTTP mocked with aioresponses)."""
import asyncio
import re
from datetime import date, datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest
from aioresponses import aioresponses
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from src.clients.kalshi_client import BASE_URL as KALSHI_BASE, KalshiClient
from src.clients.binance_client import BASE_URL as BINANCE_BASE, BinanceClient

# Generate a test RSA-2048 key once for all tests
_TEST_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_TEST_PEM = _TEST_PRIVATE_KEY.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption(),
).decode()

# ── URL patterns (regex) — aioresponses includes query params in the match key ─

_CUTOFF_RE   = re.compile(rf"{re.escape(KALSHI_BASE)}/historical/cutoff.*")
_MARKETS_RE  = re.compile(rf"{re.escape(KALSHI_BASE)}/historical/markets.*")
_CANDLES_RE  = re.compile(rf"{re.escape(KALSHI_BASE)}/markets/candlesticks.*")
_KLINES_RE   = re.compile(rf"{re.escape(BINANCE_BASE)}/api/v3/klines.*")
# Note: BASE_URL is imported so this regex auto-updates if the URL changes

_MARKET = {
    "ticker": "KXBTCD-25MAY24-B95000",
    "close_time": "2024-05-25T21:00:00Z",
    "status": "settled",
    "result": "0.5",
}

_CANDLE_RESPONSE = {
    "candles": {
        "KXBTCD-25MAY24-B95000": [
            {"end_period_ts": 1_716_681_600, "price": {"close": 45}, "volume": 100},
            {"end_period_ts": 1_716_681_660, "price": {"close": 50}, "volume": 0},   # zero-vol
            {"end_period_ts": 1_716_681_720, "price": {"close": 55}, "volume": 200},
        ]
    }
}


def _make_client(cutoff_ts: int = 9_999_999_999) -> KalshiClient:
    """Construct KalshiClient with mocked cutoff HTTP call."""
    with aioresponses() as m:
        m.get(_CUTOFF_RE, payload={"market_settled_ts": cutoff_ts})
        with patch.dict("os.environ", {"KEY_ID": "test-key-id"}):
            return KalshiClient(api_key=_TEST_PEM)


# ── KalshiClient init ─────────────────────────────────────────────────────────

class TestKalshiClientInit:
    def test_cutoff_parsed_as_utc_datetime(self):
        with aioresponses() as m:
            m.get(_CUTOFF_RE, payload={"market_settled_ts": 1_700_000_000})
            with patch.dict("os.environ", {"KEY_ID": "test-key-id"}):
                client = KalshiClient(api_key=_TEST_PEM)
        assert client.historical_cutoff == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

    def test_is_historical_before_cutoff(self):
        client = _make_client(cutoff_ts=1_700_000_000)
        assert client._is_historical(date(2020, 1, 1)) is True

    def test_is_historical_after_cutoff(self):
        client = _make_client(cutoff_ts=1_700_000_000)
        assert client._is_historical(date(2099, 1, 1)) is False


# ── KalshiClient.fetch_markets ────────────────────────────────────────────────

class TestKalshiClientFetchMarkets:
    def test_returns_correct_schema(self):
        client = _make_client()  # far-future cutoff → historical branch
        with aioresponses() as m:
            m.get(_MARKETS_RE, payload={"markets": [_MARKET], "cursor": None})
            df = asyncio.run(client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25)))
        assert {"ticker", "strike", "trade_date", "expiry_time", "status"}.issubset(df.columns)
        assert df["strike"].dtype == pl.UInt32
        assert df["trade_date"].dtype == pl.Date

    def test_strike_parsed_from_ticker(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(_MARKETS_RE, payload={"markets": [_MARKET], "cursor": None})
            df = asyncio.run(client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25)))
        assert df["strike"][0] == 95_000

    def test_pagination_follows_cursor(self):
        market2 = {**_MARKET, "ticker": "KXBTCD-25MAY24-B95500"}
        client = _make_client()
        with aioresponses() as m:
            m.get(_MARKETS_RE, payload={"markets": [_MARKET], "cursor": "abc"})
            m.get(_MARKETS_RE, payload={"markets": [market2], "cursor": None})
            df = asyncio.run(client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25)))
        assert len(df) == 2

    def test_non_kxbtcd_tickers_excluded(self):
        other = {**_MARKET, "ticker": "INXU-25MAY24-B4500"}
        client = _make_client()
        with aioresponses() as m:
            m.get(_MARKETS_RE, payload={"markets": [other, _MARKET], "cursor": None})
            df = asyncio.run(client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25)))
        assert len(df) == 1
        assert df["ticker"].cast(pl.Utf8)[0] == _MARKET["ticker"]


# ── KalshiClient.fetch_candles ────────────────────────────────────────────────

class TestKalshiClientFetchCandles:
    def test_zero_volume_rows_dropped(self):
        client = _make_client()
        start = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)
        with aioresponses() as m:
            m.get(_CANDLES_RE, payload=_CANDLE_RESPONSE)
            df = asyncio.run(client.fetch_candles(
                ["KXBTCD-25MAY24-B95000"], start, end, is_historical=True
            ))
        assert (df["volume"] > 0).all()
        assert len(df) == 2  # zero-vol row dropped

    def test_close_dtype_is_uint8(self):
        client = _make_client()
        start = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)
        with aioresponses() as m:
            m.get(_CANDLES_RE, payload=_CANDLE_RESPONSE)
            df = asyncio.run(client.fetch_candles(
                ["KXBTCD-25MAY24-B95000"], start, end, is_historical=True
            ))
        assert df["close"].dtype == pl.UInt8

    def test_unexpected_response_shape_raises(self):
        client = _make_client()
        start = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)
        with aioresponses() as m:
            m.get(_CANDLES_RE, payload={"unexpected_key": []})
            with pytest.raises(KeyError):
                asyncio.run(client.fetch_candles(
                    ["KXBTCD-25MAY24-B95000"], start, end, is_historical=True
                ))


# ── BinanceClient ─────────────────────────────────────────────────────────────

def _kline(open_time_ms: int, close: float) -> list:
    return [open_time_ms, "0", "0", "0", str(close), "0", open_time_ms + 59_999, "0", 0, "0", "0", "0"]




class TestBinanceClient:
    def test_schema(self):
        klines = [_kline(1_700_000_000_000, 50_000.0)]
        with aioresponses() as m:
            m.get(_KLINES_RE, payload=klines)
            df = asyncio.run(BinanceClient().fetch_klines(
                datetime(2023, 11, 14, 23, 0, tzinfo=timezone.utc),
                datetime(2023, 11, 14, 23, 2, tzinfo=timezone.utc),
            ))
        assert df["close"].dtype == pl.Float32
        assert df["timestamp"].dtype == pl.Datetime("us", "UTC")
        assert abs(df["close"][0] - 50_000.0) < 0.1

    def test_pagination_stops_on_short_page(self):
        klines = [_kline(1_700_000_000_000 + i * 60_000, 50_000.0) for i in range(999)]
        with aioresponses() as m:
            m.get(_KLINES_RE, payload=klines)
            asyncio.run(BinanceClient().fetch_klines(
                datetime(2023, 11, 14, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 11, 14, 23, 59, tzinfo=timezone.utc),
            ))
        # aioresponses raises ConnectionError if an unexpected extra request is made
        # — if pagination stopped correctly, no extra request was sent

    def test_empty_response_returns_empty_df(self):
        with aioresponses() as m:
            m.get(_KLINES_RE, payload=[])
            df = asyncio.run(BinanceClient().fetch_klines(
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 1, 2, tzinfo=timezone.utc),
            ))
        assert df.is_empty()
