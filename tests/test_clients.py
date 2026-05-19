from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

# ── KalshiClient ──────────────────────────────────────────────────────────────

def _make_kalshi_client(cutoff_ts: int = 1_700_000_000):
    """Return a KalshiClient with mocked HTTP for the cutoff call."""
    from src.clients.kalshi_client import KalshiClient

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"market_settled_ts": cutoff_ts}
    mock_resp.raise_for_status = MagicMock()

    with patch("requests.Session.get", return_value=mock_resp):
        client = KalshiClient(api_key="test-key")
    return client


class TestKalshiClientInit:
    def test_cutoff_parsed_as_utc_datetime(self):
        client = _make_kalshi_client(cutoff_ts=1_700_000_000)
        expected = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
        assert client.historical_cutoff == expected

    def test_is_historical_before_cutoff(self):
        client = _make_kalshi_client(cutoff_ts=1_700_000_000)
        old_date = date(2020, 1, 1)
        assert client._is_historical(old_date) is True

    def test_is_historical_after_cutoff(self):
        client = _make_kalshi_client(cutoff_ts=1_700_000_000)
        future_date = date(2099, 1, 1)
        assert client._is_historical(future_date) is False


class TestKalshiClientFetchMarkets:
    _TICKER = "KXBTCD-25MAY24-B95000"
    _MARKET = {
        "ticker": _TICKER,
        "close_time": "2024-05-25T21:00:00Z",
        "status": "settled",
        "result": "0.5",
    }

    def _client_with_market_response(self, pages):
        client = _make_kalshi_client(cutoff_ts=0)  # all dates → live
        responses = [{"markets": page_markets, "cursor": cursor}
                     for page_markets, cursor in pages]
        client._get = MagicMock(side_effect=responses)
        return client

    def test_returns_correct_schema(self):
        client = self._client_with_market_response([([self._MARKET], None)])
        df = client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25))
        assert set(["ticker", "strike", "trade_date", "expiry_time", "status"]).issubset(df.columns)
        assert df["strike"].dtype == pl.UInt32
        assert df["trade_date"].dtype == pl.Date

    def test_strike_parsed_from_ticker(self):
        client = self._client_with_market_response([([self._MARKET], None)])
        df = client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25))
        assert df["strike"][0] == 95_000

    def test_pagination_follows_cursor(self):
        market2 = {**self._MARKET, "ticker": "KXBTCD-25MAY24-B95500"}
        client = self._client_with_market_response([
            ([self._MARKET], "cursor_abc"),
            ([market2], None),
        ])
        df = client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25))
        assert client._get.call_count == 2
        assert len(df) == 2

    def test_non_kxbtcd_tickers_excluded(self):
        other = {**self._MARKET, "ticker": "INXU-25MAY24-B4500"}
        client = self._client_with_market_response([([other, self._MARKET], None)])
        df = client.fetch_markets(date(2024, 5, 25), date(2024, 5, 25))
        assert len(df) == 1
        assert df["ticker"].cast(pl.Utf8)[0] == self._TICKER


class TestKalshiClientFetchCandles:
    _CANDLE_RESPONSE = {
        "candles": {
            "KXBTCD-25MAY24-B95000": [
                {"end_period_ts": 1_716_681_600, "price": {"close": 45}, "volume": 100},
                {"end_period_ts": 1_716_681_660, "price": {"close": 50}, "volume": 0},   # zero-vol
                {"end_period_ts": 1_716_681_720, "price": {"close": 55}, "volume": 200},
            ]
        }
    }

    def test_zero_volume_rows_dropped(self):
        from src.clients.kalshi_client import KalshiClient
        client = _make_kalshi_client()
        client._get = MagicMock(return_value=self._CANDLE_RESPONSE)
        start = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)
        df = client.fetch_candles(["KXBTCD-25MAY24-B95000"], start, end, is_historical=False)
        assert (df["volume"] > 0).all()
        assert len(df) == 2  # zero-vol row dropped

    def test_close_dtype_is_uint8(self):
        from src.clients.kalshi_client import KalshiClient
        client = _make_kalshi_client()
        client._get = MagicMock(return_value=self._CANDLE_RESPONSE)
        start = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 25, 21, 0, tzinfo=timezone.utc)
        df = client.fetch_candles(["KXBTCD-25MAY24-B95000"], start, end, is_historical=False)
        assert df["close"].dtype == pl.UInt8

    def test_unexpected_response_shape_raises(self):
        from src.clients.kalshi_client import KalshiClient
        client = _make_kalshi_client()
        client._get = MagicMock(return_value={"unexpected_key": []})
        with pytest.raises(KeyError):
            client.fetch_candles(["KXBTCD-25MAY24-B95000"],
                                 datetime(2024, 5, 25, tzinfo=timezone.utc),
                                 datetime(2024, 5, 25, 21, tzinfo=timezone.utc),
                                 is_historical=False)


# ── BinanceClient ─────────────────────────────────────────────────────────────

def _make_kline(open_time_ms: int, close: float):
    return [open_time_ms, "0", "0", "0", str(close), "0", open_time_ms + 59_999, "0", 0, "0", "0", "0"]


class TestBinanceClient:
    def test_schema(self):
        from src.clients.binance_client import BinanceClient
        kline = _make_kline(1_700_000_000_000, 50_000.0)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = [kline]

        with patch("requests.Session.get", return_value=mock_resp):
            client = BinanceClient()
            start = datetime(2023, 11, 14, 23, 0, tzinfo=timezone.utc)
            end = datetime(2023, 11, 14, 23, 2, tzinfo=timezone.utc)
            df = client.fetch_klines(start, end)

        assert df["close"].dtype == pl.Float32
        assert df["timestamp"].dtype == pl.Datetime("us", "UTC")
        assert abs(df["close"][0] - 50_000.0) < 0.1

    def test_pagination_stops_on_short_page(self):
        from src.clients.binance_client import BinanceClient
        # 999 bars → single page, loop stops
        klines = [_make_kline(1_700_000_000_000 + i * 60_000, 50_000.0) for i in range(999)]
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = klines

        with patch("requests.Session.get", return_value=mock_resp) as mock_get:
            client = BinanceClient()
            start = datetime(2023, 11, 14, 0, 0, tzinfo=timezone.utc)
            end = datetime(2023, 11, 14, 23, 59, tzinfo=timezone.utc)
            client.fetch_klines(start, end)
            assert mock_get.call_count == 1

    def test_empty_response_returns_empty_df(self):
        from src.clients.binance_client import BinanceClient
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = []

        with patch("requests.Session.get", return_value=mock_resp):
            client = BinanceClient()
            df = client.fetch_klines(
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 1, 2, tzinfo=timezone.utc),
            )
        assert df.is_empty()
