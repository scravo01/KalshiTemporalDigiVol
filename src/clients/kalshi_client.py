import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

import polars as pl
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


class KalshiClient:
    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.historical_cutoff: datetime = self._fetch_historical_cutoff()
        logger.info(f"Kalshi historical cutoff: {self.historical_cutoff}")

    def _fetch_historical_cutoff(self) -> datetime:
        resp = self.session.get(f"{BASE_URL}/historical/cutoff", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        ts = data.get("market_settled_ts")
        if ts is None:
            ts = data.get("cutoff")
        if ts is None:
            raise RuntimeError(f"Unexpected /historical/cutoff response: {data}")
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        resp = self.session.get(f"{BASE_URL}{path}", params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _is_historical(self, trade_date: date) -> bool:
        dt = datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc)
        return dt < self.historical_cutoff

    def fetch_markets(self, start_date: date, end_date: date) -> pl.DataFrame:
        rows = []
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)

        start_dt_utc = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        end_dt_utc = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

        for use_historical in (True, False):
            # Skip branch if the date range doesn't overlap with it
            if use_historical and start_dt_utc >= self.historical_cutoff:
                continue
            if not use_historical and end_dt_utc < self.historical_cutoff:
                continue

            path = "/historical/markets" if use_historical else "/markets"
            params: dict = {"series_ticker": "KXBTCD", "limit": 200}
            if use_historical:
                params["max_close_ts"] = int(self.historical_cutoff.timestamp())
                params["min_close_ts"] = int(start_dt_utc.timestamp())
            else:
                params["min_close_ts"] = int(self.historical_cutoff.timestamp())
                params["max_close_ts"] = int(end_dt_utc.timestamp())

            while True:
                data = self._get(path, params)
                markets = data.get("markets", [])
                for m in markets:
                    ticker = m.get("ticker", "")
                    if not ticker.startswith("KXBTCD"):
                        continue
                    strike = _parse_strike(ticker)
                    if strike is None:
                        continue
                    trade_dt = _parse_trade_date(ticker)
                    if trade_dt is None or not (start_date <= trade_dt <= end_date):
                        continue
                    expiry_raw = m.get("close_time") or m.get("expiration_time") or m.get("expected_expiration_ts")
                    expiry_time = _parse_timestamp(expiry_raw)
                    settlement_raw = m.get("result") or m.get("settlement_value_dollars")
                    settlement_price = float(settlement_raw) if settlement_raw not in (None, "", "unknown") else None
                    rows.append({
                        "ticker": ticker,
                        "trade_date": trade_dt,
                        "strike": strike,
                        "expiry_time": expiry_time,
                        "status": m.get("status", "unknown"),
                        "settlement_price": settlement_price,
                        "ingested_at": ingested_at,
                    })

                cursor = data.get("cursor")
                if not cursor:
                    break
                params["cursor"] = cursor

        if not rows:
            logger.warning("fetch_markets returned 0 rows")
            return _empty_markets_df()

        df = pl.DataFrame(rows).with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("trade_date").cast(pl.Date),
            pl.col("strike").cast(pl.UInt32),
            pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
            pl.col("status").cast(pl.Categorical),
            pl.col("settlement_price").cast(pl.Float32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])
        logger.info(f"fetch_markets: {len(df)} rows")
        return df

    def fetch_candles(
        self,
        tickers: list[str],
        start_ts: datetime,
        end_ts: datetime,
        is_historical: bool,
    ) -> pl.DataFrame:
        path = "/historical/market-candlesticks" if is_historical else "/markets/candlesticks"
        params = {
            "tickers": ",".join(tickers),
            "start_ts": int(start_ts.timestamp()),
            "end_ts": int(end_ts.timestamp()),
            "period_interval": 1,
        }
        data = self._get(path, params)

        # Response shape: {"candles": {"TICKER": [{"end_period_ts": ..., "yes_bid": {...}, "price": {...}, ...}]}}
        # or {"markets_candles": [{"ticker": "...", "candles": [...]}]}
        # Handle both shapes defensively.
        candles_by_ticker: dict = {}
        if "candles" in data and isinstance(data["candles"], dict):
            candles_by_ticker = data["candles"]
        elif "markets_candles" in data:
            for entry in data["markets_candles"]:
                candles_by_ticker[entry["ticker"]] = entry.get("candles", [])
        else:
            raise KeyError(
                f"Unexpected candle response shape. Keys: {list(data.keys())}. "
                "Expected 'candles' (dict) or 'markets_candles' (list)."
            )

        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []
        for ticker, candles in candles_by_ticker.items():
            for c in candles:
                # Timestamp: try end_period_ts, ts, open_period_ts
                ts_raw = c.get("end_period_ts") or c.get("ts") or c.get("open_period_ts")
                if ts_raw is None:
                    continue
                ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)

                # Close price: prefer trade close, fall back to yes_ask close, then yes_bid close
                price_block = c.get("price") or {}
                ask_block = c.get("yes_ask") or {}
                bid_block = c.get("yes_bid") or {}
                close_raw = (
                    price_block.get("close")
                    or ask_block.get("close")
                    or bid_block.get("close")
                )
                if close_raw is None:
                    continue
                close_cents = int(round(float(close_raw)))
                if not (0 <= close_cents <= 100):
                    continue

                volume_raw = c.get("volume") or c.get("volume_fp") or 0
                volume = int(float(volume_raw))

                rows.append({
                    "ticker": ticker,
                    "timestamp": ts,
                    "close": close_cents,
                    "volume": volume,
                    "ingested_at": ingested_at,
                })

        if not rows:
            return _empty_candles_df()

        df = pl.DataFrame(rows)
        n_before = len(df)
        df = df.filter(pl.col("volume") > 0)
        n_dropped = n_before - len(df)
        if n_dropped:
            logger.debug(f"Dropped {n_dropped} zero-volume candle rows from batch of {len(tickers)} tickers")

        return df.with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("close").cast(pl.UInt8),
            pl.col("volume").cast(pl.UInt32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])


# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_strike(ticker: str) -> Optional[int]:
    # KXBTCD-25MAY24-B95000  or  KXBTCD-2024-05-25-B95000
    try:
        part = ticker.split("-B")[-1]
        return int(part.split("-")[0])
    except (IndexError, ValueError):
        return None


def _parse_trade_date(ticker: str) -> Optional[date]:
    # Try to extract the date segment between first and last '-B'
    try:
        date_part = ticker.split("-B")[0].split("-", 1)[1]  # strip "KXBTCD-"
        # Try ISO format first (2024-05-25)
        try:
            return date.fromisoformat(date_part)
        except ValueError:
            pass
        # Try shorthand like 25MAY24
        return datetime.strptime(date_part, "%d%b%y").date()
    except Exception:
        return None


def _parse_timestamp(raw) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None


def _empty_markets_df() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "ticker": pl.Categorical,
        "trade_date": pl.Date,
        "strike": pl.UInt32,
        "expiry_time": pl.Datetime("us", "UTC"),
        "status": pl.Categorical,
        "settlement_price": pl.Float32,
        "ingested_at": pl.Datetime("us", "UTC"),
    })


def _empty_candles_df() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "ticker": pl.Categorical,
        "timestamp": pl.Datetime("us", "UTC"),
        "close": pl.UInt8,
        "volume": pl.UInt32,
        "ingested_at": pl.Datetime("us", "UTC"),
    })
