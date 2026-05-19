import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Optional

import aiohttp
import polars as pl
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm as atqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
BATCH_SIZE = 7          # max tickers per candle request (10k candles / ~1 260 per ticker)
CONCURRENCY = 5         # parallel candle requests
RATE_LIMIT = 20         # requests per second
_TIMEOUT = aiohttp.ClientTimeout(total=30)


class KalshiClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.historical_cutoff: datetime = asyncio.run(self._fetch_historical_cutoff())
        logger.info("Kalshi historical cutoff: %s", self.historical_cutoff)

    # ── internal helpers ──────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def _get(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: Optional[dict] = None,
        limiter: Optional[AsyncLimiter] = None,
    ) -> dict:
        if limiter:
            async with limiter:
                pass  # acquire token before request
        async with session.get(
            f"{BASE_URL}{path}", params=params or {}, timeout=_TIMEOUT
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ── cutoff (called once at init) ──────────────────────────────────────────

    async def _fetch_historical_cutoff(self) -> datetime:
        async with aiohttp.ClientSession(headers=self._headers(), timeout=_TIMEOUT) as session:
            data = await self._get(session, "/historical/cutoff")
        ts = data.get("market_settled_ts")
        if ts is None:
            ts = data.get("cutoff")
        if ts is None:
            raise RuntimeError(f"Unexpected /historical/cutoff response: {data}")
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

    # ── public API ────────────────────────────────────────────────────────────

    def _is_historical(self, trade_date: date) -> bool:
        dt = datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc)
        return dt < self.historical_cutoff

    async def fetch_markets(self, start_date: date, end_date: date) -> pl.DataFrame:
        rows: list[dict] = []
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
        start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

        async with aiohttp.ClientSession(headers=self._headers(), timeout=_TIMEOUT) as session:
            limiter = AsyncLimiter(RATE_LIMIT, 1.0)
            for use_historical in (True, False):
                if use_historical and start_dt >= self.historical_cutoff:
                    continue
                if not use_historical and end_dt < self.historical_cutoff:
                    continue

                path = "/historical/markets" if use_historical else "/markets"
                params: dict = {"series_ticker": "KXBTCD", "limit": 200}
                if use_historical:
                    params["max_close_ts"] = int(self.historical_cutoff.timestamp())
                    params["min_close_ts"] = int(start_dt.timestamp())
                else:
                    params["min_close_ts"] = int(self.historical_cutoff.timestamp())
                    params["max_close_ts"] = int(end_dt.timestamp())

                while True:
                    async with limiter:
                        data = await self._get(session, path, params)
                    for m in data.get("markets", []):
                        ticker = m.get("ticker", "")
                        if not ticker.startswith("KXBTCD"):
                            continue
                        strike = _parse_strike(ticker)
                        if strike is None:
                            continue
                        trade_dt = _parse_trade_date(ticker)
                        if trade_dt is None or not (start_date <= trade_dt <= end_date):
                            continue
                        expiry_raw = (
                            m.get("close_time")
                            or m.get("expiration_time")
                            or m.get("expected_expiration_ts")
                        )
                        sr = m.get("result") or m.get("settlement_value_dollars")
                        rows.append({
                            "ticker": ticker,
                            "trade_date": trade_dt,
                            "strike": strike,
                            "expiry_time": _parse_timestamp(expiry_raw),
                            "status": m.get("status", "unknown"),
                            "settlement_price": float(sr) if sr not in (None, "", "unknown") else None,
                            "ingested_at": ingested_at,
                        })
                    cursor = data.get("cursor")
                    if not cursor:
                        break
                    params["cursor"] = cursor

        if not rows:
            logger.warning("fetch_markets returned 0 rows")
            return _empty_markets_df()

        return pl.DataFrame(rows).with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("trade_date").cast(pl.Date),
            pl.col("strike").cast(pl.UInt32),
            pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
            pl.col("status").cast(pl.Categorical),
            pl.col("settlement_price").cast(pl.Float32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])

    async def fetch_candles(
        self,
        tickers: list[str],
        start_ts: datetime,
        end_ts: datetime,
        is_historical: bool,
    ) -> pl.DataFrame:
        """Fetch candles for all tickers, running up to CONCURRENCY requests in parallel."""
        path = "/historical/market-candlesticks" if is_historical else "/markets/candlesticks"
        base_params = {
            "start_ts": int(start_ts.timestamp()),
            "end_ts": int(end_ts.timestamp()),
            "period_interval": 1,
        }
        batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)

        sem = asyncio.Semaphore(CONCURRENCY)
        limiter = AsyncLimiter(RATE_LIMIT, 1.0)

        async with aiohttp.ClientSession(headers=self._headers(), timeout=_TIMEOUT) as session:
            async def _fetch_batch(batch: list[str]) -> list[dict]:
                async with sem:
                    async with limiter:
                        data = await self._get(
                            session, path, {**base_params, "tickers": ",".join(batch)}
                        )
                return _parse_candle_response(data, ingested_at)

            label = "historical" if is_historical else "live"
            all_rows: list[list[dict]] = await atqdm.gather(
                *[_fetch_batch(b) for b in batches],
                desc=f"Kalshi candles ({label})",
            )

        rows = [r for batch_rows in all_rows for r in batch_rows]
        if not rows:
            return _empty_candles_df()

        df = pl.DataFrame(rows)
        n_before = len(df)
        df = df.filter(pl.col("volume") > 0)
        dropped = n_before - len(df)
        if dropped:
            logger.debug("Dropped %d zero-volume candle rows", dropped)

        return df.with_columns([
            pl.col("ticker").cast(pl.Categorical),
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("close").cast(pl.UInt8),
            pl.col("volume").cast(pl.UInt32),
            pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
        ])


# ── response parsing ──────────────────────────────────────────────────────────

def _parse_candle_response(data: dict, ingested_at: datetime) -> list[dict]:
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

    rows = []
    for ticker, candles in candles_by_ticker.items():
        for c in candles:
            ts_raw = c.get("end_period_ts") or c.get("ts") or c.get("open_period_ts")
            if ts_raw is None:
                continue
            ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
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
            volume = int(float(c.get("volume") or c.get("volume_fp") or 0))
            rows.append({
                "ticker": ticker,
                "timestamp": ts,
                "close": close_cents,
                "volume": volume,
                "ingested_at": ingested_at,
            })
    return rows


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_strike(ticker: str) -> Optional[int]:
    try:
        return int(ticker.split("-B")[-1].split("-")[0])
    except (IndexError, ValueError):
        return None


def _parse_trade_date(ticker: str) -> Optional[date]:
    try:
        date_part = ticker.split("-B")[0].split("-", 1)[1]
        try:
            return date.fromisoformat(date_part)
        except ValueError:
            pass
        from datetime import datetime as _dt
        return _dt.strptime(date_part, "%d%b%y").date()
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
