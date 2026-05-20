import asyncio
import base64
import logging
import math
import os
import time
from datetime import date, datetime, timezone
from typing import ClassVar, Optional

import aiohttp
import polars as pl
from aiolimiter import AsyncLimiter
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm

_MAX_RETRIES = 3
_RETRY_BACKOFF = [1.0, 3.0, 9.0]  # seconds between retries

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
_API_PATH_PREFIX = "/trade-api/v2"  # path component prepended when signing requests
BATCH_SIZE = 5  # max tickers per candle request (10k candles / ~1 260 per ticker)
CONCURRENCY = 3  # parallel candle requests
RATE_LIMIT = 5  # requests per second — reduced to avoid 429s on large backfills
_TIMEOUT = aiohttp.ClientTimeout(total=30)


class KalshiClient:
    # Shared across all instances so the 10 req/s budget is never doubled in a
    # backfill loop that creates multiple clients.
    _limiter: ClassVar[AsyncLimiter] = AsyncLimiter(RATE_LIMIT, 1.0)

    def __init__(
        self, api_key: str, session: aiohttp.ClientSession | None = None
    ) -> None:
        # api_key holds the RSA PEM private key; KEY_ID env var is the key UUID
        self.key_id: str = os.environ.get("KEY_ID", "")
        pem = api_key.encode() if isinstance(api_key, str) else api_key
        self._private_key = serialization.load_pem_private_key(pem, password=None)
        self._session = session
        # Fetched lazily on first fetch_markets call so __init__ stays sync and
        # can safely be called inside an already-running event loop.
        self.historical_cutoff: Optional[datetime] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=_TIMEOUT)
        return self._session

    # ── internal helpers ──────────────────────────────────────────────────────

    def _make_headers(self, method: str, path: str) -> dict:
        """Generate per-request RSA-PSS signed headers. `path` is the endpoint path (/historical/...)."""
        timestamp_ms = str(int(time.time() * 1000))
        full_path = _API_PATH_PREFIX + path
        msg = (timestamp_ms + method.upper() + full_path).encode("utf-8")
        sig = self._private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json",
        }

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
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt, backoff in enumerate([0.0] + _RETRY_BACKOFF):
            if backoff:
                await asyncio.sleep(backoff)
            try:
                headers = self._make_headers("GET", path)
                async with session.get(
                    f"{BASE_URL}{path}",
                    params=params or {},
                    headers=headers,
                    timeout=_TIMEOUT,
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientResponseError as exc:
                if 400 <= exc.status < 500 and exc.status != 429:
                    raise  # client error — retrying won't help
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(
                        "Request to %s failed (attempt %d/%d): %s — retrying in %.0fs",
                        path,
                        attempt + 1,
                        _MAX_RETRIES,
                        exc,
                        _RETRY_BACKOFF[attempt],
                    )
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(
                        "Request to %s failed (attempt %d/%d): %s — retrying in %.0fs",
                        path,
                        attempt + 1,
                        _MAX_RETRIES,
                        exc,
                        _RETRY_BACKOFF[attempt],
                    )
        raise last_exc

    # ── cutoff ────────────────────────────────────────────────────────────────

    async def _ensure_cutoff(self) -> None:
        if self.historical_cutoff is not None:
            return
        session = self._get_session()
        data = await self._get(session, "/historical/cutoff")
        ts = data.get("market_settled_ts")
        if ts is None:
            ts = data.get("cutoff")
        if ts is None:
            raise RuntimeError(f"Unexpected /historical/cutoff response: {data}")
        if isinstance(ts, (int, float)):
            self.historical_cutoff = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        else:
            self.historical_cutoff = datetime.fromisoformat(
                str(ts).replace("Z", "+00:00")
            )
        logger.info("Kalshi historical cutoff: %s", self.historical_cutoff)

    # ── public API ────────────────────────────────────────────────────────────

    def _is_historical(self, trade_date: date) -> bool:
        if self.historical_cutoff is None:
            raise RuntimeError(
                "historical_cutoff not yet fetched — call fetch_markets first"
            )
        dt = datetime(
            trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc
        )
        return dt < self.historical_cutoff

    async def fetch_markets(self, start_date: date, end_date: date) -> pl.DataFrame:
        await self._ensure_cutoff()
        rows: list[dict] = []
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
        start_dt = datetime(
            start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc
        )
        end_dt = datetime(
            end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc
        )

        session = self._get_session()
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

            label = "historical" if use_historical else "live"
            days = (end_date - start_date).days + 1
            estimated_pages = max(1, math.ceil(days * 3_500 / 200))
            with tqdm(
                desc=f"Kalshi markets ({label})",
                unit=" pages",
                total=estimated_pages,
                leave=True,
            ) as pbar:
                while True:
                    async with self._limiter:
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
                        settlement_price: Optional[float] = None
                        if sr not in (None, "", "unknown"):
                            if sr == "yes":
                                settlement_price = 1.0
                            elif sr == "no":
                                settlement_price = 0.0
                            else:
                                try:
                                    settlement_price = float(sr)
                                except (TypeError, ValueError):
                                    pass
                        rows.append(
                            {
                                "ticker": ticker,
                                "trade_date": trade_dt,
                                "strike": strike,
                                "expiry_time": _parse_timestamp(expiry_raw),
                                "status": m.get("status", "unknown"),
                                "settlement_price": settlement_price,
                                "ingested_at": ingested_at,
                            }
                        )
                    pbar.update(1)
                    pbar.set_postfix(markets=len(rows))
                    cursor = data.get("cursor")
                    if not cursor:
                        pbar.total = pbar.n  # snap to actual so bar shows 100%
                        pbar.refresh()
                        break
                    params["cursor"] = cursor

        if not rows:
            logger.warning("fetch_markets returned 0 rows")
            return _empty_markets_df()

        return pl.DataFrame(rows).with_columns(
            [
                pl.col("ticker").cast(pl.Categorical),
                pl.col("trade_date").cast(pl.Date),
                pl.col("strike").cast(pl.UInt32),
                pl.col("expiry_time").cast(pl.Datetime("us", "UTC")),
                pl.col("status").cast(pl.Categorical),
                pl.col("settlement_price").cast(pl.Float32),
                pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
            ]
        )

    async def fetch_candles(
        self,
        tickers: list[str],
        start_ts: datetime,
        end_ts: datetime,
        is_historical: bool = False,
        period_interval: int = 1,
    ) -> pl.DataFrame:
        """Fetch candles for all tickers, running up to CONCURRENCY requests in parallel."""
        path = "/markets/candlesticks"
        base_params = {
            "start_ts": int(start_ts.timestamp()),
            "end_ts": int(end_ts.timestamp()),
            "period_interval": period_interval,
        }
        batches = [
            tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)
        ]
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)

        sem = asyncio.Semaphore(CONCURRENCY)
        session = self._get_session()

        async def _fetch_batch(batch: list[str]) -> list[dict]:
            async with sem:
                async with self._limiter:
                    try:
                        data = await self._get(
                            session,
                            path,
                            {**base_params, "market_tickers": ",".join(batch)},
                        )
                    except aiohttp.ClientResponseError as exc:
                        if exc.status == 400:
                            logger.warning(
                                "Candle 400 for batch starting %s — skipping (settled/unavailable)",
                                batch[0] if batch else "",
                            )
                            return []
                        raise
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

        return df.with_columns(
            [
                pl.col("ticker").cast(pl.Categorical),
                pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
                pl.col("close").cast(pl.UInt8),
                pl.col("volume").cast(pl.UInt32),
                pl.col("ingested_at").cast(pl.Datetime("us", "UTC")),
            ]
        )


# ── response parsing ──────────────────────────────────────────────────────────


def _parse_candle_response(data: dict, ingested_at: datetime) -> list[dict]:
    candles_by_ticker: dict = {}

    if "candles" in data and isinstance(data["candles"], dict):
        # Old format: {"candles": {"TICKER": [...]}}
        candles_by_ticker = data["candles"]
    elif "markets" in data and isinstance(data["markets"], list):
        # New format: {"markets": [{"candlesticks": [...], "market_ticker": "..."}]}
        for entry in data["markets"]:
            ticker = entry.get("market_ticker") or entry.get("ticker", "")
            candles_by_ticker[ticker] = entry.get("candlesticks") or entry.get(
                "candles", []
            )
    elif "markets_candles" in data:
        for entry in data["markets_candles"]:
            candles_by_ticker[entry["ticker"]] = entry.get("candles", [])
    else:
        raise KeyError(
            f"Unexpected candle response shape. Keys: {list(data.keys())}. "
            "Expected 'candles' (dict), 'markets' (list), or 'markets_candles' (list)."
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

            # New API uses dollar strings; old API uses integer cents in price.close
            close_raw = price_block.get("close")
            if close_raw is not None:
                close_cents = int(round(float(close_raw)))
            else:
                ask_close = ask_block.get("close_dollars") or ask_block.get("close")
                bid_close = bid_block.get("close_dollars") or bid_block.get("close")
                if ask_close is not None and bid_close is not None:
                    # mid-price in dollars → multiply by 100 for cents
                    mid = (float(ask_close) + float(bid_close)) / 2.0
                    close_cents = int(round(mid * 100))
                elif ask_close is not None:
                    close_cents = int(round(float(ask_close) * 100))
                elif bid_close is not None:
                    close_cents = int(round(float(bid_close) * 100))
                else:
                    continue

            if not (0 <= close_cents <= 100):
                continue
            volume = int(float(c.get("volume") or c.get("volume_fp") or 0))
            rows.append(
                {
                    "ticker": ticker,
                    "timestamp": ts,
                    "close": close_cents,
                    "volume": volume,
                    "ingested_at": ingested_at,
                }
            )
    return rows


# ── helpers ───────────────────────────────────────────────────────────────────


def _parse_strike(ticker: str) -> Optional[int]:
    try:
        # New format: KXBTCD-26MAY1901-T85799.99 → T prefix with decimal price
        if "-T" in ticker:
            raw = ticker.split("-T")[-1].split("-")[0]
            return int(round(float(raw)))
        # Old format: KXBTCD-25MAY24-B95000 → B prefix with integer price
        if "-B" in ticker:
            return int(ticker.split("-B")[-1].split("-")[0])
        return None
    except (IndexError, ValueError):
        return None


def _parse_trade_date(ticker: str) -> Optional[date]:
    """Extract the settlement calendar date from a KXBTCD ticker."""
    try:
        # Strip series prefix and strike: KXBTCD-26MAY1901-T85799.99 → 26MAY1901
        # or KXBTCD-25MAY24-B95000 → 25MAY24
        separator = "-T" if "-T" in ticker else "-B"
        date_part = ticker.split(separator)[0].split("-", 1)[1]
        try:
            return date.fromisoformat(date_part)
        except ValueError:
            pass
        from datetime import datetime as _dt

        # Old daily format: 25MAY24 → %d%b%y
        try:
            return _dt.strptime(date_part, "%d%b%y").date()
        except ValueError:
            pass
        # New hourly format: 26MAY1901 (yymmmDDHH or yyMMMDDHH)
        # "26MAY19" is the date part (2026-May-19), "01" is the hour
        for fmt in ("%y%b%d%H", "%y%b%d"):
            try:
                return _dt.strptime(
                    date_part[:8] if len(date_part) >= 8 else date_part, fmt
                ).date()
            except ValueError:
                pass
        return None
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
    return pl.DataFrame(
        schema={
            "ticker": pl.Categorical,
            "trade_date": pl.Date,
            "strike": pl.UInt32,
            "expiry_time": pl.Datetime("us", "UTC"),
            "status": pl.Categorical,
            "settlement_price": pl.Float32,
            "ingested_at": pl.Datetime("us", "UTC"),
        }
    )


def _empty_candles_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "ticker": pl.Categorical,
            "timestamp": pl.Datetime("us", "UTC"),
            "close": pl.UInt8,
            "volume": pl.UInt32,
            "ingested_at": pl.Datetime("us", "UTC"),
        }
    )
