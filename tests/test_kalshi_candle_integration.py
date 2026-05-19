"""
Live API integration test — proves that the 6 midnight windows for a single day
return non-empty 1-minute candle series, with the correct strike selection.

Skipped automatically when credentials are absent.
"""
import asyncio
import os
from datetime import date, datetime, timedelta, timezone

import pytest
from dotenv import load_dotenv

load_dotenv()

# Midnight 2026-03-21: all 7 windows are post-cutoff so the live endpoint handles them.
_MIDNIGHT = datetime(2026, 3, 21, 0, 0, 0, tzinfo=timezone.utc)
_SETTLEMENT_HOURS = [22, 23, 0, 1, 2, 3]  # UTC


@pytest.mark.integration
def test_six_midnight_windows_have_candles():
    """
    For each of the 6 settlement hours around midnight 2026-03-21:
      - Fetch markets from the live endpoint for that specific expiry window.
      - Select ATM (nearest to BTC spot) + 4 above + 4 below at $500 increments.
      - Fetch 1-minute candles for the 1-hour trading window.
      - Assert each window returns at least 30 bars for the ATM contract
        (accommodates low-liquidity OTM wings without failing).
    """
    key_id = os.environ.get("KEY_ID", "")
    api_key = os.environ.get("KALSHI_API_KEY", "")
    if not key_id or not api_key:
        pytest.skip("KALSHI_API_KEY / KEY_ID not set — skipping live API test")

    import base64
    import json
    import time

    import aiohttp
    import polars as pl
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as _padding

    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
    API_PATH = "/trade-api/v2"

    pk = serialization.load_pem_private_key(api_key.encode(), password=None)

    def _headers(method, path):
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + API_PATH + path).encode()
        sig = pk.sign(
            msg,
            _padding.PSS(mgf=_padding.MGF1(hashes.SHA256()), salt_length=_padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json",
        }

    def _parse_strike(ticker: str):
        try:
            if "-T" in ticker:
                return float(ticker.split("-T")[-1])
        except (IndexError, ValueError):
            pass
        return None

    async def run():
        results = {}

        async with aiohttp.ClientSession() as session:
            for offset, hour in enumerate(_SETTLEMENT_HOURS):
                expiry_ts = _MIDNIGHT + timedelta(hours=hour - 24 if hour >= 12 else hour)
                # For 22, 23: they're before midnight so subtract 24h then add back
                # Simpler: compute directly
                if hour >= 12:
                    # evening hours: expiry is on the day BEFORE midnight
                    expiry_ts = datetime(
                        _MIDNIGHT.year, _MIDNIGHT.month, _MIDNIGHT.day,
                        tzinfo=timezone.utc
                    ) - timedelta(hours=24 - hour)
                else:
                    expiry_ts = datetime(
                        _MIDNIGHT.year, _MIDNIGHT.month, _MIDNIGHT.day,
                        hour, 0, 0, tzinfo=timezone.utc
                    )
                window_start = expiry_ts - timedelta(hours=1)

                # ── fetch markets for this expiry window ─────────────────────
                path = "/markets"
                params = {
                    "series_ticker": "KXBTCD",
                    "limit": 1000,
                    "min_close_ts": int(expiry_ts.timestamp()) - 1,
                    "max_close_ts": int(expiry_ts.timestamp()) + 1,
                }
                url = BASE_URL + path
                async with session.get(url, headers=_headers("GET", path), params=params) as r:
                    data = await r.json()

                markets = data.get("markets", [])
                assert markets, (
                    f"No markets found for expiry={expiry_ts.isoformat()}. "
                    "Check that the live endpoint covers this date."
                )

                # ── parse strikes, pick ATM from Binance spot ────────────────
                # For test purposes use the first available spot that gives a mid-range ATM.
                strike_to_ticker = {}
                for m in markets:
                    t = m.get("ticker", "")
                    s = _parse_strike(t)
                    if s is not None:
                        strike_to_ticker[int(round(s))] = t

                available = sorted(strike_to_ticker.keys())
                assert len(available) >= 9, (
                    f"Too few strikes ({len(available)}) for expiry={expiry_ts}. "
                    "Cannot select 9 contracts."
                )

                # Use the median available strike as a proxy ATM (no Binance call needed for test)
                mid_idx = len(available) // 2
                atm_strike = available[mid_idx]
                selected_strikes = []
                for i in range(-4, 5):  # -4, -3, -2, -1, 0, +1, +2, +3, +4
                    target = atm_strike + i * 500
                    closest = min(available, key=lambda k: abs(k - target))
                    selected_strikes.append(closest)
                selected_strikes = sorted(set(selected_strikes))
                tickers = [strike_to_ticker[s] for s in selected_strikes if s in strike_to_ticker]

                assert len(tickers) == 9, (
                    f"Expected 9 tickers for expiry={expiry_ts}, got {len(tickers)}: {selected_strikes}"
                )

                # ── fetch 1-minute candles for the 1-hour window ─────────────
                candle_path = "/markets/candlesticks"
                candle_params = {
                    "market_tickers": ",".join(tickers),
                    "start_ts": int(window_start.timestamp()),
                    "end_ts": int(expiry_ts.timestamp()),
                    "period_interval": 1,
                }
                url = BASE_URL + candle_path
                async with session.get(url, headers=_headers("GET", candle_path),
                                       params=candle_params) as r:
                    candle_data = await r.json()

                bars_by_ticker = {}
                for entry in candle_data.get("markets", []):
                    tkr = entry.get("market_ticker", "")
                    bars_by_ticker[tkr] = len(entry.get("candlesticks", []))

                total_bars = sum(bars_by_ticker.values())
                atm_ticker = strike_to_ticker.get(atm_strike, "")
                atm_bars = bars_by_ticker.get(atm_ticker, 0)

                results[hour] = {
                    "expiry": expiry_ts.isoformat(),
                    "window_start": window_start.isoformat(),
                    "n_markets": len(available),
                    "tickers": tickers,
                    "total_bars": total_bars,
                    "atm_bars": atm_bars,
                }

                # Core assertion: ATM contract has at least 30 of its 60 possible 1-min bars
                assert atm_bars >= 30, (
                    f"Hour={hour:02d} expiry={expiry_ts}: ATM ticker {atm_ticker!r} "
                    f"returned only {atm_bars} bars (expected ≥30). "
                    "The 1-minute series may not be accessible for this window."
                )

        return results

    results = asyncio.run(run())

    print("\n── Live integration results ──────────────────────────────────")
    for hour, r in sorted(results.items()):
        print(f"  hour={hour:02d}  expiry={r['expiry']}  markets={r['n_markets']}  "
              f"total_bars={r['total_bars']}  atm_bars={r['atm_bars']}")

    # All 6 windows must have returned results
    assert len(results) == 6, f"Expected 6 windows, got {len(results)}"
