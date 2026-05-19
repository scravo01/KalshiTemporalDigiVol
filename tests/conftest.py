"""Shared fixtures available to all test modules."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

_REPO_ROOT = Path(__file__).parent.parent
_PROD_DATA = _REPO_ROOT / "data"


@pytest.fixture(autouse=True, scope="session")
def _guard_production_data():
    """Fail loudly if any test writes to or creates files in data/."""
    def _snapshot(directory: Path) -> dict:
        if not directory.exists():
            return {}
        return {p: p.stat().st_mtime for p in directory.rglob("*") if p.is_file()}

    before = _snapshot(_PROD_DATA)
    yield
    after = _snapshot(_PROD_DATA)
    modified = [str(p) for p in after if after[p] != before.get(p)]
    created  = [str(p) for p in after if p not in before]
    problems = modified + created
    assert not problems, (
        "Tests must not write to the production data/ directory.\n"
        f"Modified/created: {problems}\n"
        "Use tmp_path or --bronze-dir / --silver-dir flags instead."
    )


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Synthetic bronze for one trade-date (2024-05-25): 3 strikes × 24 expiry windows.

    All 24 hourly windows for ET day 2024-05-25 (= UTC 04:00 May 25 through 03:00 May 26).
    Snapshot labels relative to T0 = window-open at 00:00 UTC (Asian open):
      T+3 … T+12  → UTC expiry 04:00–13:00 May 25
      T-11 … T-2  → UTC expiry 14:00–23:00 May 25
      T-1  → UTC expiry 00:00 May 26
      T0   → UTC expiry 01:00 May 26
      T+1  → UTC expiry 02:00 May 26
      T+2  → UTC expiry 03:00 May 26

    Kalshi candles use end_period_ts; Binance uses open-period timestamps.
    Silver joins on b.timestamp = c.timestamp - 1 minute.
    """
    trade_date = date(2024, 5, 25)
    ingested = datetime(2024, 5, 26, 4, 0, 0, tzinfo=timezone.utc)

    strikes = [94_500, 95_000, 95_500]
    # 24 expiry windows: hours 4–23 on May 25, then hours 0–3 on May 26
    expiry_times = (
        [datetime(2024, 5, 25, h, 0, 0, tzinfo=timezone.utc) for h in range(4, 24)]
        + [datetime(2024, 5, 26, h, 0, 0, tzinfo=timezone.utc) for h in range(0, 4)]
    )

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

    # One candle per contract at expiry_time - 59 min (first end-period bar).
    # Avoid close=50 for any strike to prevent S=K degenerate IV (σ→0, brentq fails).
    # Spot is 95_150 (not equal to any strike), so ln(S/K) ≠ 0 for all three strikes.
    close_by_strike = {94_500: 65, 95_000: 53, 95_500: 38}
    candle_rows = []
    for exp in expiry_times:
        first_bar_ts = exp - timedelta(minutes=59)
        for s in strikes:
            candle_rows.append({
                "ticker": f"KXBTCD-TEST-E{exp.hour}H-T{s}",
                "timestamp": first_bar_ts,
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

    # Binance bars at candle_ts - 1 min = expiry - 1 hour (open-period convention).
    # Use 95_150.0 for all windows — spot ≠ any strike (94500, 95000, 95500).
    binance_ts = [exp - timedelta(hours=1) for exp in expiry_times]
    binance = pl.DataFrame({
        "timestamp": binance_ts,
        "close": [95_150.0] * len(binance_ts),
        "ingested_at": [ingested] * len(binance_ts),
    }).with_columns([
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
