import asyncio
import logging
from datetime import date, timedelta
from pathlib import Path

import aiohttp
import click
from dotenv import load_dotenv

from src.clients.binance_client import _TIMEOUT as _BINANCE_TIMEOUT
from src.clients.binance_client import BinanceClient
from src.clients.kalshi_client import _TIMEOUT as _KALSHI_TIMEOUT
from src.clients.kalshi_client import KalshiClient
from src.etl.bronze.binance_bronze import BinanceBronzeETL
from src.etl.bronze.kalshi_bronze import KalshiBronzeETL
from src.etl.gold.gold_etl import run as gold_run
from src.etl.silver.silver_etl import SilverETL
from src.etl.silver.vol_surface_etl import VolSurfaceETL

_LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"


class _Date(click.ParamType):
    name = "DATE"

    def convert(self, value, param, ctx):
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(value)
        except ValueError:
            self.fail(
                f"'{value}' is not a valid date (expected YYYY-MM-DD)", param, ctx
            )


DATE = _Date()


def _default_start() -> str:
    return str(date.today() - timedelta(days=90))


def _default_end() -> str:
    return str(date.today() - timedelta(days=1))


@click.group()
def cli() -> None:
    """BTC Kalshi temporal digital vol pipeline."""


@cli.command("bronze-kalshi")
@click.option(
    "--start-date",
    type=DATE,
    default=_default_start,
    show_default="90 days ago",
    help="First trade date to fetch (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=DATE,
    default=_default_end,
    show_default="yesterday",
    help="Last trade date to fetch (YYYY-MM-DD).",
)
@click.option(
    "--api-key",
    envvar="KALSHI_API_KEY",
    required=True,
    help="Kalshi API key (env: KALSHI_API_KEY).",
)
@click.option(
    "--bronze-dir",
    type=click.Path(),
    default="data/bronze",
    show_default=True,
    help="Output directory for bronze parquets.",
)
def bronze_kalshi(
    start_date: date, end_date: date, api_key: str, bronze_dir: str
) -> None:
    """Fetch Kalshi markets and candles → bronze parquets (hours 22,23,0,1,2,3 UTC)."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )

    async def _run() -> None:
        async with aiohttp.ClientSession(timeout=_KALSHI_TIMEOUT) as session:
            client = KalshiClient(api_key=api_key, session=session)
            await KalshiBronzeETL(
                client=client,
                start_date=start_date,
                end_date=end_date,
                bronze_dir=Path(bronze_dir),
            )._pipeline()

    asyncio.run(_run())


@cli.command("bronze-binance")
@click.option(
    "--start-date", type=DATE, default=_default_start, show_default="90 days ago"
)
@click.option("--end-date", type=DATE, default=_default_end, show_default="yesterday")
@click.option(
    "--bronze-dir", type=click.Path(), default="data/bronze", show_default=True
)
def bronze_binance(start_date: date, end_date: date, bronze_dir: str) -> None:
    """Fetch Binance BTCUSDT 1m klines → bronze parquet."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )

    async def _run() -> None:
        async with aiohttp.ClientSession(timeout=_BINANCE_TIMEOUT) as session:
            client = BinanceClient(session=session)
            await BinanceBronzeETL(
                client=client,
                start_date=start_date,
                end_date=end_date,
                bronze_dir=Path(bronze_dir),
            )._pipeline()

    asyncio.run(_run())


@cli.command("silver")
@click.option(
    "--bronze-dir", type=click.Path(), default="data/bronze", show_default=True
)
@click.option(
    "--silver-dir", type=click.Path(), default="data/silver", show_default=True
)
def silver(bronze_dir: str, silver_dir: str) -> None:
    """Join bronze parquets, compute IV and delta → silver parquet."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    SilverETL(bronze_dir=Path(bronze_dir), silver_dir=Path(silver_dir)).run()


@cli.command("vol-surface")
@click.option(
    "--bronze-dir", type=click.Path(), default="data/bronze", show_default=True
)
@click.option(
    "--silver-dir", type=click.Path(), default="data/silver", show_default=True
)
def vol_surface(bronze_dir: str, silver_dir: str) -> None:
    """Compute IV at every traded minute → silver/vol_surface.parquet."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    VolSurfaceETL(bronze_dir=Path(bronze_dir), silver_dir=Path(silver_dir)).run()


@cli.command("gold")
def gold_cmd() -> None:
    """Compute gold-layer features and summary stats → gold parquets."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    gold_run()


@cli.command("rv-iv")
def rv_iv_cmd() -> None:
    """Compute 5-min realised vol vs ATM implied vol and generate analysis plots."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    from src.etl.gold.rv_iv_analysis import run
    run()


@cli.command("pipeline")
@click.option(
    "--start-date", type=DATE, default=_default_start, show_default="90 days ago"
)
@click.option("--end-date", type=DATE, default=_default_end, show_default="yesterday")
@click.option(
    "--api-key",
    envvar="KALSHI_API_KEY",
    required=True,
    help="Kalshi API key (env: KALSHI_API_KEY).",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default="data",
    show_default=True,
    help="Root data directory (sub-dirs bronze/silver/gold created automatically).",
)
def pipeline(start_date: date, end_date: date, api_key: str, data_dir: str) -> None:
    """Run the full bronze → silver → gold pipeline."""
    logging.basicConfig(
        level=logging.INFO, format=_LOG_FMT, datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    root = Path(data_dir)
    bronze = root / "bronze"
    silver_dir = root / "silver"

    async def _run_bronze() -> None:
        async with aiohttp.ClientSession(timeout=_BINANCE_TIMEOUT) as binance_session:
            await BinanceBronzeETL(
                client=BinanceClient(session=binance_session),
                start_date=start_date,
                end_date=end_date,
                bronze_dir=bronze,
            )._pipeline()
        async with aiohttp.ClientSession(timeout=_KALSHI_TIMEOUT) as kalshi_session:
            await KalshiBronzeETL(
                client=KalshiClient(api_key=api_key, session=kalshi_session),
                start_date=start_date,
                end_date=end_date,
                bronze_dir=bronze,
            )._pipeline()

    asyncio.run(_run_bronze())
    SilverETL(bronze_dir=bronze, silver_dir=silver_dir).run()
    VolSurfaceETL(bronze_dir=bronze, silver_dir=silver_dir).run()
    gold_run()


def main() -> None:
    """Entry point: loads .env then dispatches to the Click CLI."""
    load_dotenv()
    cli()
