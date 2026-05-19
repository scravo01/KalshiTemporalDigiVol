import argparse
import logging
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("pipeline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BTC Kalshi Asia Vol Shift Pipeline")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=90),
        help="First trade date to fetch (YYYY-MM-DD). Default: 90 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="Last trade date to fetch (YYYY-MM-DD). Default: yesterday.",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    api_key = os.environ.get("KALSHI_API_KEY")
    if not api_key:
        logger.error("KALSHI_API_KEY not set. Add it to .env or export it.")
        sys.exit(1)

    logger.info(f"Pipeline start — {args.start_date} → {args.end_date}")

    # ── Stage 1: Bronze ───────────────────────────────────────────────────────
    logger.info("=== STAGE 1: BRONZE ETL ===")
    from src.clients.binance_client import BinanceClient
    from src.clients.kalshi_client import KalshiClient
    from src.etl.bronze.binance_bronze import run as binance_bronze_run
    from src.etl.bronze.kalshi_bronze import run as kalshi_bronze_run

    kalshi_client = KalshiClient(api_key=api_key)
    binance_client = BinanceClient()

    kalshi_bronze_run(kalshi_client, args.start_date, args.end_date)
    binance_bronze_run(binance_client, args.start_date, args.end_date)

    # ── Stage 2: Silver ───────────────────────────────────────────────────────
    logger.info("=== STAGE 2: SILVER ETL ===")
    from src.etl.silver.silver_etl import run as silver_run
    silver_run()

    # ── Stage 3: Gold ─────────────────────────────────────────────────────────
    logger.info("=== STAGE 3: GOLD ETL ===")
    from src.etl.gold.gold_etl import run as gold_run
    gold_run()

    logger.info("=== PIPELINE COMPLETE ===")
    logger.info("Launch the UI with:  streamlit run src/ui/app.py")


if __name__ == "__main__":
    main()
