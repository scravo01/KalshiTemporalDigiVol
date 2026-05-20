from datetime import datetime, date, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

_PROJECT = "/opt/airflow/project"
# set -o pipefail preserves the Python exit code through the tr pipe
_CLI = f"set -o pipefail && cd {_PROJECT} && python -m src.cli.main"
# tr converts tqdm's \r updates into newlines so each step appears as its own log line
_PIPE = "2>&1 | tr '\\r' '\\n'"

_today = date.today()
_default_start = str(_today - timedelta(days=60))
_default_end = str(_today)


@dag(
    dag_id="kalshi_etl_pipeline",
    schedule="30 22 * * *",   # 22:30 UTC — after Kalshi hourly markets settle
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "kalshi_research",
    },
    params={
        "start_date": Param(
            default=_default_start,
            type="string",
            format="date",
            description="First trade date to ingest (YYYY-MM-DD). Defaults to 60 days ago.",
        ),
        "end_date": Param(
            default=_default_end,
            type="string",
            format="date",
            description="Last trade date to ingest (YYYY-MM-DD). Defaults to today.",
        ),
    },
    tags=["kalshi", "etl"],
)
def kalshi_pipeline():
    # On a scheduled run {{ ds }} is used; on a manual trigger the UI form
    # values override via params.start_date / params.end_date.
    start = "{{ params.start_date }}"
    end = "{{ params.end_date }}"

    bronze_binance = BashOperator(
        task_id="bronze_binance",
        bash_command=f"{_CLI} bronze-binance --start-date {start} --end-date {end} {_PIPE}",
    )

    bronze_kalshi = BashOperator(
        task_id="bronze_kalshi",
        bash_command=f"{_CLI} bronze-kalshi --start-date {start} --end-date {end} {_PIPE}",
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{_CLI} silver {_PIPE}",
    )

    vol_surface = BashOperator(
        task_id="vol_surface",
        bash_command=f"{_CLI} vol-surface {_PIPE}",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{_CLI} gold {_PIPE}",
    )

    rv_iv = BashOperator(
        task_id="rv_iv",
        bash_command=f"{_CLI} rv-iv {_PIPE}",
    )

    # bronze_kalshi needs binance_btc_1m.parquet to compute the ATM strike ladder
    bronze_binance >> bronze_kalshi >> [silver, vol_surface]
    silver >> gold
    vol_surface >> rv_iv


kalshi_pipeline()
