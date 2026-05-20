# Airflow Deployment

The project includes a full Airflow + Docker Compose stack for automated daily scheduling. The DAG runs the complete bronze → silver → gold → RV-IV pipeline once per day after Kalshi hourly markets settle.

## Prerequisites

- Docker Desktop running
- `.env` file at repo root with Kalshi credentials and an Airflow Fernet key (see below)
- Port 8080 available on localhost

## One-Time Setup

**1. Generate a Fernet key and add it to `.env`:**

```bash
python -c "from cryptography.fernet import Fernet; print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())" >> .env
```

**2. Create the Airflow password file** (plaintext JSON, gitignored):

```bash
echo '{"admin": "admin"}' > .airflow_passwords.json
```

**3. Build the custom image** (installs project Python dependencies into the Airflow container):

```bash
docker compose -f docker-compose.airflow.yml build
```

This takes ~5–10 minutes on first build (polars, scipy, etc.).

## Start / Stop

```bash
# Start stack (detached)
docker compose -f docker-compose.airflow.yml up -d

# Open Airflow UI
open http://localhost:8080   # username: admin  password: admin

# Stop stack (data in data/ persists on host)
docker compose -f docker-compose.airflow.yml down
```

## DAG Overview

**DAG ID:** `kalshi_etl_pipeline`  
**Schedule:** `30 22 * * *` — 22:30 UTC daily, after Kalshi hourly markets settle (~22:00 UTC)  
**Start date:** 2026-05-01  
**Retries:** 2 attempts per task, 5-minute delay

Task dependency graph:

```
bronze_binance
      │
      ▼
bronze_kalshi
      │
      ├──────────────────────┐
      ▼                      ▼
   silver               vol_surface
      │                      │
      ▼                      ▼
    gold                   rv_iv
```

Each `bronze_*` task accepts `--start-date` and `--end-date` flags. On a scheduled run these default to the params defined at DAG level (60 days ago → today). On a manual trigger you can override them via the UI form.

## Manual Trigger with Custom Dates

In the Airflow UI: **DAGs → kalshi_etl_pipeline → Trigger DAG ▶ → Trigger DAG w/ config**

The form exposes two date pickers:

| Param | Default | Description |
|-------|---------|-------------|
| `start_date` | 60 days ago | First trade date to ingest (YYYY-MM-DD) |
| `end_date` | Today | Last trade date to ingest (YYYY-MM-DD) |

The `silver`, `vol_surface`, `gold`, and `rv_iv` tasks always reprocess all available bronze data — they do not accept date range params.

## Log Visibility

All tasks pipe output through `tr '\r' '\n'` so tqdm progress bars appear as individual log lines rather than a single overwritten line. Set `set -o pipefail` is also applied so Python exceptions propagate correctly through the pipe and mark the task as failed.

To view task logs: click any task box in the DAG run graph → **Logs**.

## Data Persistence

All data is written to `data/` in the repo root via a bind mount:

```
Container: /opt/airflow/project/data/
Host:       <repo-root>/data/
```

Files written inside the container are immediately visible on the host. No copy step is needed.

## Airflow 3.x Notes

This stack uses **Airflow 3.2.1** which has several breaking changes from 2.x:

- `airflow standalone` runs scheduler + API server in a single process (no separate webserver command)
- Authentication uses `SimpleAuthManager` — credentials are stored as plaintext in `.airflow_passwords.json` (not FAB/LDAP)
- `airflow.decorators.dag`, `airflow.models.param.Param`, and `airflow.operators.bash.BashOperator` are deprecated import paths but still work; you will see `UserWarning` in the DAG parsing logs — these are harmless
- Preferred imports: `airflow.sdk.dag`, `airflow.sdk.Param`, `airflow.providers.standard.operators.bash.BashOperator`

## Secrets Management

The Kalshi API key and Fernet key are loaded from `.env` at the repo root. The project bind-mounts `.` → `/opt/airflow/project`, and `python-dotenv` loads `.env` automatically when the CLI runs from that directory. No secrets are injected via docker-compose `environment:` entries (which would expose them in `docker inspect` output).

## Troubleshooting

**Task completes in < 1 second with exit code 0 but does nothing:**
- Verify `python -m src.cli.main` is used (not `kvol`) — the `kvol` entry point is not registered in the container
- Check that `src/cli/main.py` has `if __name__ == "__main__": main()` at the end

**No Python logs appear in task output:**
- Ensure all `logging.basicConfig()` calls use `force=True` to override Airflow's pre-configured logger

**`AIRFLOW_FERNET_KEY` not found:**
- Confirm `.env` contains the `AIRFLOW_FERNET_KEY=...` line
- Docker Compose automatically reads `.env` for `${VAR}` substitutions in the compose file
