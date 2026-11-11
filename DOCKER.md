# WaterWise Airflow on Docker — setup guide

Runs three DAGs:

| DAG | File | Schedule (Asia/Manila) |
| --- | --- | --- |
| `waterwise_daily_extraction` | `pipeline/dag_waterwise_daily_extraction.py` | daily 01:00, 3 retries |
| `waterwise_daily_transformation` | `pipeline/dag_waterwise_daily_transformation.py` | daily 02:00, 3 retries 5 minutes apart, only on a read error or when no staged batch exists yet |
| `waterwise_daily_load` | `pipeline/dag_waterwise_daily_load.py` | daily 03:00, 3 retries 5 minutes apart, only when the warehouse is unreachable |

## Prereqs

- Docker Desktop running (`docker run hello-world` works).
- `pipeline/.env` holds `WATERWISE_DB_URL` (pooler `:6543`, password
  URL-encoded: `?`->`%3F` `@`->`%40`). Never commit this file.
- For the load, `pipeline/.env` also holds `WAREHOUSE_DB_URL`: the
  session pooler URL (`:5432`) of the warehouse Supabase project, with
  the `etl_loader` role. Only `pipeline/.env` is passed to the containers,
  so a value in the repo-root `.env` is not seen by Airflow.
- The warehouse tables exist: run `db/warehouse_schema.sql` once in the
  warehouse project's SQL Editor.

## First-time init (run once, in this folder)

```powershell
docker compose up -d postgres
docker compose run --rm airflow-scheduler airflow db migrate
docker compose run --rm airflow-scheduler airflow users create `
  --username admin --password admin `
  --firstname Admin --lastname User --role Admin `
  --email admin@example.com
docker compose up -d
```

Then open **http://localhost:8080**, log in as `admin` / `admin`
(change the password in the UI), and unpause
`waterwise_daily_extraction`, `waterwise_daily_transformation` and
`waterwise_daily_load`.

> First start downloads ~1 GB of images and pip-installs
> `psycopg2-binary pandas python-dotenv` in the containers —
> allow several minutes.

## Daily use

- UI shows green/red per run, logs, manual **Trigger DAG**, and
  **Clear/Retry** for re-running a window (idempotent — safe to repeat).
- Outputs land in `./rawdata/`, `./transformed/`, `./quarantine/` and
  `./logs/` on your machine (mounted into the containers).
- A red `load` task means the batch was not published; nothing was
  written. The task log, `warehouse.load_log` and
  `logs/waterwise-load/` name the failed check; rejected rows are in
  `quarantine/load/<run_date>/rejected.csv`.
- A batch held for **MANUAL REVIEW** is published after review with:

  ```powershell
  docker compose exec -w /opt/airflow/dags airflow-scheduler `
    python -m pipeline.load --accept-warnings "<your name>"
  ```

  Add `--dry-run` to any load command to run every check and roll back.

## Useful commands

```powershell
docker compose ps            # status
docker compose logs -f airflow-scheduler   # scheduler logs
docker compose down          # stop everything
docker compose down -v       # stop + wipe metadata DB (runs history)
```

## Notes

- Timezone is `Asia/Manila`, so cron `0 1 * * *` = 01:00 Manila.
- Airflow here is the **scheduler/monitor**; extraction logic stays in
  `pipeline/stage.py` and also runs standalone:
  `python -m pipeline.stage --dry-run`. The load logic stays in
  `pipeline/load.py` and runs standalone the same way:
  `python -m pipeline.load --dry-run`.
- The three DAGs run one after another by schedule (01:00 extract,
  02:00 transform, 03:00 load); each works on the newest batch the step
  before it produced. If a step fails, the next one finds no new batch:
  the load then fails on the duplicate-batch check instead of loading
  the same data twice. Fix the failed step, rerun it, then trigger the
  next DAG by hand.
