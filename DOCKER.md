# WaterWise Airflow on Docker — setup guide

Runs `waterwise_daily_extraction` (daily 01:00 Asia/Manila, 3 retries)
from `pipeline/dag_waterwise_daily_extraction.py`.

## Prereqs

- Docker Desktop running (`docker run hello-world` works).
- `pipeline/.env` holds `WATERWISE_DB_URL` (pooler `:6543`, password
  URL-encoded: `?`->`%3F` `@`->`%40`). Never commit this file.

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
`waterwise_daily_extraction`.

> First start downloads ~1 GB of images and pip-installs
> `psycopg2-binary pandas python-dotenv` in the containers —
> allow several minutes.

## Daily use

- UI shows green/red per run, logs, manual **Trigger DAG**, and
  **Clear/Retry** for re-running a window (idempotent — safe to repeat).
- Outputs land in `./rawdata/` and `./logs/` on your machine
  (mounted into the containers).

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
  `python -m pipeline.stage --dry-run`.
