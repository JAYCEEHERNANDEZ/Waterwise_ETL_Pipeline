"""Airflow DAG: waterwise_daily_load (docs/load.md Sections 1 and 8).

Schedule: daily at 03:00 Asia/Manila, after the 01:00 extraction and the 02:00
transformation. The task calls pipeline.load.main() on the newest
transformation handover. A batch that was already loaded is refused by the
duplicate-batch check, so a day without a new handover fails visibly instead
of loading the same data twice.

Retries (docs/load.md Section 8):
- Warehouse unreachable (exit code 4): retried up to 3 times, 5 minutes apart.
  Safe, because a failed attempt was rolled back.
- Data or contract failure (exit code 1) and MANUAL REVIEW (exit code 3):
  failed at once without retrying, because a retry would fail the same way.
  The data engineer reviews the task log, load_log, and quarantine/load/.
  A held batch is published by rerunning with --accept-warnings "<reviewer>".

Requires on the worker: pip install -r pipeline/requirements.txt + apache-airflow,
and WAREHOUSE_DB_URL (role etl_loader) in pipeline/.env, which docker-compose
passes to the containers. Never commit that file.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import PythonOperator
    from pipeline.load import EXIT_RETRY, main as run_load_stage

    def run_load() -> None:
        rc = run_load_stage([])
        if rc == 0:
            return
        if rc == EXIT_RETRY:
            # Plain exception: Airflow marks the try FAILED and retries it.
            raise RuntimeError("warehouse unreachable; Airflow will retry")
        reason = {1: "REJECTED (critical check failed)",
                  2: "no transformation handover to load",
                  3: "MANUAL REVIEW (held; rerun with --accept-warnings after review)"}
        # AirflowFailException fails the task without using the remaining retries.
        raise AirflowFailException(f"load {reason.get(rc, f'failed with exit code {rc}')}")

    with DAG(
        dag_id="waterwise_daily_load",
        start_date=datetime(2026, 1, 1),
        schedule_interval="0 3 * * *",  # 03:00 daily; Airflow timezone is Asia/Manila
        catchup=False,
        max_active_runs=1,              # never two loads of the warehouse at once
        default_args={"owner": "data-engineering", "retries": 3,
                      "retry_delay": timedelta(minutes=5)},
        tags=["waterwise", "load"],
    ) as dag:
        load = PythonOperator(task_id="load", python_callable=run_load,
                              execution_timeout=timedelta(minutes=30))
except ImportError:
    # Importable without Airflow for local runs: `python -m pipeline.load --dry-run`
    dag = None
    load = None
