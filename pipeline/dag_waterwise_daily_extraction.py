"""Airflow DAG: waterwise_daily_extraction (docs/extraction.md Section 1.4).

Schedule: once daily off-peak, e.g. 01:00 Asia/Manila. Retries: 3 with backoff.
Deploy this file to your Airflow dags/ folder. The task calls pipeline.stage.main(),
which performs incremental extraction using the stored last_successful_run state.

Requires on the worker: pip install -r pipeline/requirements.txt + apache-airflow,
and WATERWISE_DB_URL set in the Airflow connection / environment (service-role, server-side).
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from pipeline.stage import main as run_extraction

    with DAG(
        dag_id="waterwise_daily_extraction",
        start_date=datetime(2026, 1, 1),
        schedule_interval="0 1 * * *",  # 01:00 daily; set timezone to Asia/Manila in Airflow config
        catchup=False,
        default_args={"owner": "data-engineering", "retries": 3, "retry_delay": timedelta(minutes=5)},
        tags=["waterwise", "extraction"],
    ) as dag:
        extract = PythonOperator(task_id="extract", python_callable=lambda: run_extraction([]))
except ImportError:
    # Importable without Airflow for local runs: `python -m pipeline.stage --dry-run`
    dag = None
    extract = None
