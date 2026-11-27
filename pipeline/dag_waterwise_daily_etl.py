"""Airflow DAG for the complete daily WaterWise ETL pipeline."""

from __future__ import annotations

from datetime import timedelta

try:
    import pendulum
    from airflow import DAG
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import PythonOperator

    from pipeline.extract import main as run_extraction_stage
    from pipeline.load import EXIT_RETRY, main as run_load_stage
    from pipeline.transform import main as run_transformation_stage

    def run_extract() -> None:
        rc = run_extraction_stage(["--full"])
        if rc:
            # A normal exception lets Airflow retry the extraction task.
            raise RuntimeError(f"extraction failed with exit code {rc}")

    def run_transform() -> None:
        rc = run_transformation_stage([])
        if rc == 0:
            return
        if rc == 2:
            raise RuntimeError(
                "no staged extraction batch to transform yet; Airflow will retry"
            )
        reason = {
            1: "REJECTED (critical check failed; batch quarantined)",
            3: "MANUAL REVIEW (held; batch not promoted to Load)",
        }
        raise AirflowFailException(
            f"transformation {reason.get(rc, f'failed with exit code {rc}')}"
        )

    def run_load() -> None:
        rc = run_load_stage([])
        if rc == 0:
            return
        if rc in (2, EXIT_RETRY):
            reason = (
                "no transformation handover to load"
                if rc == 2
                else "warehouse unreachable"
            )
            raise RuntimeError(f"{reason}; Airflow will retry")
        reason = {
            1: "REJECTED (critical check failed)",
            3: "MANUAL REVIEW (held for a reviewer)",
        }
        raise AirflowFailException(
            f"load {reason.get(rc, f'failed with exit code {rc}')}"
        )

    with DAG(
        dag_id="waterwise_daily_etl",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Manila"),
        schedule_interval="0 1 * * *",
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-engineering",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["waterwise", "etl"],
    ) as dag:
        extract = PythonOperator(
            task_id="extract",
            python_callable=run_extract,
            execution_timeout=timedelta(minutes=30),
        )
        transform = PythonOperator(
            task_id="transform",
            python_callable=run_transform,
            execution_timeout=timedelta(minutes=30),
        )
        load = PythonOperator(
            task_id="load",
            python_callable=run_load,
            execution_timeout=timedelta(minutes=30),
        )

        extract >> transform >> load
except ImportError:
    # The processing modules remain importable without Airflow for local runs.
    dag = None
    extract = None
    transform = None
    load = None
