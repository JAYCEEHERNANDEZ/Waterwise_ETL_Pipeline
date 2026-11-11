"""Airflow DAG: waterwise_daily_transformation (docs/transformation.md Section 1.2).

Schedule: daily at 02:00 Asia/Manila, between the 01:00 extraction and the 03:00
load. The task calls pipeline.transform.main() on the newest staged extraction
batch in rawdata/ and writes the handover for the load to transformed/.
Reruns are idempotent: the same batch always produces the same output
(docs/transformation.md Section 1.4).

Retries (docs/transformation.md Sections 1.2 and 10.3):
- File-read or parse error (an exception), or no staged batch yet (exit code 2,
  for example when the extraction is still retrying): retried up to 3 times,
  5 minutes apart.
- REJECTED (exit code 1) and MANUAL REVIEW (exit code 3): failed at once
  without retrying, because a data or contract problem fails the same way
  every time. The data engineer reviews the task log,
  logs/waterwise-transformation/ and quarantine/transformation/.

Requires on the worker: pip install -r pipeline/requirements.txt + apache-airflow.
The transformation reads only files; it needs no database credentials.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import PythonOperator
    from pipeline.transform import main as run_transformation_stage

    def run_transform() -> None:
        rc = run_transformation_stage([])
        if rc == 0:
            return
        if rc == 2:
            # Plain exception: Airflow marks the try FAILED and retries it.
            raise RuntimeError("no staged extraction batch to transform yet; Airflow will retry")
        reason = {1: "REJECTED (critical check failed; batch quarantined)",
                  3: "MANUAL REVIEW (held; batch not promoted to Load)"}
        # AirflowFailException fails the task without using the remaining retries.
        raise AirflowFailException(
            f"transformation {reason.get(rc, f'failed with exit code {rc}')}")

    with DAG(
        dag_id="waterwise_daily_transformation",
        start_date=datetime(2026, 1, 1),
        schedule_interval="0 2 * * *",  # 02:00 daily; Airflow timezone is Asia/Manila
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "data-engineering", "retries": 3,
                      "retry_delay": timedelta(minutes=5)},
        tags=["waterwise", "transformation"],
    ) as dag:
        transform = PythonOperator(task_id="transform", python_callable=run_transform,
                                   execution_timeout=timedelta(minutes=30))
except ImportError:
    # Importable without Airflow for local runs: `python -m pipeline.transform --dry-run`
    dag = None
    transform = None
