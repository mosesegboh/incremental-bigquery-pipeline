"""Optional Airflow DAG for hourly orchestration.

Set Airflow Variable `de_test_project_dir` to the absolute project directory.
The DAG calls the same incremental command used by reviewers locally.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id="de_test_incremental_pipeline",
    description="Clean, upload, and commit pending transaction batches to BigQuery.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["de-test", "incremental"],
) as dag:
    run_incremental_cleaner = BashOperator(
        task_id="run_incremental_cleaner",
        bash_command=(
            "cd {{ var.value.de_test_project_dir }} && "
            "PIPELINE_ENV=prod bash scripts/gcp_bigquery_test.sh all"
        ),
    )

    run_incremental_cleaner
