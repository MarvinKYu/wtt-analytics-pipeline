"""
wtt_transform_dag.py

DAG 2: dbt transformation + rating engine.
Triggered by wtt_ingest_dag on success, or run manually.

Tasks:
  1. dbt_staging        → runs dbt staging models (stg_*)
  2. dbt_intermediate   → runs dbt intermediate models (int_*)
  3. run_rating_engine  → calls replay.py, writes mart_player_ratings_raw
  4. dbt_marts          → runs dbt mart models (mart_*)
  5. notify_success     → logs completion summary
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

_DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_rating_engine():
    from google.cloud import bigquery

    from rating_engine.replay import load_match_history, run_replay, write_ratings_to_bq

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_TRANSFORMED_DATASET", "wtt")
    bq_client = bigquery.Client(project=project_id)

    matches = load_match_history(bq_client, project_id)
    logger.info("Loaded %d matches for replay", len(matches))

    player_states = run_replay(matches)
    logger.info("Replay complete: %d players rated", len(player_states))

    write_ratings_to_bq(player_states, bq_client, project_id, dataset)


def _notify_success(**context):
    dag_run = context.get("dag_run")
    logger.info(
        "wtt_transform complete. run_id=%s logical_date=%s",
        dag_run.run_id if dag_run else "unknown",
        context.get("logical_date"),
    )


with DAG(
    dag_id="wtt_transform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # triggered only, not scheduled
    catchup=False,
    tags=["wtt", "transform", "dbt"],
) as dag:

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {_DBT_DIR} && dbt run --profiles-dir . --select staging",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"cd {_DBT_DIR} && dbt run --profiles-dir . --select intermediate",
    )

    run_rating_engine = PythonOperator(
        task_id="run_rating_engine",
        python_callable=_run_rating_engine,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {_DBT_DIR} && dbt run --profiles-dir . --select marts",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
    )

    dbt_staging >> dbt_intermediate >> run_rating_engine >> dbt_marts >> notify_success
