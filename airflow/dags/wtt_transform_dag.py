"""
wtt_transform_dag.py

DAG 2: dbt transformation + rating engine.
Triggered by wtt_ingest_dag on success, or run manually.

Tasks:
  1. dbt_run_staging       → runs dbt staging models
  2. dbt_run_intermediate  → runs dbt intermediate models
  3. run_rating_engine     → calls replay.py, writes mart_player_ratings_raw
  4. dbt_run_marts         → runs dbt mart models (depends on rating engine output)
  5. notify_success        → logs completion summary
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wtt_transform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # triggered only, not scheduled
    catchup=False,
    tags=["wtt", "transform", "dbt"],
) as dag:

    # TODO: implement task functions and wire up operators
    pass
