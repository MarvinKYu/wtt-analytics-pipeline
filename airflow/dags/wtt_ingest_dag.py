"""
wtt_ingest_dag.py

DAG 1: Weekly ingestion pipeline.
Schedule: Every Monday at 06:00 UTC.

Tasks:
  1. scrape_rankings      → load to bronze_ittf_rankings
  2. scrape_match_history → load to bronze_wtt_matches (watermark-incremental)
  3. scrape_ranking_history → load to bronze_ittf_ranking_history
  4. trigger_transform    → triggers wtt_transform_dag on success
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wtt_ingest",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1",  # every Monday at 06:00 UTC
    catchup=False,
    tags=["wtt", "ingestion"],
) as dag:

    # TODO: implement task functions and wire up operators
    pass
