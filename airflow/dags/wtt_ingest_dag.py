"""
wtt_ingest_dag.py

DAG 1: Weekly ingestion pipeline.
Schedule: Every Monday at 06:00 UTC.

Tasks:
  1. scrape_rankings       → load to bronze_ittf_rankings
  2. scrape_match_history  → load to bronze_wtt_matches (watermark-incremental)
  3. scrape_ranking_history → load to bronze_ittf_ranking_history
  4. trigger_transform     → triggers wtt_transform_dag on success
"""

import logging
import os
import time
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _scrape_rankings():
    from ingestion.scrape_rankings import load_to_bq, scrape_rankings

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_RAW_DATASET", "wtt_raw")

    rows = scrape_rankings("men") + scrape_rankings("women")
    load_to_bq(rows, project_id, dataset)
    logger.info("Loaded %d ranking rows to %s.bronze_ittf_rankings", len(rows), dataset)


def _scrape_match_history():
    from google.cloud import bigquery

    from ingestion.scrape_match_history import (
        create_session,
        get_last_watermark,
        load_to_bq,
        scrape_player_matches,
    )

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_RAW_DATASET", "wtt_raw")
    delay = float(os.getenv("SCRAPE_DELAY_SECONDS", "1.5"))
    current_year = date.today().year
    bq_client = bigquery.Client(project=project_id)

    seed_query = f"""
        SELECT DISTINCT player_id
        FROM `{project_id}.{dataset}.bronze_ittf_rankings`
        WHERE player_id IS NOT NULL
        ORDER BY player_id
    """
    player_ids = [row.player_id for row in bq_client.query(seed_query).result()]
    logger.info("Seeded %d player IDs from bronze_ittf_rankings", len(player_ids))

    sess = create_session()
    total = 0

    for pid in player_ids:
        watermark = get_last_watermark(pid, bq_client, project_id, dataset)
        year_filter = current_year if watermark is not None else None
        rows = scrape_player_matches(pid, sess, year_filter=year_filter)
        if rows:
            load_to_bq(rows, project_id, dataset)
            total += len(rows)
        time.sleep(delay)

    logger.info("Loaded %d match rows to %s.bronze_wtt_matches", total, dataset)


def _scrape_ranking_history():
    from google.cloud import bigquery

    from ingestion.scrape_match_history import create_session
    from ingestion.scrape_ranking_history import (
        get_last_watermark,
        load_to_bq,
        scrape_player_ranking_history,
    )

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_RAW_DATASET", "wtt_raw")
    delay = float(os.getenv("SCRAPE_DELAY_SECONDS", "1.5"))
    current_year = date.today().year
    bq_client = bigquery.Client(project=project_id)

    seed_query = f"""
        SELECT DISTINCT player_id
        FROM `{project_id}.{dataset}.bronze_ittf_rankings`
        WHERE player_id IS NOT NULL
        ORDER BY player_id
    """
    player_ids = [row.player_id for row in bq_client.query(seed_query).result()]
    logger.info("Seeded %d player IDs from bronze_ittf_rankings", len(player_ids))

    sess = create_session()
    total = 0

    for pid in player_ids:
        watermark = get_last_watermark(pid, bq_client, project_id, dataset)
        year_filter = current_year if watermark is not None else None
        rows = scrape_player_ranking_history(pid, sess, year_filter=year_filter)
        if rows:
            load_to_bq(rows, project_id, dataset)
            total += len(rows)
        time.sleep(delay)

    logger.info(
        "Loaded %d ranking history rows to %s.bronze_ittf_ranking_history", total, dataset
    )


with DAG(
    dag_id="wtt_ingest",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1",  # every Monday at 06:00 UTC
    catchup=False,
    tags=["wtt", "ingestion"],
) as dag:

    scrape_rankings_task = PythonOperator(
        task_id="scrape_rankings",
        python_callable=_scrape_rankings,
        execution_timeout=timedelta(minutes=30),
    )

    scrape_match_history_task = PythonOperator(
        task_id="scrape_match_history",
        python_callable=_scrape_match_history,
        execution_timeout=timedelta(hours=4),
    )

    scrape_ranking_history_task = PythonOperator(
        task_id="scrape_ranking_history",
        python_callable=_scrape_ranking_history,
        execution_timeout=timedelta(hours=4),
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="wtt_transform",
        wait_for_completion=False,
    )

    (
        scrape_rankings_task
        >> scrape_match_history_task
        >> scrape_ranking_history_task
        >> trigger_transform
    )
