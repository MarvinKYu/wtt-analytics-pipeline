"""
bq_loader.py

Shared utility for loading pandas DataFrames into BigQuery.
Handles schema inference, append vs replace modes, and error logging.
"""

import logging

from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)


def load_dataframe(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    write_mode: str = "WRITE_APPEND",  # or "WRITE_TRUNCATE"
) -> None:
    """Load a DataFrame to a BigQuery table."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows to %s", len(df), table_ref)
