"""
bq_loader.py

Shared utility for loading pandas DataFrames into BigQuery.
Handles schema inference, append vs replace modes, and error logging.
"""

from google.cloud import bigquery
import pandas as pd

def load_dataframe(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    write_mode: str = "WRITE_APPEND",  # or "WRITE_TRUNCATE"
) -> None:
    """Load a DataFrame to a BigQuery table."""
    raise NotImplementedError
