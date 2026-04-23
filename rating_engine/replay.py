"""
replay.py

Replays all matches in chronological order through glicko.py to compute
current ratings for all WTT players. Reads from BigQuery silver layer,
writes results to BigQuery mart table (wtt.mart_player_ratings_raw).

This is called by the Airflow wtt_transform_dag after dbt staging models run.
"""

def load_match_history(bq_client, project_id: str) -> list[dict]:
    """Read int_player_match_history from BigQuery, ordered by match_date ASC."""
    raise NotImplementedError

def run_replay(matches: list[dict]) -> dict[int, "PlayerState"]:
    """Replay all matches in order. Returns final PlayerState per player_id."""
    raise NotImplementedError

def write_ratings_to_bq(
    player_states: dict,
    bq_client,
    project_id: str,
    dataset: str,
) -> None:
    """Write final player ratings to mart_player_ratings_raw table."""
    raise NotImplementedError
