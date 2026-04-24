"""
replay.py

Replays all matches in chronological order through glicko.py to compute
current ratings for all WTT players. Reads from BigQuery silver layer,
writes results to BigQuery mart table (wtt.mart_player_ratings_raw).

This is called by the Airflow wtt_transform_dag after dbt staging models run.

Depends on: int_player_match_history (dbt intermediate model) existing in BigQuery.

Input contract for int_player_match_history rows (one row per player per match):
  match_id    INT     -- Fabrik record ID, unique per match
  player_id   INT     -- the player this row describes
  opponent_id INT     -- opponent's player_id (NULL if opponent not in rankings)
  match_year  INT     -- calendar year (only date precision available)
  result      STRING  -- 'WIN' | 'LOSS'

load_match_history queries WHERE result = 'WIN' to get one row per unique match,
with player_id as winner_id and opponent_id as loser_id.
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

try:
    from ingestion import bq_loader
except ImportError:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from ingestion import bq_loader  # type: ignore[no-redef]

from rating_engine.glicko import MatchResult, PlayerState, update_match

load_dotenv()

logger = logging.getLogger(__name__)

# match_day epoch: all match_years are converted to integer days from this year.
# Using 2000 keeps all known ITTF data (earliest ~2000) non-negative.
_EPOCH_YEAR = 2000


def load_match_history(bq_client, project_id: str) -> list[dict]:
    """Read int_player_match_history from BigQuery, ordered by match_year, match_id ASC."""
    dataset = os.getenv("BQ_TRANSFORMED_DATASET", "wtt")
    query = f"""
        SELECT
            match_id,
            player_id   AS winner_id,
            opponent_id AS loser_id,
            match_year
        FROM `{project_id}.{dataset}.int_player_match_history`
        WHERE result = 'WIN'
          AND opponent_id IS NOT NULL
        ORDER BY match_year ASC, match_id ASC
    """
    return [dict(row) for row in bq_client.query(query).result()]


def run_replay(matches: list[dict]) -> dict[int, PlayerState]:
    """Replay all matches in order. Returns final PlayerState per player_id."""
    states: dict[int, PlayerState] = {}

    for row in matches:
        winner_id = int(row["winner_id"])
        loser_id = int(row["loser_id"])
        if winner_id == loser_id:
            continue

        match_day = (int(row["match_year"]) - _EPOCH_YEAR) * 365

        if winner_id not in states:
            states[winner_id] = PlayerState(player_id=winner_id)
        if loser_id not in states:
            states[loser_id] = PlayerState(player_id=loser_id)

        new_winner, new_loser = update_match(
            states[winner_id],
            states[loser_id],
            MatchResult(winner_id=winner_id, loser_id=loser_id, match_day=match_day),
        )
        states[winner_id] = new_winner
        states[loser_id] = new_loser

    return states


def write_ratings_to_bq(
    player_states: dict,
    bq_client,
    project_id: str,
    dataset: str,
) -> None:
    """Write final player ratings to mart_player_ratings_raw table (full replace each run)."""
    if not player_states:
        return
    computed_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "player_id": state.player_id,
            "rating": state.rating,
            "rd": state.rd,
            "sigma": state.sigma,
            "matches_played": state.matches_played,
            "computed_at": computed_at,
        }
        for state in player_states.values()
    ]
    df = pd.DataFrame(rows)
    bq_loader.load_dataframe(df, project_id, dataset, "mart_player_ratings_raw", write_mode="WRITE_TRUNCATE")
    logger.info("Wrote %d player ratings to %s.mart_player_ratings_raw", len(rows), dataset)


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery as bq_module

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_TRANSFORMED_DATASET", "wtt")
    bq_client = bq_module.Client(project=project_id)

    matches = load_match_history(bq_client, project_id)
    logger.info("Loaded %d matches", len(matches))

    player_states = run_replay(matches)
    logger.info("Replay complete: %d players rated", len(player_states))

    write_ratings_to_bq(player_states, bq_client, project_id, dataset)
