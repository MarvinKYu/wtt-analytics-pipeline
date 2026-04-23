"""
scrape_ranking_history.py

For each player_id, fetches their historical ITTF ranking over time.

Ranking history URL pattern:
  https://results.ittf.link/index.php/player-ranking-history-seniors/list/45
  ?resetfilters=1&fab_rank_seniors___PID[value][]={player_id}&abc={player_id}

Each row yields: player_id, week_date, rank, points, scraped_at.

Output: loads to BigQuery table wtt_raw.bronze_ittf_ranking_history.
"""

def scrape_player_ranking_history(player_id: int) -> list[dict]:
    raise NotImplementedError

def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    raise NotImplementedError
