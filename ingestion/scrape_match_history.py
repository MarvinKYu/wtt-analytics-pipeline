"""
scrape_match_history.py

For each player_id in the seed list (sourced from bronze_ittf_rankings),
fetches that player's full match history from results.ittf.link.

Match history URL pattern:
  https://results.ittf.link/index.php/player-matches/list/31
  ?resetfilters=1&abc={player_id}&vw_matches___player_a_id[value][]={player_id}
  &vw_matches___player_a_id[join][]=OR&vw_matches___player_x_id[value][]={player_id}
  ...

Each match row yields:
  match_id (derived), player_id, opponent_id, opponent_name,
  event_name, event_type (WTT/ITTF), draw_phase, round,
  player_games_won, opponent_games_won,
  game_scores (raw string e.g. "11:7 9:11 11:3"),
  result (WIN/LOSS), match_date, scraped_at

Output: loads to BigQuery table wtt_raw.bronze_wtt_matches.
Uses watermark from metadata table to only fetch new matches on incremental runs.
"""

def scrape_player_matches(player_id: int) -> list[dict]:
    """Fetch all match rows for a single player_id. Returns list of dicts."""
    raise NotImplementedError

def get_last_watermark(player_id: int, bq_client) -> str | None:
    """Return the most recent match_date already loaded for this player, or None."""
    raise NotImplementedError

def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    """Append match rows to BigQuery bronze table, deduped by match_id."""
    raise NotImplementedError
