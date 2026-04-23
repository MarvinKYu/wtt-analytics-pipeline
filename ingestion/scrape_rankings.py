"""
scrape_rankings.py

Scrapes the ITTF Men's and Women's Singles ranking tables from results.ittf.link.

Source URLs:
  Men's:   https://results.ittf.link/index.php/ittf-rankings/ittf-ranking-men-singles
  Women's: https://results.ittf.link/index.php/ittf-rankings/ittf-ranking-women-singles

Pagination: URL param limitstart57=N (increments by 50 per page).
Each row yields: player_id, rank, points, name, association, continent, scraped_at.

Output: loads to BigQuery table wtt_raw.bronze_ittf_rankings (append with snapshot_date).
"""

def scrape_rankings(gender: str = "men") -> list[dict]:
    """Scrape all pages of the ranking table for the given gender.
    Returns a list of dicts, one per player row.
    """
    raise NotImplementedError

def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    """Load ranking rows to BigQuery bronze table."""
    raise NotImplementedError

if __name__ == "__main__":
    rows = scrape_rankings("men") + scrape_rankings("women")
    load_to_bq(rows, ...)
