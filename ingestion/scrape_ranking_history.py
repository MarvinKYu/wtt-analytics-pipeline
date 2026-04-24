"""
scrape_ranking_history.py

For each player_id, fetches their historical ITTF weekly ranking from results.ittf.link.

Requires login: reuses create_session() from scrape_match_history.
Ranking history page: /index.php/player-ranking-history-seniors/list/45
Pagination key: limitstart45 (increments by 50).

Each row yields:
  player_id, week_date (ISO date parsed from week field), week_number,
  rank, rank_change, points, scraped_at

Output: loads to BigQuery table wtt_raw.bronze_ittf_ranking_history (WRITE_APPEND).
Watermark: on incremental runs, only fetches entries from the current calendar year.
"""

import logging
import os
import re
import time
from datetime import date, datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

try:
    from ingestion import bq_loader
    from ingestion.scrape_match_history import create_session
except ImportError:
    import bq_loader  # type: ignore[no-redef]
    from scrape_match_history import create_session  # type: ignore[no-redef]

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("ITTF_BASE_URL", "https://results.ittf.link")
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY_SECONDS", "1.5"))
PAGE_SIZE = 50
_LIST_ID = 45

# "17 (Apr 21st, 2026)" → capture month abbrev, day digits, year
_WEEK_DATE_RE = re.compile(r"\((\w+)\s+(\d+)(?:st|nd|rd|th),\s+(\d{4})\)")


def _parse_week_date(week_text: str) -> tuple[str | None, int | None]:
    """Return (iso_date, week_number) from '17 (Apr 21st, 2026)', or (None, None)."""
    m = _WEEK_DATE_RE.search(week_text)
    if not m:
        return None, None
    try:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y")
        iso_date = dt.strftime("%Y-%m-%d")
    except ValueError:
        return None, None
    week_num_m = re.match(r"(\d+)", week_text.strip())
    week_number = int(week_num_m.group(1)) if week_num_m else None
    return iso_date, week_number


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch_page(sess: requests.Session, player_id: int, limitstart: int, year_filter: int | None) -> requests.Response:
    url = BASE_URL + f"/index.php/player-ranking-history-seniors/list/{_LIST_ID}"
    params = {
        "resetfilters": "1" if limitstart == 0 else "0",
        "abc": str(player_id),
        "clearordering": "0",
        "clearfilters": "0",
        f"limitstart{_LIST_ID}": limitstart,
        "fab_rank_seniors___PID[value][]": str(player_id),
    }
    if year_filter is not None:
        params["fab_rank_seniors___Year[value][]"] = str(year_filter)
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp


def _parse_rows(soup: BeautifulSoup, player_id: int, scraped_at: str) -> list[dict]:
    rows = []
    tbl = soup.find("table")
    if not tbl:
        return rows

    for tr in tbl.find_all("tr"):
        pos_td = tr.select_one("td[class*='fab_rank_seniors___Position']")
        if not pos_td:
            continue
        rank_text = pos_td.get_text(strip=True)
        if not rank_text.isdigit():
            continue

        def cell(suffix: str) -> str:
            td = tr.select_one(f"td[class*='{suffix}']")
            return td.get_text(strip=True) if td else ""

        week_text = cell("fab_rank_seniors___Week")
        week_date, week_number = _parse_week_date(week_text)

        # Fall back to year+month when the week field has no date (older records)
        if week_date is None:
            yr_text = cell("fab_rank_seniors___Year")
            mo_text = cell("fab_rank_seniors___Month")
            if yr_text.isdigit() and mo_text.isdigit():
                week_date = f"{yr_text}-{int(mo_text):02d}-01"

        rank_change_text = cell("fab_rank_seniors___PositionDifference")
        rank_change = int(rank_change_text) if rank_change_text.lstrip("-").isdigit() else None

        points_text = cell("fab_rank_seniors___Points")
        points = int(points_text) if points_text.isdigit() else None

        rows.append({
            "player_id": player_id,
            "week_date": week_date,
            "week_number": week_number,
            "rank": int(rank_text),
            "rank_change": rank_change,
            "points": points,
            "scraped_at": scraped_at,
        })

    return rows


def scrape_player_ranking_history(
    player_id: int,
    sess: requests.Session,
    year_filter: int | None = None,
) -> list[dict]:
    """Fetch all weekly ranking history rows for a single player_id.

    year_filter: if set, only fetch entries from that calendar year.
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_rows: list[dict] = []
    limitstart = 0

    while True:
        resp = _fetch_page(sess, player_id, limitstart, year_filter)
        soup = BeautifulSoup(resp.text, "lxml")
        page_rows = _parse_rows(soup, player_id, scraped_at)

        if not page_rows:
            break

        all_rows.extend(page_rows)
        logger.debug(
            "player_id=%d offset=%d page_rows=%d total=%d",
            player_id, limitstart, len(page_rows), len(all_rows),
        )

        if len(page_rows) < PAGE_SIZE:
            break

        limitstart += PAGE_SIZE
        time.sleep(SCRAPE_DELAY)

    return all_rows


def get_last_watermark(player_id: int, bq_client, project_id: str, dataset: str) -> int | None:
    """Return the max year already loaded for this player_id, or None."""
    query = f"""
        SELECT MAX(EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', week_date))) AS max_year
        FROM `{project_id}.{dataset}.bronze_ittf_ranking_history`
        WHERE player_id = {player_id}
    """
    try:
        rows = list(bq_client.query(query).result())
        val = rows[0].max_year if rows else None
        return int(val) if val is not None else None
    except Exception:
        return None


def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    """Append ranking history rows to BigQuery bronze table."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["rank_change"] = df["rank_change"].astype("Int64")
    bq_loader.load_dataframe(df, project_id, dataset, "bronze_ittf_ranking_history")


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery as bq_module

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_RAW_DATASET", "wtt_raw")
    current_year = date.today().year
    bq_client = bq_module.Client(project=project_id)

    seed_query = f"""
        SELECT DISTINCT player_id
        FROM `{project_id}.{dataset}.bronze_ittf_rankings`
        WHERE player_id IS NOT NULL
        ORDER BY player_id
    """
    player_ids = [row.player_id for row in bq_client.query(seed_query).result()]
    logger.info("Seeded %d player IDs from bronze_ittf_rankings", len(player_ids))

    sess = create_session()
    total_rows = 0

    for pid in player_ids:
        watermark = get_last_watermark(pid, bq_client, project_id, dataset)
        year_filter = current_year if watermark is not None else None
        rows = scrape_player_ranking_history(pid, sess, year_filter=year_filter)
        if rows:
            load_to_bq(rows, project_id, dataset)
            total_rows += len(rows)
            logger.info("player_id=%d scraped=%d year_filter=%s", pid, len(rows), year_filter)
        time.sleep(SCRAPE_DELAY)

    print(f"Done. Loaded {total_rows} rows to {dataset}.bronze_ittf_ranking_history")
