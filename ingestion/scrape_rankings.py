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
except ImportError:
    import bq_loader  # type: ignore[no-redef]

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("ITTF_BASE_URL", "https://results.ittf.link")
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY_SECONDS", "1.5"))
PAGE_SIZE = 50

_GENDER_CONFIG = {
    "men":   ("/index.php/ittf-rankings/ittf-ranking-men-singles",   "limitstart57"),
    "women": ("/index.php/ittf-rankings/ittf-ranking-women-singles", "limitstart58"),
}

_PLAYER_ID_RE = re.compile(r"vw_profiles___player_id_raw=(\d+)")


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch_page(url: str, params: dict) -> requests.Response:
    resp = requests.get(url, params=params, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp


def _parse_rows(soup: BeautifulSoup, gender: str, snapshot_date: str, scraped_at: str) -> list[dict]:
    # Class names differ between genders (fab_rank_ms___* vs fab_rank_ws___*) so we
    # match on the shared suffix via CSS attribute substring selectors.
    rows = []
    for tr in soup.select("table tbody tr"):
        rank_td     = tr.select_one("td[class*='___Position']")
        points_td   = tr.select_one("td[class*='___Points']")
        name_td     = tr.select_one("td[class*='___Name']")
        country_td  = tr.select_one("td[class*='___Country']")
        continent_td = tr.select_one("td[class*='___ITTF']")

        if not all([rank_td, points_td, name_td, country_td]):
            continue

        rank_text   = rank_td.get_text(strip=True)
        points_text = points_td.get_text(strip=True).replace(",", "")

        name_link = name_td.find("a")
        name = name_link.get_text(strip=True) if name_link else name_td.get_text(strip=True)

        player_id = None
        if name_link:
            m = _PLAYER_ID_RE.search(name_link.get("href", ""))
            if m:
                player_id = int(m.group(1))

        rows.append({
            "player_id": player_id,
            "rank": int(rank_text) if rank_text.isdigit() else None,
            "points": int(points_text) if points_text.isdigit() else None,
            "name": name,
            "association": country_td.get_text(strip=True),
            "continent": continent_td.get_text(strip=True) if continent_td else None,
            "gender": gender,
            "snapshot_date": snapshot_date,
            "scraped_at": scraped_at,
        })

    return rows


def scrape_rankings(gender: str = "men") -> list[dict]:
    """Scrape all pages of the ranking table for the given gender.
    Returns a list of dicts, one per player row.
    """
    if gender not in _GENDER_CONFIG:
        raise ValueError(f"gender must be 'men' or 'women', got {gender!r}")

    path, limitstart_key = _GENDER_CONFIG[gender]
    url = BASE_URL + path
    snapshot_date = date.today().isoformat()
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_rows: list[dict] = []
    limitstart = 0

    while True:
        params = {
            "resetfilters": "0",
            "clearordering": "0",
            "clearfilters": "0",
            limitstart_key: limitstart,
        }
        resp = _fetch_page(url, params)
        soup = BeautifulSoup(resp.text, "lxml")
        page_rows = _parse_rows(soup, gender, snapshot_date, scraped_at)

        if not page_rows:
            break

        all_rows.extend(page_rows)
        logger.info("offset=%d: fetched %d rows (running total: %d)", limitstart, len(page_rows), len(all_rows))

        if len(page_rows) < PAGE_SIZE:
            break

        limitstart += PAGE_SIZE
        time.sleep(SCRAPE_DELAY)

    logger.info("Scraped %d %s rankings total", len(all_rows), gender)
    return all_rows


def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    """Load ranking rows to BigQuery bronze table."""
    df = pd.DataFrame(rows)
    bq_loader.load_dataframe(df, project_id, dataset, "bronze_ittf_rankings")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_RAW_DATASET", "wtt_raw")

    rows = scrape_rankings("men") + scrape_rankings("women")
    load_to_bq(rows, project_id, dataset)
    print(f"Done. Loaded {len(rows)} rows to {dataset}.bronze_ittf_rankings")
