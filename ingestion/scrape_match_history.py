"""
scrape_match_history.py

For each player_id in the seed list (sourced from bronze_ittf_rankings),
fetches that player's full match history from results.ittf.link.

Requires login: set ITTF_USERNAME and ITTF_PASSWORD in .env.
One login per run; the session is reused across all player scrapes.

Match history page: /index.php/player-matches/list/31
Pagination key: limitstart31 (increments by 50).
The vw_matches___kind[value][]=1 filter restricts to singles matches.

Each match row yields:
  match_id, player_id, event_name, sub_event, stage, round,
  name_a, name_x, result_a_games, result_x_games,
  game_scores, winner_name, match_year, scraped_at

The queried player appears in either position A or position X per row.
Deduplication by match_id (Fabrik record ID) is handled in the staging model.

Output: loads to BigQuery table wtt_raw.bronze_wtt_matches (WRITE_APPEND).
Watermark: on incremental runs, only fetches matches from the current calendar year.
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
_LIST_ID = 31

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_RESULT_RE = re.compile(r"(\d+)\s*-\s*(\d+)")
_RECORD_ID_RE = re.compile(r"ids\[(\d+)\]")


def create_session() -> requests.Session:
    """Return a logged-in requests.Session for results.ittf.link."""
    username = os.environ["ITTF_USERNAME"]
    password = os.environ["ITTF_PASSWORD"]

    sess = requests.Session()
    sess.headers.update(_HEADERS)

    r0 = sess.get(BASE_URL + "/", timeout=30)
    r0.raise_for_status()
    soup = BeautifulSoup(r0.text, "lxml")

    form = soup.find("form", id=lambda x: x and "login" in x.lower())
    if not form:
        raise RuntimeError("Login form not found on results.ittf.link homepage")

    token_input = next(
        (inp for inp in form.find_all("input", type="hidden") if len(inp.get("name", "")) == 32),
        None,
    )
    login_data = {
        "username": username,
        "password": password,
        "return": "",
        "option": "com_users",
        "task": "user.login",
    }
    if token_input:
        login_data[token_input["name"]] = "1"

    r_login = sess.post(BASE_URL + "/index.php", data=login_data, timeout=30, allow_redirects=True)
    r_login.raise_for_status()

    if sess.cookies.get("joomla_user_state") != "logged_in":
        raise RuntimeError("Login failed — check ITTF_USERNAME and ITTF_PASSWORD in .env")

    logger.info("Logged in to results.ittf.link")
    return sess


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch_page(sess: requests.Session, player_id: int, limitstart: int, year_filter: int | None) -> requests.Response:
    url = BASE_URL + f"/index.php/player-matches/list/{_LIST_ID}"
    params = {
        "resetfilters": "1" if limitstart == 0 else "0",
        "abc": str(player_id),
        "clearordering": "0",
        "clearfilters": "0",
        f"limitstart{_LIST_ID}": limitstart,
        "vw_matches___player_a_id[value][]": str(player_id),
        "vw_matches___player_a_id[join][]": "OR",
        "vw_matches___player_x_id[value][]": str(player_id),
        "vw_matches___player_x_id[join][]": "OR",
        "vw_matches___player_x_id[grouped_to_previous][]": "1",
        "vw_matches___kind[value][]": "1",
        "vw_matches___irm[condition]": "<>",
        "vw_matches___irm[value][]": "WO",
    }
    if year_filter is not None:
        params["vw_matches___yr[value][]"] = str(year_filter)
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp


def _parse_match_rows(soup: BeautifulSoup, player_id: int, scraped_at: str) -> list[dict]:
    rows = []
    tbl = soup.find("table")
    if not tbl:
        return rows

    for tr in tbl.find_all("tr"):
        yr_td = tr.select_one("td[class*='vw_matches___yr']")
        if not yr_td:
            continue
        yr_text = yr_td.get_text(strip=True)
        if not yr_text.isdigit():
            continue

        def cell(suffix: str) -> str:
            td = tr.select_one(f"td[class*='{suffix}']")
            return td.get_text(strip=True) if td else ""

        res_text = cell("vw_matches___res")
        res_m = _RESULT_RE.match(res_text.strip())
        result_a = int(res_m.group(1)) if res_m else None
        result_x = int(res_m.group(2)) if res_m else None

        match_id = None
        select_td = tr.find("td", class_="fabrik_select")
        if select_td:
            inp = select_td.find("input")
            if inp:
                id_m = _RECORD_ID_RE.search(inp.get("name", ""))
                if id_m:
                    match_id = int(id_m.group(1))

        rows.append({
            "match_id": match_id,
            "player_id": player_id,
            "event_name": cell("vw_matches___tournament_id"),
            "sub_event": cell("vw_matches___event"),
            "stage": cell("vw_matches___stage"),
            "round": cell("vw_matches___round"),
            "name_a": cell("vw_matches___name_a"),
            "name_x": cell("vw_matches___name_x"),
            "result_a_games": result_a,
            "result_x_games": result_x,
            "game_scores": cell("vw_matches___games"),
            "winner_name": cell("vw_matches___winner_name"),
            "match_year": int(yr_text),
            "scraped_at": scraped_at,
        })

    return rows


def scrape_player_matches(
    player_id: int,
    sess: requests.Session,
    year_filter: int | None = None,
) -> list[dict]:
    """Fetch all match rows for a single player_id.

    year_filter: if set, only fetch matches from that calendar year.
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_rows: list[dict] = []
    limitstart = 0

    while True:
        resp = _fetch_page(sess, player_id, limitstart, year_filter)
        soup = BeautifulSoup(resp.text, "lxml")
        page_rows = _parse_match_rows(soup, player_id, scraped_at)

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
    """Return the max match_year already loaded for this player_id, or None."""
    query = f"""
        SELECT MAX(match_year) AS max_year
        FROM `{project_id}.{dataset}.bronze_wtt_matches`
        WHERE player_id = {player_id}
    """
    try:
        rows = list(bq_client.query(query).result())
        val = rows[0].max_year if rows else None
        return int(val) if val is not None else None
    except Exception:
        return None


def load_to_bq(rows: list[dict], project_id: str, dataset: str) -> None:
    """Append match rows to BigQuery bronze table."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    bq_loader.load_dataframe(df, project_id, dataset, "bronze_wtt_matches")


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
        # Incremental: if we have prior data, only fetch this year's new matches.
        # First run (no watermark): fetch full history.
        year_filter = current_year if watermark is not None else None
        rows = scrape_player_matches(pid, sess, year_filter=year_filter)
        if rows:
            load_to_bq(rows, project_id, dataset)
            total_rows += len(rows)
            logger.info("player_id=%d scraped=%d year_filter=%s", pid, len(rows), year_filter)
        time.sleep(SCRAPE_DELAY)

    print(f"Done. Loaded {total_rows} rows to {dataset}.bronze_wtt_matches")
