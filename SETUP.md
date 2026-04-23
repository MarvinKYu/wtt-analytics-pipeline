# WTT Analytics Pipeline — Complete Setup Guide

This document is written for Claude Code in VS Code. Follow it top to bottom.
Every command, file path, and config value needed to get the project running from
zero is included here. Nothing is assumed to be pre-installed except Python 3.11+,
Docker Desktop, and Git.

---

## Table of Contents

1. [Repo structure to scaffold](#1-repo-structure-to-scaffold)
2. [GCP project + BigQuery setup](#2-gcp-project--bigquery-setup)
3. [Local environment setup](#3-local-environment-setup)
4. [dbt Core setup](#4-dbt-core-setup)
5. [Airflow setup (Docker Compose)](#5-airflow-setup-docker-compose)
6. [Environment variables](#6-environment-variables)
7. [File stubs to create](#7-file-stubs-to-create)
8. [Verification checklist](#8-verification-checklist)
9. [v2 roadmap (for README)](#9-v2-roadmap-for-readme)

---

## 1. Repo Structure to Scaffold

Create exactly this directory tree inside `wtt_analytics_pipeline/`:

```
wtt_analytics_pipeline/
│
├── SETUP.md                        ← this file
├── README.md                       ← to be created
├── .gitignore
├── .env                            ← never committed; holds secrets
├── .env.example                    ← committed; shows required keys without values
├── requirements.txt
│
├── ingestion/                      ← Python scrapers
│   ├── __init__.py
│   ├── scrape_rankings.py          ← scrapes ITTF rankings pages → bronze_ittf_rankings
│   ├── scrape_match_history.py     ← scrapes per-player match history → bronze_wtt_matches
│   ├── scrape_ranking_history.py   ← scrapes per-player ranking history over time
│   └── bq_loader.py                ← shared BigQuery load utility (append / replace)
│
├── rating_engine/                  ← Python port of rallybase-glicko.ts
│   ├── __init__.py
│   ├── glicko.py                   ← core algorithm (port of rallybase-glicko.ts)
│   └── replay.py                   ← replays match history through glicko.py, writes output
│
├── dbt/                            ← dbt Core project
│   ├── dbt_project.yml
│   ├── profiles.yml                ← BigQuery connection config (references env vars)
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/                ← Silver layer: clean + type raw tables
│   │   │   ├── stg_matches.sql
│   │   │   ├── stg_rankings.sql
│   │   │   ├── stg_ranking_history.sql
│   │   │   └── schema.yml
│   │   │
│   │   ├── intermediate/           ← Silver layer: enriched joins
│   │   │   ├── int_player_match_history.sql
│   │   │   └── schema.yml
│   │   │
│   │   └── marts/                  ← Gold layer: final analytical outputs
│   │       ├── mart_player_ratings.sql          ← reads from rating_engine output table
│   │       ├── mart_rating_vs_ranking.sql        ← over/underranked analysis
│   │       ├── mart_player_timeseries.sql        ← weekly rating + rank per player (v2)
│   │       ├── mart_match_predictions.sql        ← head-to-head win probabilities (v2)
│   │       └── schema.yml
│   │
│   ├── tests/
│   │   └── generic/
│   │       └── assert_winner_nonnegative.sql
│   │
│   └── macros/
│       └── generate_schema_name.sql
│
├── airflow/
│   ├── docker-compose.yml          ← Airflow 2.x with LocalExecutor
│   ├── Dockerfile                  ← extends apache/airflow, installs requirements
│   │
│   └── dags/
│       ├── wtt_ingest_dag.py       ← DAG 1: scrape → load bronze → trigger transform
│       └── wtt_transform_dag.py    ← DAG 2: dbt run (can run independently)
│
└── docs/
    └── architecture.md             ← architecture diagram in Mermaid + written explanation
```

---

## 2. GCP Project + BigQuery Setup

### 2.1 Create a Google Cloud account and project

1. Go to https://console.cloud.google.com
2. Sign in with your Google account (use a personal Gmail — same account you'll use for Looker Studio later)
3. Click **Select a project** → **New Project**
4. Name it `wtt-analytics` (or similar — note the auto-generated **Project ID**, e.g. `wtt-analytics-461203`. You'll need this exact string.)
5. Click **Create**

> GCP gives you a $300 free credit for 90 days AND a permanent free tier. BigQuery's permanent free tier includes 10 GB storage and 1 TB of queries/month — your entire dataset will be well under 100 MB. You will not be charged.

### 2.2 Enable the BigQuery API

1. In the GCP console, navigate to **APIs & Services → Library**
2. Search for **BigQuery API** and click **Enable**

### 2.3 Create the BigQuery dataset

1. In the GCP console, navigate to **BigQuery**
2. In the left panel, click the three-dot menu next to your project ID → **Create dataset**
3. Set **Dataset ID** to `wtt_raw` — this is the Bronze layer
4. Set **Location** to `US` (multi-region)
5. Click **Create dataset**
6. Repeat to create a second dataset named `wtt` — this is the Silver + Gold layer (dbt writes here)

### 2.4 Create a service account for programmatic access

This is the credential your Python scripts and dbt will use to authenticate.

1. Navigate to **IAM & Admin → Service Accounts**
2. Click **Create Service Account**
3. Name it `wtt-pipeline-sa`, click **Create and Continue**
4. Grant it the role **BigQuery Admin** (for a personal project this is fine; in production you'd scope it more tightly)
5. Click **Done**
6. Click the service account email you just created → **Keys** tab → **Add Key → Create new key → JSON**
7. Download the JSON file. Save it to your repo root as `gcp-credentials.json`

> **Important:** `gcp-credentials.json` must NEVER be committed to Git. It is already in the `.gitignore` stub below.

### 2.5 Note your project ID

Open `gcp-credentials.json` and find the `"project_id"` field. Copy it — you'll use it in `.env`.

---

## 3. Local Environment Setup

### 3.1 Python virtual environment

Run from the repo root:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate
```

### 3.2 Install Python dependencies

Create `requirements.txt` with the following content (stub provided in Section 7),
then run:

```bash
pip install -r requirements.txt
```

### 3.3 Docker Desktop

Airflow runs in Docker. Install Docker Desktop from https://www.docker.com/products/docker-desktop
and confirm it's running before proceeding to Section 5.

```bash
docker --version   # should return Docker version 24.x or later
```

---

## 4. dbt Core Setup

### 4.1 dbt profiles.yml

dbt looks for `profiles.yml` at `~/.dbt/profiles.yml` by default. For this project,
we keep it inside the `dbt/` folder and reference it explicitly. The file should
read from environment variables so credentials are never hardcoded.

Create `dbt/profiles.yml` with this content:

```yaml
wtt_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: wtt
      keyfile: "{{ env_var('GCP_KEYFILE_PATH') }}"
      location: US
      timeout_seconds: 300
      threads: 4
```

### 4.2 dbt_project.yml

Create `dbt/dbt_project.yml`:

```yaml
name: wtt_analytics
version: '1.0.0'
config-version: 2

profile: wtt_analytics

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  wtt_analytics:
    staging:
      +schema: wtt
      +materialized: view
    intermediate:
      +schema: wtt
      +materialized: view
    marts:
      +schema: wtt
      +materialized: table
```

### 4.3 Run dbt to verify connection

From the `dbt/` directory:

```bash
cd dbt
dbt debug --profiles-dir .
```

You should see `Connection test: OK`. If you get an auth error, verify `GCP_KEYFILE_PATH`
in your `.env` points to the absolute path of `gcp-credentials.json`.

---

## 5. Airflow Setup (Docker Compose)

### 5.1 docker-compose.yml

Create `airflow/docker-compose.yml`. This sets up Airflow 2.9 with LocalExecutor
(no Redis/Celery needed for a single-machine project):

```yaml
version: '3.8'

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    GCP_PROJECT_ID: ${GCP_PROJECT_ID}
    GCP_KEYFILE_PATH: /opt/airflow/gcp-credentials.json
    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/gcp-credentials.json
    DBT_PROJECT_DIR: /opt/airflow/dbt
  volumes:
    - ./dags:/opt/airflow/dags
    - ../dbt:/opt/airflow/dbt
    - ../gcp-credentials.json:/opt/airflow/gcp-credentials.json:ro
    - ../ingestion:/opt/airflow/ingestion
    - ../rating_engine:/opt/airflow/rating_engine
    - airflow-logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init &&
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com
    restart: on-failure

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

volumes:
  postgres-db-volume:
  airflow-logs:
```

### 5.2 Airflow Dockerfile

Create `airflow/Dockerfile`:

```dockerfile
FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
COPY ../requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### 5.3 Start Airflow

```bash
cd airflow
docker compose up airflow-init   # run once to initialize DB + create admin user
docker compose up -d             # start all services in background
```

Open http://localhost:8080 in your browser. Login: `admin` / `admin`.

> The DAGs will appear paused by default. Once your scraper code is working,
> enable them via the Airflow UI toggle or `airflow dags unpause <dag_id>`.

---

## 6. Environment Variables

### 6.1 Create .env

Create `.env` in the repo root (never commit this):

```env
# GCP
GCP_PROJECT_ID=your-project-id-here        # e.g. wtt-analytics-461203
GCP_KEYFILE_PATH=/absolute/path/to/wtt_analytics_pipeline/gcp-credentials.json

# BigQuery datasets
BQ_RAW_DATASET=wtt_raw
BQ_TRANSFORMED_DATASET=wtt

# Scraper config
ITTF_BASE_URL=https://results.ittf.link
SCRAPE_DELAY_SECONDS=1.5                   # polite delay between requests

# Airflow
AIRFLOW_UID=50000
```

### 6.2 Create .env.example

Create `.env.example` in the repo root (this IS committed — it's a template):

```env
GCP_PROJECT_ID=
GCP_KEYFILE_PATH=
BQ_RAW_DATASET=wtt_raw
BQ_TRANSFORMED_DATASET=wtt
ITTF_BASE_URL=https://results.ittf.link
SCRAPE_DELAY_SECONDS=1.5
AIRFLOW_UID=50000
```

---

## 7. File Stubs to Create

Claude Code: create each of these files with the stub content shown.
Do not implement logic yet — just structure and docstrings.

### 7.1 .gitignore

```gitignore
# Secrets
.env
gcp-credentials.json
*.json.key

# Python
.venv/
__pycache__/
*.pyc
*.pyo
.pytest_cache/

# dbt
dbt/target/
dbt/dbt_packages/
dbt/logs/

# Airflow
airflow/logs/
airflow/plugins/

# IDE
.vscode/settings.json
.idea/

# OS
.DS_Store
Thumbs.db
```

### 7.2 requirements.txt

```
# Scraping
requests==2.31.0
beautifulsoup4==4.12.3
lxml==5.2.1

# BigQuery
google-cloud-bigquery==3.21.0
google-auth==2.29.0
pandas==2.2.2
pandas-gbq==0.23.0
pyarrow==16.0.0

# dbt
dbt-core==1.8.3
dbt-bigquery==1.8.2

# Airflow (installed separately via Docker, listed here for reference)
# apache-airflow==2.9.1

# Utils
python-dotenv==1.0.1
tenacity==8.3.0       # retry logic for scraper
tqdm==4.66.4          # progress bars for bulk scraping
```

### 7.3 ingestion/scrape_rankings.py

```python
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
```

### 7.4 ingestion/scrape_match_history.py

```python
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
```

### 7.5 ingestion/scrape_ranking_history.py

```python
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
```

### 7.6 ingestion/bq_loader.py

```python
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
```

### 7.7 rating_engine/glicko.py

```python
"""
glicko.py

Python port of the RallyBase Glicko-lite rating engine (rallybase-glicko.ts).
Validated on 359,724 USATT matches. Brier score: 0.176 (vs 0.191 USATT baseline).

v1 Release Parameters (locked — do not change without re-validation):
  default_rating           = 1200.0
  default_rd               = 300.0
  default_sigma            = 0.06
  min_rating               = 100.0
  max_rd                   = 350.0
  min_rd                   = 40.0
  base_k                   = 120.0
  junior_rd_min            = 220.0
  inactivity_rd_growth_c   = 100.0
  winner_nonnegative       = True
  enable_score_modifier    = False

See: v1_release_report.md for full algorithm spec and design rationale.
"""

import math
from dataclasses import dataclass, field

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_RATING = 1200.0
DEFAULT_RD = 300.0
DEFAULT_SIGMA = 0.06
MIN_RATING = 100.0
MAX_RD = 350.0
MIN_RD = 40.0
BASE_K = 120.0
JUNIOR_RD_MIN = 220.0
INACTIVITY_RD_GROWTH_C = 100.0


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class PlayerState:
    player_id: int
    rating: float = DEFAULT_RATING
    rd: float = DEFAULT_RD
    sigma: float = DEFAULT_SIGMA
    matches_played: int = 0
    last_active_day: int | None = None
    is_junior: bool = False


@dataclass
class MatchResult:
    winner_id: int
    loser_id: int
    match_day: int          # integer days since epoch (epoch = first match date in dataset)
    match_type: str = "tournament"   # "tournament" | "casual_rated" | "casual_unrated"


# ── Core functions ──────────────────────────────────────────────────────────────

def clamp(value: float, lo: float, hi: float) -> float:
    raise NotImplementedError

def inflate_rd_for_inactivity(rd: float, days_inactive: int, is_junior: bool) -> float:
    """rd' = clamp(sqrt(rd^2 + c * days_inactive), MIN_RD, MAX_RD), with junior floor."""
    raise NotImplementedError

def expected_win_prob(rating_a: float, rating_b: float) -> float:
    """Standard Elo logistic: 1 / (1 + 10^(-(rA - rB) / 400))"""
    raise NotImplementedError

def effective_k(player: PlayerState) -> float:
    """Scales BASE_K by RD, sigma, new-player boost, junior boost."""
    raise NotImplementedError

def update_match(
    winner: PlayerState,
    loser: PlayerState,
    match: MatchResult,
) -> tuple[PlayerState, PlayerState]:
    """
    Process one match. Returns updated (winner, loser) states.
    Applies: inactivity inflation → win prob → effective_k → base delta
             → winner non-negative clamp → rating floor → RD/sigma update.
    """
    raise NotImplementedError
```

### 7.8 rating_engine/replay.py

```python
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
```

### 7.9 dbt/models/staging/stg_matches.sql

```sql
-- stg_matches.sql
-- Silver layer: cleans and types raw match rows from bronze_wtt_matches.
-- Parses game score string into structured fields.
-- Standardizes event_type to WTT | ITTF | OTHER.

with source as (
    select * from {{ source('wtt_raw', 'bronze_wtt_matches') }}
),

cleaned as (
    select
        -- TODO: implement cleaning transformations
        *
    from source
)

select * from cleaned
```

### 7.10 dbt/models/staging/stg_rankings.sql

```sql
-- stg_rankings.sql
-- Silver layer: cleans and types bronze_ittf_rankings.
-- One row per player per snapshot_date.

with source as (
    select * from {{ source('wtt_raw', 'bronze_ittf_rankings') }}
),

cleaned as (
    select
        -- TODO: implement cleaning transformations
        *
    from source
)

select * from cleaned
```

### 7.11 dbt/models/intermediate/int_player_match_history.sql

```sql
-- int_player_match_history.sql
-- One row per player per match.
-- Joins stg_matches to stg_rankings to attach pre-match rank context.
-- Used as the input to rating_engine/replay.py.

with matches as (
    select * from {{ ref('stg_matches') }}
),

-- TODO: pivot to one row per player per match, join ranking snapshot

final as (
    select
        -- player_id, opponent_id, match_date, event_name, event_type,
        -- player_games_won, opponent_games_won, result,
        -- player_rank_at_match_time, opponent_rank_at_match_time
        1 as placeholder  -- remove this line when implementing
    from matches
)

select * from final
```

### 7.12 dbt/models/marts/mart_player_ratings.sql

```sql
-- mart_player_ratings.sql
-- Gold layer: final player ratings as computed by the Python rating engine.
-- Reads from mart_player_ratings_raw (written by replay.py).
-- Joins with latest ITTF rank for the over/underranked analysis.

with ratings as (
    select * from {{ source('wtt', 'mart_player_ratings_raw') }}
),

latest_rankings as (
    select *
    from {{ ref('stg_rankings') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_rankings') }})
),

-- TODO: join and compute implied_rank from rating ordering

final as (
    select
        -- player_id, player_name, nationality, gender,
        -- rating, rd, sigma, matches_played,
        -- ittf_rank, ittf_points,
        -- implied_rank (row_number() over (order by rating desc)),
        -- divergence (ittf_rank - implied_rank)
        1 as placeholder  -- remove when implementing
    from ratings
)

select * from final
```

### 7.13 dbt/models/marts/mart_rating_vs_ranking.sql

```sql
-- mart_rating_vs_ranking.sql
-- Gold layer: the headline output — over/underranked analysis.
-- Filters to players with RD < 200 (high-confidence ratings only).
-- Positive divergence = overranked by ITTF vs model.
-- Negative divergence = underranked by ITTF vs model.

with base as (
    select * from {{ ref('mart_player_ratings') }}
),

filtered as (
    select *
    from base
    where rd < 200  -- only surface high-confidence ratings
),

-- TODO: add divergence categorization and percentile bands

final as (
    select
        -- player_id, player_name, nationality,
        -- ittf_rank, implied_rank, divergence,
        -- divergence_category ('overranked' | 'underranked' | 'aligned'),
        -- rating, rd
        1 as placeholder  -- remove when implementing
    from filtered
)

select * from final
```

### 7.14 airflow/dags/wtt_ingest_dag.py

```python
"""
wtt_ingest_dag.py

DAG 1: Weekly ingestion pipeline.
Schedule: Every Monday at 06:00 UTC.

Tasks:
  1. scrape_rankings      → load to bronze_ittf_rankings
  2. scrape_match_history → load to bronze_wtt_matches (watermark-incremental)
  3. scrape_ranking_history → load to bronze_ittf_ranking_history
  4. trigger_transform    → triggers wtt_transform_dag on success
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wtt_ingest",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1",  # every Monday at 06:00 UTC
    catchup=False,
    tags=["wtt", "ingestion"],
) as dag:

    # TODO: implement task functions and wire up operators
    pass
```

### 7.15 airflow/dags/wtt_transform_dag.py

```python
"""
wtt_transform_dag.py

DAG 2: dbt transformation + rating engine.
Triggered by wtt_ingest_dag on success, or run manually.

Tasks:
  1. dbt_run_staging       → runs dbt staging models
  2. dbt_run_intermediate  → runs dbt intermediate models
  3. run_rating_engine     → calls replay.py, writes mart_player_ratings_raw
  4. dbt_run_marts         → runs dbt mart models (depends on rating engine output)
  5. notify_success        → logs completion summary
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wtt_transform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # triggered only, not scheduled
    catchup=False,
    tags=["wtt", "transform", "dbt"],
) as dag:

    # TODO: implement task functions and wire up operators
    pass
```

### 7.16 docs/architecture.md

````markdown
# WTT Analytics Pipeline — Architecture

## Overview

A production-style ELT pipeline ingesting World Table Tennis (WTT) and ITTF
match results into BigQuery, transforming them through a Medallion architecture
using dbt Core, and surfacing findings in a Looker Studio dashboard.

## Architecture Diagram

```mermaid
flowchart TD
    A[results.ittf.link\nITTF Rankings + Match History] -->|Python scraper\nweekly Airflow DAG| B[(BigQuery\nBronze Layer\nwtt_raw dataset)]
    B -->|dbt staging models| C[(BigQuery\nSilver Layer\nstg_ views)]
    C -->|dbt intermediate models| D[(BigQuery\nSilver Layer\nint_ views)]
    D -->|Python rating engine\nGlicko-lite replay| E[(BigQuery\nGold Layer\nmart_player_ratings_raw)]
    E -->|dbt mart models| F[(BigQuery\nGold Layer\nmart_ tables)]
    F -->|Native connector| G[Looker Studio\nDashboard]
```

## Layer Definitions

| Layer | dbt prefix | BigQuery dataset | Materialization |
|---|---|---|---|
| Bronze | `bronze_` | `wtt_raw` | Tables (raw, append-only) |
| Silver - Staging | `stg_` | `wtt` | Views |
| Silver - Intermediate | `int_` | `wtt` | Views |
| Gold - Marts | `mart_` | `wtt` | Tables |

## Key Design Decisions

### Watermark-based incremental ingestion
Rather than full reloads, each weekly run checks the latest `match_date`
already loaded per `player_id` and only fetches newer matches. This keeps
Airflow run times under 10 minutes for weekly refreshes.

### Player-centric ingestion pattern
Data is scraped per-player rather than per-tournament. The ITTF ranking table
provides the seed list of all active player IDs. Each ID maps deterministically
to profile, match history, and ranking history endpoints.

### Rating engine outside dbt
The Glicko-lite rating replay is a stateful, ordered computation — not a SQL
transformation. It runs as a Python Airflow task between the intermediate and
mart dbt layers, writing its output to BigQuery as a raw table that mart models
then read from.

### Score modifier disabled in v1
Per RallyBase v1 release spec: the game-score modifier was disabled due to
data quality concerns in USATT data. WTT match data includes individual game
scores (e.g. 11:7, 9:11, 11:3) which are cleaner — enabling and validating
the score modifier on WTT data is a planned v2 experiment.

## v2 Roadmap

See SETUP.md Section 9.
````

---

## 8. Verification Checklist

Work through these in order after scaffolding. Each should pass before moving on.

- [ ] `python -m venv .venv && pip install -r requirements.txt` completes with no errors
- [ ] `gcp-credentials.json` is present in repo root and listed in `.gitignore`
- [ ] BigQuery datasets `wtt_raw` and `wtt` exist in your GCP project
- [ ] `GCP_PROJECT_ID` and `GCP_KEYFILE_PATH` are set correctly in `.env`
- [ ] `cd dbt && dbt debug --profiles-dir .` returns `Connection test: OK`
- [ ] `docker compose up airflow-init` completes with exit code 0
- [ ] `docker compose up -d` starts all containers; Airflow UI loads at http://localhost:8080
- [ ] Both DAGs (`wtt_ingest`, `wtt_transform`) appear in Airflow UI (paused)
- [ ] `git init && git add . && git commit -m "chore: initial repo scaffold"` succeeds
- [ ] Confirm `gcp-credentials.json` does NOT appear in `git status` (should be gitignored)

---

## 9. v2 Roadmap (for README)

Document these as explicitly planned but out of scope for v1:

| Feature | Description |
|---|---|
| **Score modifier validation** | Enable the game-score modifier (disabled in v1) and measure Brier improvement on WTT data. WTT game scores are cleaner than USATT — this is the primary validation experiment for RallyBase v2. |
| **Parameter retuning** | Re-run `base_k` and `inactivity_rd_growth_c` sweep against WTT population. Elite players have more concentrated skill distributions and less frequent match schedules than USATT — parameters will likely need adjustment. |
| **Time-series mart** | `mart_player_timeseries`: weekly rating + RD + ITTF rank per player. Feeds the Player Profile page in Looker Studio and enables longitudinal over/underranked analysis. |
| **Match prediction engine** | `mart_match_predictions`: for all upcoming WTT events, pre-compute `expectedWinProb()` for scheduled head-to-heads. Feeds the Predictions page in Looker Studio. |
| **Looker Studio pages 3–4** | Player Profile (rating trajectory over time) and Match Predictions pages. Dependent on time-series mart and prediction engine. |
| **Full Glicko-2 upgrade** | Swap Glicko-lite for full Glicko-2 sigma update rules if prediction improves on WTT validation set. |

---

*Last updated: April 2026*
*Repo: `wtt_analytics_pipeline`*
*Author: Marvin Yu*
