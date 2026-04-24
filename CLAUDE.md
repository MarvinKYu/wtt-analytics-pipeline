# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WTT Analytics Pipeline — an ELT pipeline that scrapes World Table Tennis (WTT) / ITTF match data into BigQuery, transforms it via dbt Core, runs a Glicko-lite rating engine in Python, and surfaces results in Looker Studio. The data source is `results.ittf.link`.

## Common Commands

### Python environment
```bash
python -m venv .venv
source .venv/bin/activate          # Mac/Linux
.venv\Scripts\activate             # Windows
pip install -r requirements.txt
```

### dbt (run from the `dbt/` directory)
```bash
dbt debug --profiles-dir .         # verify BigQuery connection
dbt run --profiles-dir .           # run all models
dbt run --profiles-dir . -s staging  # run a single layer
dbt test --profiles-dir .          # run schema tests
```

### Airflow (Docker Compose, run from `airflow/`)
```bash
docker compose up airflow-init     # first-time DB init + admin user creation (run once)
docker compose up -d               # start all services (UI at http://localhost:8080)
docker compose down                # stop
```

### Environment setup
```bash
cp .env.example .env               # then fill in GCP_PROJECT_ID, GCP_KEYFILE_PATH
```

## Architecture

### Data Flow
```
results.ittf.link
  → ingestion/ (Python scrapers, weekly Airflow DAG)
  → BigQuery wtt_raw dataset (Bronze, append-only tables: bronze_ittf_rankings, bronze_wtt_matches, bronze_ittf_ranking_history)
  → dbt staging models (Silver views: stg_matches, stg_rankings, stg_ranking_history)
  → dbt intermediate models (Silver views: int_player_match_history)
  → rating_engine/replay.py (Python Glicko-lite replay → writes mart_player_ratings_raw)
  → dbt mart models (Gold tables: mart_player_ratings, mart_rating_vs_ranking)
  → Looker Studio dashboard
```

### Medallion Layers

| Layer | Prefix | Dataset | Materialization |
|---|---|---|---|
| Bronze | `bronze_` | `wtt_raw` | Tables (raw, append-only) |
| Silver staging | `stg_` | `wtt` | Views |
| Silver intermediate | `int_` | `wtt` | Views |
| Gold | `mart_` | `wtt` | Tables |

### Key Design Decisions

**Rating engine runs outside dbt** — Glicko-lite replay is a stateful, chronologically-ordered computation. It runs as a Python Airflow task between the intermediate and mart layers, writing `mart_player_ratings_raw` to BigQuery, which mart models then read.

**Player-centric ingestion** — data is scraped per-player (not per-tournament). The ITTF ranking table provides the seed list of active player IDs; each ID maps to profile, match history, and ranking history endpoints.

**Watermark-based incremental ingestion** — each weekly run checks the latest `match_date` already loaded per `player_id` and only fetches newer matches, keeping Airflow run times under 10 minutes.

**Score modifier disabled in v1** — `enable_score_modifier = False` in `rating_engine/glicko.py`. The v1 parameters are locked (see constants in that file); do not change them without re-validation.

### Airflow DAGs

- `wtt_ingest` (schedule: every Monday 06:00 UTC) — scrape rankings + match history + ranking history → load to Bronze → trigger `wtt_transform`
- `wtt_transform` (schedule: None, triggered only) — dbt staging → dbt intermediate → rating engine replay → dbt marts

### Glicko-lite v1 Parameters (locked)

```python
DEFAULT_RATING = 1200.0
DEFAULT_RD = 300.0
DEFAULT_SIGMA = 0.06
MIN_RATING = 100.0
MAX_RD = 350.0
MIN_RD = 40.0
BASE_K = 120.0
JUNIOR_RD_MIN = 220.0
INACTIVITY_RD_GROWTH_C = 100.0
```

## Environment Variables

Required in `.env` (never committed):
```
GCP_PROJECT_ID=         # e.g. wtt-analytics-461203
GCP_KEYFILE_PATH=       # absolute path to gcp-credentials.json
BQ_RAW_DATASET=wtt_raw
BQ_TRANSFORMED_DATASET=wtt
ITTF_BASE_URL=https://results.ittf.link
SCRAPE_DELAY_SECONDS=1.5
AIRFLOW_UID=50000
```

`gcp-credentials.json` (GCP service account key) must never be committed — it is gitignored.

## dbt Configuration

`dbt/profiles.yml` reads credentials from environment variables. Always run dbt with `--profiles-dir .` from the `dbt/` directory so it uses the local profiles file rather than `~/.dbt/profiles.yml`.

## ITTF Site Scraping Notes

`results.ittf.link` uses the **Fabrik CMS** component. Key facts for writing scrapers:

- **Browser User-Agent required** — the site returns 403 to the default `python-requests` UA. Use a Chrome UA string.
- **Pagination keys are list-specific** — each Fabrik list has its own numbered `limitstartN` param. Rankings: men's = `limitstart57`, women's = `limitstart58`. Always inspect the "page 2" href before coding pagination.
- **Column classes carry a gender prefix** — men's: `fab_rank_ms___*`, women's: `fab_rank_ws___*`. Use CSS `[class*="___ColumnName"]` substring selectors to write one parser for both.
- **Player IDs** are in the `vw_profiles___player_id_raw` query param of name-cell link hrefs.
- **Page size** is 50 rows. Terminate pagination when a page returns fewer than 50 rows.

## v2 Roadmap

Planned but out of scope for v1: score modifier validation on WTT data, parameter retuning for elite player distributions, `mart_player_timeseries` (weekly rating + rank per player), match prediction engine, and Looker Studio Player Profile / Predictions pages.
