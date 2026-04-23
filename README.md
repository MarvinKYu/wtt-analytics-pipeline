# WTT Analytics Pipeline

A production-style ELT pipeline that scrapes World Table Tennis (WTT) and ITTF match data,
applies a Glicko-lite rating engine, and surfaces over/underranked analysis in Looker Studio.

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 (Docker Compose, LocalExecutor) |
| Ingestion | Python 3.11 (requests, BeautifulSoup) |
| Data Warehouse | Google BigQuery |
| Transformation | dbt Core 1.8 + dbt-bigquery |
| Rating Engine | Python (Glicko-lite, ported from RallyBase) |
| Visualization | Looker Studio |

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full diagram and design decisions.

**Data flow:** ITTF scraper → BigQuery Bronze → dbt staging/intermediate → Glicko-lite replay → dbt marts → Looker Studio

## Setup

Follow [SETUP.md](SETUP.md) top to bottom. Prerequisites: Python 3.11+, Docker Desktop, Git.

```bash
# Quick start (after GCP setup in SETUP.md §2)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in GCP_PROJECT_ID and GCP_KEYFILE_PATH

cd dbt && dbt debug --profiles-dir .  # verify BigQuery connection

cd ../airflow
docker compose up airflow-init        # first-time only
docker compose up -d                  # http://localhost:8080
```

## v2 Roadmap

| Feature | Description |
|---|---|
| Score modifier validation | Enable game-score modifier and validate on WTT data |
| Parameter retuning | Re-sweep base_k and inactivity_rd_growth_c for elite player distribution |
| Time-series mart | Weekly rating + rank per player (`mart_player_timeseries`) |
| Match prediction engine | Head-to-head win probabilities (`mart_match_predictions`) |
| Looker Studio pages 3–4 | Player Profile and Match Predictions pages |
| Full Glicko-2 upgrade | Full sigma update rules if Brier improves on WTT validation set |
