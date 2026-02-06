# WikiPulse: Wikipedia Cultural Trends Pipeline

A data engineering portfolio project that ingests daily top-viewed Wikipedia articles from the Wikimedia Pageviews API, transforms data through a medallion architecture (bronze → silver → gold), and publishes analytical datasets to HuggingFace.

## Overview

WikiPulse answers questions like:
- What does the world pay attention to, and how does that shift over time?
- Can we detect breaking news events purely from pageview spikes?
- Which articles dominate Wikipedia traffic consistently vs. appear in bursts?

## Architecture

```mermaid
flowchart LR
    A[Wikimedia API] --> B[Dagster]
    B --> C[Bronze (Parquet)]
    C --> D[Silver (Polars)]
    D --> E[Gold (dbt/DuckDB)]
    E --> F[HuggingFace]

    E --> G[PostgreSQL]
    G --> H[Metabase]

```

The entire stack runs locally via Docker Compose.

## Tech Stack

| Component | Tool |
|-----------|------|
| Extraction | Python (`httpx`) |
| Orchestration | Dagster |
| Storage | Parquet on Docker volume |
| Analytical Engine | DuckDB |
| Transformation | dbt Core + dbt-duckdb |
| Serving Layer | PostgreSQL |
| Visualization | Metabase |
| Publishing | HuggingFace Hub |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- (Optional) HuggingFace account for dataset publishing

### Setup

```bash
# Clone the repository
git clone <repo-url>
cd wikipulse

# Start the stack
docker compose up -d

# Access services
# Dagster UI: http://localhost:3001
# Metabase:   http://localhost:3000
```

### Running the Pipeline

1. Open Dagster UI at `http://localhost:3001`
2. Navigate to Assets
3. Materialize assets in order: bronze → silver → gold
4. For historical backfill, use the partition selector

## Project Structure

```
wikipulse/
├── orchestrator/          # Dagster assets and resources
│   ├── assets/           # Bronze, silver, gold, publish assets
│   ├── resources/        # API clients
│   └── checks/           # Data quality checks
├── dbt_src/              # dbt transformation project
│   ├── models/staging/   # Staging models
│   └── models/marts/     # Dimension, fact, aggregate models
├── notebooks/            # Exploratory analysis
├── tests/                # Unit and integration tests
├── data/                 # Parquet storage (gitignored)
└── docs/                 # Documentation
```

## Data Sources

### Primary: Top Articles Per Day
```
GET https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{YYYY}/{MM}/{DD}
```
Returns top ~1,000 most-viewed articles daily. Available from July 2015 to present.

### Optional: Article Metadata
```
GET https://en.wikipedia.org/api/rest_v1/page/summary/{title}
```
Returns article descriptions, extracts, and Wikidata IDs.

## Data Model

- **Bronze**: Raw API responses as Parquet
- **Silver**: Cleaned data (filtered, decoded, validated)
- **Gold**: Analytical models (dim_articles, fact_daily_pageviews, agg_weekly_summary)

## Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# Run dbt locally
cd dbt_src
dbt run
dbt test
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Roadmap](docs/ROADMAP.md)
- [Data Dictionary](docs/DATA_DICTIONARY.md)

## License

MIT License. Wikimedia data is public domain (CC0).
