# Architecture

## System Overview

WikiPulse implements a medallion architecture (bronze → silver → gold) for processing Wikipedia pageview data, fully containerized via Docker Compose.

```
flowchart LR
  subgraph NET["Docker Compose Network"]
    WS["dagster-webserver<br/>:3001"]
    DD["dagster-daemon"]
    MB["metabase<br/>:3000"]

    PG["PostgreSQL<br/>:5432<br/>(metadata + serving)"]
    VOL["Shared Volume<br/>./data<br/>(Parquet I/O)"]

    WS --> PG
    DD --> PG
    MB --> PG

    WS --- VOL
    DD --- VOL
  end

```

## Components

### Dagster (Orchestration)

Two containers running from the same image:

- **dagster-webserver**: UI and GraphQL API (port 3001)
- **dagster-daemon**: Scheduler, sensor, and run execution

Dagster manages:
- Daily partitioned assets (2026-01-01 to present) and unpartitioned assets
- Asset dependencies and materialization
- dbt integration via `dagster-dbt`
- Data quality checks via `@asset_check`

### PostgreSQL

Single instance serving three purposes:
1. **Dagster metadata**: Run history, asset catalog, schedules
2. **Staging layer**: Silver data loaded from Parquet by Dagster (`staging` schema)
3. **Gold layer**: dbt mart tables for Metabase queries (`gold` schema)

### Metabase (Visualization)

Open-source BI tool connected to PostgreSQL for interactive dashboards.

## Data Flow

```
flowchart TB
  EXT["External APIs"]

  subgraph EXTRACT["EXTRACTION"]
    PV["Wikimedia Pageviews API"]
    SUM["Wikipedia Summary API"]
    HTTPX["httpx AsyncClient<br/>(rate limited, retries)"]

    PV --> HTTPX
    SUM --> HTTPX
  end

  subgraph BRONZE["BRONZE LAYER"]
    B1["data/bronze/daily_top/year=YYYY/month=MM/day=DD.parquet"]
    B2["data/bronze/article_meta/articles.parquet"]
    BDESC["Raw API responses<br/>minimal processing"]
  end

  subgraph SILVER["SILVER LAYER"]
    S1["data/silver/pageviews/"]
    S2["data/silver/articles/"]
    SDESC["Cleaned: filtered, decoded, validated,<br/>deduplicated"]
  end

  subgraph STAGING["STAGING (PostgreSQL)"]
    STG1["staging.pageviews"]
    STG2["staging.articles"]
    STDESC["Silver data loaded into PostgreSQL<br/>by Dagster staging assets"]
  end

  subgraph GOLD["GOLD LAYER (PostgreSQL)"]
    GPG["gold.dim_articles<br/>gold.fact_daily_pageviews<br/>gold.agg_weekly_pageviews<br/>gold.trending_articles<br/>gold.weekly_movers"]
    GMODELS["dbt-postgres models:<br/>- dim_articles (dimension)<br/>- fact_daily_pageviews (incremental fact)<br/>- agg_weekly_pageviews (aggregate)<br/>- trending_articles (dashboard)<br/>- weekly_movers (dashboard)"]
  end

  MB["Metabase Dashboard<br/>&quot;The Daily Pulse&quot;"]

  EXT --> EXTRACT
  EXTRACT --> BRONZE
  BRONZE -->|"Polars transformations"| SILVER
  SILVER -->|"Dagster staging assets"| STAGING
  STAGING -->|"dbt-postgres"| GOLD

  GOLD --> MB
```

## Storage Layout

```
data/
├── bronze/
│   ├── daily_top/
│   │   └── year=2024/
│   │       └── month=01/
│   │           ├── day=01.parquet
│   │           ├── day=02.parquet
│   │           └── ...
│   └── article_meta/
│       └── articles.parquet
└── silver/
    ├── pageviews/
    │   └── year=2026/
    │       └── month=01/
    │           ├── day=01.parquet
    │           ├── day=02.parquet
    │           └── ...
    └── articles/
        └── articles.parquet

PostgreSQL (wikipulse database):
├── staging schema
│   ├── pageviews            # loaded from silver Parquet by Dagster
│   └── articles             # loaded from silver Parquet by Dagster
└── gold schema
    ├── dim_articles         # dbt mart model
    ├── fact_daily_pageviews # dbt mart model (incremental)
    ├── agg_weekly_pageviews # dbt mart model
    ├── trending_articles    # dbt mart model (dashboard)
    └── weekly_movers        # dbt mart model (dashboard)
```

## Data Models

### Bronze Schema

```
bronze_daily_top:
  - ingestion_date: DATE
  - article: STRING
  - views: INT
  - rank: INT

bronze_article_meta:
  - pageid: INT
  - title: STRING
  - description: STRING
  - extract: STRING
  - wikibase_item: STRING
  - type: STRING
  - first_seen_date: DATE
```

### Silver Schema

```
silver_pageviews:
  - ingestion_date: DATE
  - article: STRING         # URL-decoded (spaces, not underscores)
  - views: INT
  - rank: INT

silver_articles:
  - pageid: INT
  - title: STRING           # URL-decoded (spaces, not underscores)
  - description: STRING     # whitespace-stripped
  - extract: STRING         # whitespace-stripped
  - wikibase_item: STRING
  - type: STRING
  - first_seen_date: DATE
```

### Gold Schema (dbt models)

```
dim_articles (table):
  - article_id: INT (PK, from pageid)
  - article_title: STRING
  - description: STRING
  - summary: STRING
  - wikibase_item: STRING
  - article_type: STRING
  - first_seen_date: DATE

fact_daily_pageviews (incremental, merge on [article_id, view_date]):
  - article_id: INT (FK → dim_articles)
  - view_date: DATE
  - views: INT
  - rank: INT

agg_weekly_pageviews (table):
  - article_id: INT (FK → dim_articles)
  - week_start: DATE
  - total_views: INT
  - avg_daily_views: FLOAT
  - best_rank: INT
  - days_at_best_rank: INT
  - avg_rank: FLOAT
  - days_in_top_1k: INT

trending_articles (table):
  - article_title: STRING
  - latest_rank: INT
  - latest_views: INT
  - avg_rank_7d: FLOAT
  - rank_improvement: FLOAT

weekly_movers (table):
  - article_title: STRING
  - this_week_views: INT
  - prev_week_views: INT
  - view_change: INT
  - view_change_pct: FLOAT
```

## Key Design Decisions

### Why PostgreSQL for Gold?
- Single database for both transformation output and Metabase serving
- Stable serving layer with JDBC support and concurrent connections
- dbt-postgres adapter is battle-tested and well-supported
- Dagster staging assets bridge the gap from Parquet (silver) to PostgreSQL (gold)

### Why Parquet Throughout (Bronze/Silver)?
- Columnar format optimized for analytical workloads
- Compression reduces storage footprint
- Schema embedded in files
- Portable across tools (DuckDB, Polars, Spark, etc.)

### Why Daily Partitions?
- Natural grain of the source data (applies to `bronze_daily_top`)
- Enables efficient incremental processing
- Easy backfill via Dagster partition selector
- ~1,000 rows per partition keeps files small
- `bronze_article_meta` is **unpartitioned** — it's an append-only registry that scans all bronze files and fetches metadata only for new titles, avoiding redundant API calls during backfills

## API Integration

### Wikimedia Pageviews API
- Rate limit: ~100 req/s (well within limits)
- No authentication required
- User-Agent header required

### Implementation Details
- `httpx.AsyncClient` for async HTTP
- Semaphore-based rate limiting (configurable)
- Exponential backoff with jitter on 429/5xx
- Circuit breaker pattern for sustained failures

## Security Considerations

- No secrets in code (use environment variables)
- PostgreSQL credentials via Docker Compose env
- No external network access required except API calls
