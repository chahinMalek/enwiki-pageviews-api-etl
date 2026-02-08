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

### DuckDB (Analytical Engine)

Runs embedded within the Dagster process—no separate container. Used for:
- Reading Parquet files efficiently
- dbt transformations (dbt-duckdb adapter)
- Exporting gold layer Parquet

### PostgreSQL

Single instance serving dual purposes:
1. **Dagster metadata**: Run history, asset catalog, schedules
2. **Serving layer**: Materialized gold models for Metabase queries

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

  subgraph GOLD["GOLD LAYER"]
    GPARQ["data/gold/*.parquet"]
    GPG["PostgreSQL tables"]
    GMODELS["Models:<br/>- dim_articles (dimension)<br/>- fact_daily_pageviews (incremental fact)<br/>- agg_weekly_summary (aggregate)"]

    GPARQ <--> GPG
  end

  HF["HuggingFace Datasets<br/>Versioned public dataset"]
  MB["Metabase Dashboard<br/>&quot;The Daily Pulse&quot;"]

  EXT --> EXTRACT
  EXTRACT --> BRONZE
  BRONZE -->|"Polars transformations"| SILVER
  SILVER -->|"dbt + DuckDB"| GOLD

  GOLD --> HF
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
├── silver/
│   ├── pageviews/
│   │   └── year=2026/
│   │       └── month=01/
│   │           ├── day=01.parquet
│   │           ├── day=02.parquet
│   │           └── ...
│   └── articles/
│       └── articles.parquet
└── gold/
    ├── dim_articles.parquet
    ├── fact_daily_pageviews.parquet
    └── agg_weekly_summary.parquet
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
dim_articles:
  - article_id: STRING (PK, surrogate from title)
  - article_title: STRING
  - description: STRING
  - extract: STRING
  - wikibase_item: STRING
  - article_type: STRING
  - first_seen_date: DATE

fact_daily_pageviews:
  - article_id: STRING (FK)
  - view_date: DATE
  - views: INT
  - rank: INT

agg_weekly_summary:
  - article_id: STRING (FK)
  - week_start: DATE
  - total_views: INT
  - avg_daily_views: FLOAT
  - best_rank: INT
  - days_in_top_1000: INT
```

## Key Design Decisions

### Why DuckDB Embedded?
- No network overhead for analytical queries
- Native Parquet support without external dependencies
- Handles full dataset (~3-4M rows) in milliseconds
- dbt-duckdb adapter provides seamless integration

### Why Dual PostgreSQL + DuckDB?
- DuckDB: Fast analytical queries during transformation
- PostgreSQL: Stable serving layer for Metabase (JDBC support, concurrent connections)

### Why Parquet Throughout?
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
- HuggingFace token optional and loaded from env
- PostgreSQL credentials via Docker Compose env
- No external network access required except API calls
