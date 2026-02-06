# Architecture

## System Overview

WikiPulse implements a medallion architecture (bronze → silver → gold) for processing Wikipedia pageview data, fully containerized via Docker Compose.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Docker Compose Network                             │
│                                                                             │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐      │
│  │  dagster-webserver│    │  dagster-daemon  │    │    metabase      │      │
│  │     :3001        │    │                  │    │     :3000        │      │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘      │
│           │                       │                       │                 │
│           │                       │                       │                 │
│           └───────────┬───────────┘                       │                 │
│                       │                                   │                 │
│                       ▼                                   │                 │
│              ┌────────────────┐                           │                 │
│              │   PostgreSQL   │◄──────────────────────────┘                 │
│              │     :5432      │                                             │
│              │  (metadata +   │                                             │
│              │   serving)     │                                             │
│              └────────────────┘                                             │
│                                                                             │
│              ┌────────────────┐                                             │
│              │  Shared Volume │                                             │
│              │    ./data      │                                             │
│              │  (Parquet I/O) │                                             │
│              └────────────────┘                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### Dagster (Orchestration)

Two containers running from the same image:

- **dagster-webserver**: UI and GraphQL API (port 3001)
- **dagster-daemon**: Scheduler, sensor, and run execution

Dagster manages:
- Daily partitioned assets (2015-07-01 to present)
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
                    External APIs
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     EXTRACTION                               │
│  Wikimedia Pageviews API ──► httpx AsyncClient              │
│  Wikipedia Summary API   ──► (rate limited, retries)        │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER                               │
│  data/bronze/daily_top/year=YYYY/month=MM/day=DD.parquet    │
│  data/bronze/article_meta/articles.parquet                  │
│                                                             │
│  Raw API responses, minimal processing                      │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼ Polars transformations
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER                               │
│  data/silver/pageviews/                                     │
│  data/silver/articles/                                      │
│                                                             │
│  Cleaned: filtered, decoded, validated, deduplicated        │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼ dbt + DuckDB
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER                                │
│  data/gold/*.parquet  ◄──► PostgreSQL tables                │
│                                                             │
│  Models:                                                    │
│  - dim_articles (dimension)                                 │
│  - fact_daily_pageviews (incremental fact)                  │
│  - agg_weekly_summary (aggregate)                           │
└─────────────────────────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
┌──────────────────┐          ┌──────────────────┐
│   HuggingFace    │          │    Metabase      │
│   Datasets       │          │   Dashboard      │
│                  │          │                  │
│  Versioned       │          │  "The Daily      │
│  public dataset  │          │   Pulse"         │
└──────────────────┘          └──────────────────┘
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
│   │   └── *.parquet
│   └── articles/
│       └── *.parquet
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
  - thumbnail_url: STRING
  - first_seen_date: DATE
```

### Silver Schema

```
silver_pageviews:
  - view_date: DATE
  - article_title: STRING
  - views: INT
  - rank: INT

silver_articles:
  - pageid: INT
  - article_title: STRING
  - description: STRING
  - extract: STRING
  - wikibase_item: STRING
  - article_type: STRING
  - thumbnail_url: STRING
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
  - thumbnail_url: STRING
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
- Natural grain of the source data
- Enables efficient incremental processing
- Easy backfill via Dagster partition selector
- ~1,000 rows per partition keeps files small

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
