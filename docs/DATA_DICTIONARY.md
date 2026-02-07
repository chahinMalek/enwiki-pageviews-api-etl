# Data Dictionary

This document describes all data fields across the bronze, silver, and gold layers.

## Data Sources

### Wikimedia Pageviews API

**Endpoint**: `GET https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{YYYY}/{MM}/{DD}`

Returns the top ~1,000 most-viewed English Wikipedia articles for a given day.

### Wikipedia Summary API

**Endpoint**: `GET https://en.wikipedia.org/api/rest_v1/page/summary/{title}`

Returns metadata for a specific article.

---

## Bronze Layer

Raw API responses with minimal processing.

### bronze_daily_top

Daily top-viewed articles from Wikimedia Pageviews API.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `ingestion_date` | DATE | Date the data represents | `2024-01-15` |
| `article` | STRING | URL-encoded article title | `Barack_Obama` |
| `views` | INT | Total pageviews for the day | `245123` |
| `rank` | INT | Rank position (1 = most viewed) | `1` |

**Storage**: `data/bronze/daily_top/year=YYYY/month=MM/day=DD.parquet`

### bronze_article_meta

Article metadata from Wikipedia Summary API. This is an **unpartitioned** asset — it scans all `bronze_daily_top` Parquet files, collects unique titles, and fetches metadata only for titles not already in the registry.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `pageid` | INT | Wikipedia page ID (stable identifier) | `534366` |
| `title` | STRING | Canonical article title | `Barack Obama` |
| `description` | STRING | Short one-line description | `44th president of the United States` |
| `extract` | STRING | Plain-text summary paragraph | `Barack Hussein Obama II is an American...` |
| `wikibase_item` | STRING | Wikidata Q-ID | `Q76` |
| `type` | STRING | Article type | `standard`, `disambiguation`, `no-extract` |
| `first_seen_date` | DATE | Date the metadata was fetched | `2024-01-15` |

**Storage**: `data/bronze/article_meta/articles.parquet`

---

## Silver Layer

Cleaned and validated data.

### silver_pageviews

Cleaned daily pageview data.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `view_date` | DATE | Date of pageviews | `2024-01-15` |
| `article_title` | STRING | Human-readable article title (decoded) | `Barack Obama` |
| `views` | INT | Total pageviews for the day | `245123` |
| `rank` | INT | Rank position (1 = most viewed) | `1` |

**Transformations from bronze**:
- Filtered: `Main_Page`, `Special:*`, `Wikipedia:*`, `File:*`, `Portal:*`
- URL-decoded: `Barack_Obama` → `Barack Obama`
- Validated: `views > 0`, no null titles
- Deduplicated by title per day

**Storage**: `data/silver/pageviews/*.parquet`

### silver_articles

Cleaned article metadata.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `pageid` | INT | Wikipedia page ID | `534366` |
| `article_title` | STRING | Canonical article title | `Barack Obama` |
| `description` | STRING | Short description | `44th president of the United States` |
| `extract` | STRING | Summary paragraph | `Barack Hussein Obama II is an American...` |
| `wikibase_item` | STRING | Wikidata Q-ID | `Q76` |
| `article_type` | STRING | Article type | `standard` |
| `first_seen_date` | DATE | First appearance date | `2024-01-15` |

**Storage**: `data/silver/articles/*.parquet`

---

## Gold Layer

Analytical models built with dbt.

### dim_articles

Article dimension table. One row per unique article.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `article_id` | STRING | Surrogate key (hash of title) | `a1b2c3d4...` |
| `article_title` | STRING | Canonical article title | `Barack Obama` |
| `description` | STRING | Short description | `44th president of the United States` |
| `extract` | STRING | Summary paragraph | `Barack Hussein Obama II is an American...` |
| `wikibase_item` | STRING | Wikidata Q-ID (for future enrichment) | `Q76` |
| `article_type` | STRING | Article type | `standard` |
| `first_seen_date` | DATE | First appearance in top list | `2024-01-15` |

**Primary Key**: `article_id`

### fact_daily_pageviews

Daily article pageview facts. Incremental model.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `article_id` | STRING | Foreign key to dim_articles | `a1b2c3d4...` |
| `view_date` | DATE | Date of pageviews | `2024-01-15` |
| `views` | INT | Total pageviews | `245123` |
| `rank` | INT | Daily rank (1 = most viewed) | `1` |

**Primary Key**: `(article_id, view_date)`
**Foreign Key**: `article_id` → `dim_articles.article_id`

### agg_weekly_summary

Weekly aggregated metrics per article.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `article_id` | STRING | Foreign key to dim_articles | `a1b2c3d4...` |
| `week_start` | DATE | Monday of the week | `2024-01-15` |
| `total_views` | INT | Sum of views for the week | `1523456` |
| `avg_daily_views` | FLOAT | Average daily views | `217636.57` |
| `best_rank` | INT | Best (lowest) rank achieved | `1` |
| `days_in_top_1000` | INT | Days article appeared in top list | `7` |

**Primary Key**: `(article_id, week_start)`
**Foreign Key**: `article_id` → `dim_articles.article_id`

---

## Data Quality Rules

### Bronze
- Row count > 0 per partition
- Expected columns present
- No duplicate dates

### Silver
- No null `article_title`
- `views > 0`
- No filtered pages (Main_Page, Special:*, etc.)
- Date within expected range (2026-01-01 to today)

### Gold
- Unique primary keys
- Referential integrity (all fact article_ids exist in dimension)
- `views > 0` on all fact rows
- Each date has 50-1,500 rows (plausible range)

---

## Notes

### URL Encoding
Wikipedia article titles in URLs use underscores for spaces and percent-encoding for special characters:
- `Barack_Obama` → `Barack Obama`
- `The_Penguin_%28TV_series%29` → `The Penguin (TV series)`

### Filtered Pages
The following are excluded from silver/gold as they are not content articles:
- `Main_Page` - Wikipedia homepage
- `Special:*` - System pages (search, login, etc.)
- `Wikipedia:*` - Project pages
- `File:*` - Media file pages
- `Portal:*` - Portal pages

### Wikidata Integration
The `wikibase_item` field contains Wikidata Q-IDs (e.g., `Q76` for Barack Obama). This enables future enrichment via the Wikidata API to fetch categories, properties, and relationships. Out of scope for v1.
