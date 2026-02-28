import psycopg2
from dataclasses import asdict
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from orchestrator.config import get_pg_config


@asset_check(
    asset=AssetKey("fact_daily_pageviews"),
    description="Fact table has at least one row.",
    blocking=True,
)
def gold_fact_has_data(context) -> AssetCheckResult:
    conn = psycopg2.connect(**asdict(get_pg_config()))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM gold.fact_daily_pageviews")
        row_count = cursor.fetchone()[0]
    finally:
        conn.close()
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": row_count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset=AssetKey("fact_daily_pageviews"),
    description="All fact article_ids exist in dim_articles.",
    blocking=True,
)
def gold_referential_integrity(context) -> AssetCheckResult:
    conn = psycopg2.connect(**asdict(get_pg_config()))
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT count(*)
            FROM gold.fact_daily_pageviews f
            LEFT JOIN gold.dim_articles d ON f.article_id = d.article_id
            WHERE d.article_id IS NULL
        """)
        orphan_count = cursor.fetchone()[0]
    finally:
        conn.close()
    return AssetCheckResult(
        passed=orphan_count == 0,
        metadata={"orphaned_article_ids": orphan_count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset=AssetKey("agg_weekly_pageviews"),
    description="Weekly aggregated total_views matches sum of daily fact views.",
)
def gold_aggregate_reconciliation(context) -> AssetCheckResult:
    conn = psycopg2.connect(**asdict(get_pg_config()))
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT count(*)
            FROM gold.agg_weekly_pageviews a
            JOIN (
                SELECT article_id, date_trunc('week', view_date) AS week_start, sum(views) AS fact_total
                FROM gold.fact_daily_pageviews
                GROUP BY article_id, date_trunc('week', view_date)
            ) f ON a.article_id = f.article_id AND a.week_start = f.week_start
            WHERE a.total_views != f.fact_total
        """)
        mismatch_count = cursor.fetchone()[0]
    finally:
        conn.close()
    return AssetCheckResult(
        passed=mismatch_count == 0,
        metadata={"mismatched_rows": mismatch_count},
        severity=AssetCheckSeverity.WARN,
    )
