from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestrator.assets.bronze import bronze_article_meta, bronze_daily_top
from orchestrator.assets.gold import dbt_project, gold_dbt_assets
from orchestrator.assets.silver import silver_articles, silver_pageviews
from orchestrator.assets.staging import pg_stg_articles, pg_stg_pageviews
from orchestrator.checks.bronze import (
    bronze_article_meta_no_duplicate_pageids,
    bronze_daily_top_expected_columns,
    bronze_daily_top_row_count,
)
from orchestrator.checks.gold import (
    gold_aggregate_reconciliation,
    gold_fact_has_data,
    gold_referential_integrity,
)
from orchestrator.checks.silver import (
    silver_articles_no_null_keys,
    silver_pageviews_no_filtered_pages,
    silver_pageviews_no_null_titles,
    silver_pageviews_views_in_range,
)
from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient

defs = Definitions(
    assets=[
        bronze_daily_top,
        bronze_article_meta,
        silver_pageviews,
        silver_articles,
        pg_stg_pageviews,
        pg_stg_articles,
        gold_dbt_assets,
    ],
    asset_checks=[
        bronze_daily_top_row_count,
        bronze_daily_top_expected_columns,
        bronze_article_meta_no_duplicate_pageids,
        silver_pageviews_no_null_titles,
        silver_pageviews_views_in_range,
        silver_pageviews_no_filtered_pages,
        silver_articles_no_null_keys,
        gold_fact_has_data,
        gold_referential_integrity,
        gold_aggregate_reconciliation,
    ],
    resources={
        "api_client": WikiPageViewsAPIClient(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
