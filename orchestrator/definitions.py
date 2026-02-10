from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestrator.assets.bronze import bronze_article_meta, bronze_daily_top
from orchestrator.assets.gold import dbt_project, gold_dbt_assets
from orchestrator.assets.silver import silver_articles, silver_pageviews
from orchestrator.assets.staging import pg_stg_articles, pg_stg_pageviews
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
    resources={
        "api_client": WikiPageViewsAPIClient(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
