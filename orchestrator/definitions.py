from dagster import Definitions

from orchestrator.assets.bronze import bronze_article_meta, bronze_daily_top
from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient

defs = Definitions(
    assets=[bronze_daily_top, bronze_article_meta],
    resources={
        "api_client": WikiPageViewsAPIClient(),
    },
)
