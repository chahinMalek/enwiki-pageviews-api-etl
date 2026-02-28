from dataclasses import asdict, dataclass

from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestrator.config import get_pg_config
from orchestrator.resources.metabase import MetabaseResource


@dataclass(frozen=True)
class CardDefinition:
    name: str
    display: str
    query: str
    col: int
    row: int
    size_x: int
    size_y: int
    template_tags: dict | None = None


CARDS = (
    CardDefinition(
        name="Top 10 Today",
        display="table",
        query="""
            SELECT
                da.article_title AS "Article",
                f.views AS "Views",
                f.rank AS "Rank"
            FROM gold.fact_daily_pageviews f
            JOIN gold.dim_articles da ON da.article_id = f.article_id
            WHERE f.view_date = (SELECT MAX(view_date) FROM gold.fact_daily_pageviews)
            ORDER BY f.rank
            LIMIT 10
        """,
        col=0,
        row=0,
        size_x=12,
        size_y=6,
    ),
    CardDefinition(
        name="Trending",
        display="table",
        query="""
            SELECT
                article_title AS "Article",
                latest_rank AS "Rank",
                latest_views AS "Views",
                round(avg_rank_7d::numeric, 1)  AS "Avg Rank (7d)",
                round(rank_improvement::numeric, 1) AS "Rank Improvement"
            FROM gold.trending_articles
            WHERE rank_improvement > 0
            ORDER BY rank_improvement DESC
            LIMIT 20
        """,
        col=12,
        row=0,
        size_x=12,
        size_y=6,
    ),
    CardDefinition(
        name="Weekly Movers",
        display="table",
        query="""
            SELECT
                article_title AS "Article",
                this_week_views AS "This Week",
                prev_week_views AS "Prev Week",
                view_change AS "Change",
                round(view_change_pct::numeric, 1) AS "Change %"
            FROM gold.weekly_movers
            ORDER BY abs(view_change) DESC
            LIMIT 20
        """,
        col=0,
        row=6,
        size_x=12,
        size_y=6,
    ),
    CardDefinition(
        name="Historical Lookup",
        display="line",
        query="""
            SELECT
                f.view_date AS "Date",
                f.views AS "Views"
            FROM gold.fact_daily_pageviews f
            JOIN gold.dim_articles da ON da.article_id = f.article_id
            WHERE da.article_title = {{ title }}
            ORDER BY f.view_date
        """,
        template_tags={
            "title": {
                "id": "title",
                "name": "title",
                "display-name": "Article Title",
                "type": "text",
                "required": True,
                "default": "Wikipedia",
            },
        },
        col=12,
        row=6,
        size_x=12,
        size_y=6,
    ),
    CardDefinition(
        name="Daily Volume",
        display="line",
        query="""
            SELECT
                view_date AS "Date",
                SUM(views) AS "Total Views"
            FROM gold.fact_daily_pageviews
            GROUP BY view_date
            ORDER BY view_date
        """,
        col=0,
        row=12,
        size_x=24,
        size_y=6,
    ),
)


@asset(
    deps=["gold_dbt_assets"],
    group_name="dashboard",
)
def metabase_dashboard(
    context: AssetExecutionContext,
    metabase: MetabaseResource,
) -> MaterializeResult:
    """
    Create a 'WikiPulse' dashboard in Metabase.
    Creates admin user, database connection, saved questions, and dashboard layout.
    Fully idempotent - safe to re-run.
    """

    dashboard_name = "WikiPulse"
    dashboard_desc = "Daily Wikipedia article page views dashboard"

    pg_params = {
        **asdict(get_pg_config()),
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "gold",
    }

    # setup metabase and authenticate
    metabase.wait_until_ready()
    metabase.setup()
    metabase.authenticate()

    # ensure db connection established to the gold schema
    db_id = metabase.ensure_database(name=pg_params["dbname"], engine="postgres", details=pg_params)

    # create cards
    card_ids = []
    for card_def in CARDS:
        card = metabase.ensure_card(
            name=card_def.name,
            database_id=db_id,
            query=card_def.query,
            display=card_def.display,
            template_tags=card_def.template_tags,
        )
        card_ids.append(card["id"])

    # build dashboard
    dashcards = [
        {
            "id": -(i + 1),  # negative temporary IDs for new cards
            "card_id": card_id,
            "col": card_def.col,
            "row": card_def.row,
            "size_x": card_def.size_x,
            "size_y": card_def.size_y,
        }
        for i, (card_id, card_def) in enumerate(zip(card_ids, CARDS))
    ]
    dashboard = metabase.ensure_dashboard(
        name=dashboard_name,
        description=dashboard_desc,
        dashcards=dashcards,
    )

    context.log.info(
        f"Dashboard '{dashboard_name}' ready (id={dashboard['id']}) with {len(card_ids)} cards"
    )
    return MaterializeResult(
        metadata={
            "dashboard_id": dashboard["id"],
            "card_count": len(card_ids),
            "dashboard_url": f"{metabase.metabase_url}/dashboard/{dashboard['id']}",
        },
    )
