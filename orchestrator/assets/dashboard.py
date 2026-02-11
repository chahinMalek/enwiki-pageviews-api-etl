from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestrator.config import get_pg_connection_params
from orchestrator.resources.metabase import MetabaseResource

# --- Card definitions (name, SQL query, display type) ---

CARDS: list[dict] = [
    {
        "name": "Top 10 Today",
        "display": "table",
        "query": """
            SELECT
                da.article_title AS "Article",
                f.views           AS "Views",
                f.rank            AS "Rank"
            FROM gold.fact_daily_pageviews f
            JOIN gold.dim_articles da ON da.article_id = f.article_id
            WHERE f.view_date = (SELECT MAX(view_date) FROM gold.fact_daily_pageviews)
            ORDER BY f.rank
            LIMIT 10
        """,
    },
    {
        "name": "Trending",
        "display": "table",
        "query": """
            SELECT
                article_title       AS "Article",
                latest_rank         AS "Rank",
                latest_views        AS "Views",
                round(avg_rank_7d::numeric, 1)  AS "Avg Rank (7d)",
                round(rank_improvement::numeric, 1) AS "Rank Improvement"
            FROM gold.trending_articles
            WHERE rank_improvement > 0
            ORDER BY rank_improvement DESC
            LIMIT 20
        """,
    },
    {
        "name": "Weekly Movers",
        "display": "table",
        "query": """
            SELECT
                article_title    AS "Article",
                this_week_views  AS "This Week",
                prev_week_views  AS "Prev Week",
                view_change      AS "Change",
                round(view_change_pct::numeric, 1) AS "Change %"
            FROM gold.weekly_movers
            ORDER BY abs(view_change) DESC
            LIMIT 20
        """,
    },
    {
        "name": "Historical Lookup",
        "display": "line",
        "query": """
            SELECT
                f.view_date AS "Date",
                f.views     AS "Views"
            FROM gold.fact_daily_pageviews f
            JOIN gold.dim_articles da ON da.article_id = f.article_id
            WHERE da.article_title = {{ title }}
            ORDER BY f.view_date
        """,
        "template_tags": {
            "title": {
                "id": "title",
                "name": "title",
                "display-name": "Article Title",
                "type": "text",
                "required": True,
                "default": "Wikipedia",
            },
        },
    },
    {
        "name": "Daily Volume",
        "display": "line",
        "query": """
            SELECT
                view_date    AS "Date",
                SUM(views)   AS "Total Views"
            FROM gold.fact_daily_pageviews
            GROUP BY view_date
            ORDER BY view_date
        """,
    },
]

# --- Dashboard grid layout (Metabase uses 24-column grid) ---
# Each row is ~4 units tall. Layout: 2 wide cards on top, 2 on middle, 1 full-width on bottom.

GRID_LAYOUT: list[dict] = [
    # Row 1: Top 10 Today (left) + Trending (right)
    {"col": 0, "row": 0, "size_x": 12, "size_y": 6},
    {"col": 12, "row": 0, "size_x": 12, "size_y": 6},
    # Row 2: Weekly Movers (left) + Historical Lookup (right)
    {"col": 0, "row": 6, "size_x": 12, "size_y": 6},
    {"col": 12, "row": 6, "size_x": 12, "size_y": 6},
    # Row 3: Daily Volume (full width)
    {"col": 0, "row": 12, "size_x": 24, "size_y": 6},
]


@asset(
    deps=["gold_dbt_assets"],
    group_name="dashboard",
)
def metabase_dashboard(
    context: AssetExecutionContext,
    metabase: MetabaseResource,
) -> MaterializeResult:
    """
    Provision 'The Daily Pulse' dashboard in Metabase.
    Creates admin user, database connection, saved questions, and dashboard layout.
    Fully idempotent — safe to re-run.
    """
    pg_params = {
        **get_pg_connection_params(),
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "gold",
    }

    # 1. Wait for Metabase, run first-time setup, authenticate
    metabase.wait_until_ready()
    metabase.setup()
    metabase.authenticate()

    # 2. Ensure database connection to the gold schema
    db_id = metabase.ensure_database(
        name="wikipulse",
        engine="postgres",
        details=pg_params,
    )

    # 3. Create / update the 5 saved questions
    card_ids: list[int] = []
    for card_def in CARDS:
        card = metabase.ensure_card(
            name=card_def["name"],
            database_id=db_id,
            query=card_def["query"],
            display=card_def["display"],
            template_tags=card_def.get("template_tags"),
        )
        card_ids.append(card["id"])

    # 4. Build dashcard layout from card IDs + grid positions
    dashcards = [
        {
            "id": -(i + 1),  # negative temporary IDs for new cards
            "card_id": card_id,
            **layout,
        }
        for i, (card_id, layout) in enumerate(zip(card_ids, GRID_LAYOUT))
    ]

    # 5. Create / update the dashboard
    dashboard = metabase.ensure_dashboard(
        name="The Daily Pulse",
        description="Daily Wikipedia article popularity dashboard",
        dashcards=dashcards,
    )

    context.log.info(
        f"Dashboard 'The Daily Pulse' ready (id={dashboard['id']}) "
        f"with {len(card_ids)} cards"
    )
    return MaterializeResult(
        metadata={
            "dashboard_id": dashboard["id"],
            "card_count": len(card_ids),
            "dashboard_url": f"{metabase.metabase_url}/dashboard/{dashboard['id']}",
        },
    )
