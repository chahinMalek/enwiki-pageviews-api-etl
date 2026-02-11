import polars as pl
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from orchestrator.config import DATA_DIR, get_pg_connection_params


def _get_pg_connection_string() -> str:
    params = get_pg_connection_params()
    return (
        f"postgresql+psycopg2://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )


@asset(
    deps=["silver_pageviews"],
    group_name="staging",
    description="Loads all silver pageview Parquet partitions into PostgreSQL staging.pageviews.",
    kinds={"python", "postgres"},
)
def pg_stg_pageviews(context: AssetExecutionContext) -> Output[None]:
    parquet_glob = str(DATA_DIR / "silver/pageviews/**/*.parquet")
    df = pl.read_parquet(parquet_glob)
    row_count = len(df)
    context.log.info(f"Read {row_count} rows from silver pageviews Parquet files")

    conn = _get_pg_connection_string()
    df.write_database("staging.pageviews", conn, if_table_exists="replace")
    context.log.info(f"Wrote {row_count} rows to staging.pageviews")

    return Output(
        None,
        metadata={
            "row_count": MetadataValue.int(row_count),
        },
    )


@asset(
    deps=["silver_articles"],
    group_name="staging",
    description="Loads silver article metadata Parquet into PostgreSQL staging.articles.",
    kinds={"python", "postgres"},
)
def pg_stg_articles(context: AssetExecutionContext) -> Output[None]:
    parquet_path = DATA_DIR / "silver/articles/articles.parquet"

    if not parquet_path.exists():
        context.log.info("No silver articles Parquet file found — nothing to load")
        return Output(None, metadata={"row_count": MetadataValue.int(0)})

    df = pl.read_parquet(parquet_path)
    row_count = len(df)
    context.log.info(f"Read {row_count} rows from {parquet_path}")

    conn = _get_pg_connection_string()
    df.write_database("staging.articles", conn, if_table_exists="replace")
    context.log.info(f"Wrote {row_count} rows to staging.articles")

    return Output(
        None,
        metadata={
            "row_count": MetadataValue.int(row_count),
        },
    )
