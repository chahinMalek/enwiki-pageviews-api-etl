import datetime
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from orchestrator.partitions import daily_partitions
from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient

DATA_DIR = Path("data")


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Daily top ~1000 most-viewed Wikipedia articles from the Wikimedia PageViews API.",
    kinds={"python", "parquet"},
)
def bronze_daily_top(
    context: AssetExecutionContext,
    api_client: WikiPageViewsAPIClient,
) -> Output[None]:
    """
    Partitioned asset using daily partitions.
    Extracts top viewed articles for each day and writes to separate parquet files.
    """

    partition_date_str = context.partition_key
    dt = datetime.date.fromisoformat(partition_date_str)

    context.log.info(f"Fetching top articles for {partition_date_str}")
    articles = api_client.fetch_top_articles(partition_date_str)
    context.log.info(f"Retrieved {len(articles)} articles")

    # convert to a polars dataframe with correct dtypes
    df = pl.DataFrame(articles).with_columns(
        pl.lit(dt).alias("ingestion_date"),
    )
    df = df.select(
        pl.col("ingestion_date").cast(pl.Date),
        pl.col("article").cast(pl.Utf8),
        pl.col("views").cast(pl.Int64),
        pl.col("rank").cast(pl.Int64),
    )

    # ensure parent dir exists
    output_dir = DATA_DIR.joinpath(f"bronze/daily_top/year={dt.year}/month={dt.month:02d}")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"day={dt.day:02d}.parquet"

    # export to correct partition paths
    df.write_parquet(output_path)
    context.log.info(f"Wrote {len(df)} rows to {output_path}")

    return Output(
        None,
        metadata={
            "row_count": MetadataValue.int(len(df)),
            "partition_date": MetadataValue.text(partition_date_str),
            "output_path": MetadataValue.path(str(output_path)),
        },
    )
