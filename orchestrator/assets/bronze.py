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

@asset(
    deps=["bronze_daily_top"],
    group_name="bronze",
    description="Article metadata from Wikipedia Summary API. Incrementally adds new articles.",
    kinds={"python", "parquet"},
)
def bronze_article_meta(
    context: AssetExecutionContext,
    api_client: WikiPageViewsAPIClient,
) -> Output[None]:
    """
    Unpartitioned asset depending on bronze_daily_top.
    Scans all bronze_daily_top files, calculates diffs and articles.parquet and fetches metadata for new articles.
    """

    bronze_dir = DATA_DIR.joinpath("bronze/daily_top")
    parquet_files = sorted(bronze_dir.rglob("*.parquet"))

    if not parquet_files:
        context.log.info("No bronze_daily_top files found — nothing to do")
        return Output(
            None,
            metadata={
                "new_articles_fetched": MetadataValue.int(0),
                "total_articles": MetadataValue.int(0),
                "bronze_files_scanned": MetadataValue.int(0),
            },
        )

    # collect every unique article title across all bronze_daily_top partitions
    all_titles: set[str] = set()
    context.log.info(f"Scanning {str(bronze_dir)} for articles...")
    for pf in parquet_files:
        df = pl.read_parquet(pf, columns=["article"])
        all_titles.update(df["article"].to_list())

    context.log.info(f"Scanned {len(parquet_files)} files, found {len(all_titles)} unique titles")

    # load existing articles metadata to filter only on new ones
    meta_path = DATA_DIR.joinpath("bronze/article_meta/articles.parquet")
    if meta_path.exists():
        existing_df = pl.read_parquet(meta_path)
        existing_titles = set(existing_df["title"].to_list())
    else:
        existing_df = None
        existing_titles = set()

    new_titles = all_titles - existing_titles
    context.log.info(
        f"{len(new_titles):,} new titles to fetch "
        f"({len(existing_titles):,} already tracked, "
        f"total={len(new_titles) + len(existing_titles):,})"
    )

    if not new_titles:
        return Output(
            None,
            metadata={
                "new_articles_fetched": MetadataValue.int(0),
                "total_articles": MetadataValue.int(len(existing_titles)),
                "bronze_files_scanned": MetadataValue.int(len(parquet_files)),
            },
        )

    # fetch metadata for new titles
    articles_metadata = api_client.fetch_articles_metadata(sorted(new_titles))
    context.log.info(f"Successfully fetched metadata for {len(articles_metadata)} articles")

    if not articles_metadata:
        return Output(
            None,
            metadata={
                "new_articles_fetched": MetadataValue.int(0),
                "total_articles": MetadataValue.int(len(existing_titles)),
                "bronze_files_scanned": MetadataValue.int(len(parquet_files)),
            },
        )

    # build dataframe with new articles metadata
    today = datetime.date.today()
    metadata_df = pl.DataFrame(articles_metadata).with_columns(
        pl.lit(today).alias("first_seen_date"),
    )

    metadata_df = metadata_df.select(
        pl.col("pageid").cast(pl.Int64),
        pl.col("title").cast(pl.Utf8),
        pl.col("description").cast(pl.Utf8),
        pl.col("extract").cast(pl.Utf8),
        pl.col("wikibase_item").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8),
        pl.col("first_seen_date").cast(pl.Date),
    )

    # merge with existing metadata
    if existing_df is not None:
        merged_df = pl.concat([existing_df, metadata_df], how="vertical_relaxed")
    else:
        merged_df = metadata_df

    # export
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.write_parquet(meta_path)

    context.log.info(f"Wrote {len(merged_df)} total articles to {meta_path} ({len(metadata_df)} new)")

    return Output(
        None,
        metadata={
            "new_articles_fetched": MetadataValue.int(len(metadata_df)),
            "total_articles": MetadataValue.int(len(merged_df)),
            "bronze_files_scanned": MetadataValue.int(len(parquet_files)),
            "output_path": MetadataValue.path(str(meta_path)),
        },
    )
