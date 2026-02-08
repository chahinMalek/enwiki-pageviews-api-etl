import datetime
from pathlib import Path
from urllib.parse import unquote

import polars as pl
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from orchestrator.partitions import daily_partitions

# constants
DATA_DIR = Path("data")
FILTERED_PREFIXES = ("special:", "wikipedia:", "file:", "portal:", "category:", "help:")
FILTERED_EXACT = frozenset({"Main_Page"})

# utility functions


def _decode_title(title: str) -> str:
    return unquote(title).replace("_", " ")


def filter_non_articles(df: pl.DataFrame, col: str = "article") -> pl.DataFrame:
    mask = ~pl.col(col).is_in(list(FILTERED_EXACT))
    for prefix in FILTERED_PREFIXES:
        mask = mask & ~pl.col(col).str.to_lowercase().str.starts_with(prefix)
    return df.filter(mask)


def decode_article_titles(df: pl.DataFrame, col: str = "article") -> pl.DataFrame:
    return df.with_columns(pl.col(col).map_elements(_decode_title, return_dtype=pl.Utf8))


# transforms


def transform_daily_pageviews(df: pl.DataFrame) -> pl.DataFrame:
    """
    Full silver transformation pipeline for daily pageviews.

    Steps:
        1. Filter non-article pages (before URL decoding, patterns match raw form)
        2. URL-decode article titles
        3. Validate: views > 0, no null titles
        4. Deduplicate by title per day (keep the highest views)
        5. Sort by rank
    """
    df = filter_non_articles(df)
    df = decode_article_titles(df)
    df = df.filter(pl.col("views") > 0)
    df = df.filter(pl.col("article").is_not_null())
    df = df.sort("views", descending=True).unique(
        subset=["article", "ingestion_date"], keep="first"
    )
    df = df.sort("rank")
    return df


def transform_article_metadata(df: pl.DataFrame) -> pl.DataFrame:
    """
    Full silver transformation pipeline for article metadata.

    Steps:
        1. URL-decode titles
        2. Strip whitespace from text fields
        3. Validate: no null pageid or title
        4. Deduplicate by pageid
    """
    df = df.with_columns(pl.col("title").map_elements(_decode_title, return_dtype=pl.Utf8))
    df = df.with_columns(
        pl.col("description").str.strip_chars(),
        pl.col("extract").str.strip_chars(),
    )
    df = df.filter(pl.col("pageid").is_not_null() & pl.col("title").is_not_null())
    df = df.unique(subset=["pageid"], keep="first")
    return df


@asset(
    partitions_def=daily_partitions,
    deps=["bronze_daily_top"],
    group_name="silver",
    description="Cleaned daily pageviews: non-article pages filtered, titles URL-decoded, validated and deduplicated.",
    kinds={"python", "parquet"},
)
def silver_pageviews(context: AssetExecutionContext) -> Output[None]:
    """
    Reads bronze daily_top files for partition date, applies silver transformations, and exports to parquet.
    """
    partition_date_str = context.partition_key
    dt = datetime.date.fromisoformat(partition_date_str)

    bronze_path = (
        DATA_DIR / f"bronze/daily_top/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    df = pl.read_parquet(bronze_path)
    bronze_row_count = len(df)
    context.log.info(f"Read {bronze_row_count} rows from {bronze_path}")

    df = transform_daily_pageviews(df)
    silver_row_count = len(df)
    filtered_count = bronze_row_count - silver_row_count
    context.log.info(f"After transformation: {silver_row_count} rows ({filtered_count} filtered)")

    output_dir = DATA_DIR / f"silver/pageviews/year={dt.year}/month={dt.month:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"day={dt.day:02d}.parquet"

    df.write_parquet(output_path)
    context.log.info(f"Wrote {silver_row_count} rows to {output_path}")

    return Output(
        None,
        metadata={
            "row_count": MetadataValue.int(silver_row_count),
            "filtered_count": MetadataValue.int(filtered_count),
            "partition_date": MetadataValue.text(partition_date_str),
            "output_path": MetadataValue.path(str(output_path)),
        },
    )


@asset(
    deps=["bronze_article_meta"],
    group_name="silver",
    description="Cleaned article metadata: titles URL-decoded, validated, and deduplicated.",
    kinds={"python", "parquet"},
)
def silver_articles(context: AssetExecutionContext) -> Output[None]:
    """
    Read bronze article_meta, apply silver transformations, write parquet.
    """
    bronze_path = DATA_DIR / "bronze/article_meta/articles.parquet"

    if not bronze_path.exists():
        context.log.info("No bronze article_meta file found — nothing to do")
        return Output(
            None,
            metadata={"row_count": MetadataValue.int(0)},
        )

    df = pl.read_parquet(bronze_path)
    bronze_row_count = len(df)
    context.log.info(f"Read {bronze_row_count} rows from {bronze_path}")

    df = transform_article_metadata(df)
    silver_row_count = len(df)
    filtered_count = bronze_row_count - silver_row_count
    context.log.info(f"After transformation: {silver_row_count} rows ({filtered_count} filtered)")

    output_dir = DATA_DIR / "silver/articles"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "articles.parquet"

    df.write_parquet(output_path)
    context.log.info(f"Wrote {silver_row_count} rows to {output_path}")

    return Output(
        None,
        metadata={
            "row_count": MetadataValue.int(silver_row_count),
            "filtered_count": MetadataValue.int(filtered_count),
            "output_path": MetadataValue.path(str(output_path)),
        },
    )
