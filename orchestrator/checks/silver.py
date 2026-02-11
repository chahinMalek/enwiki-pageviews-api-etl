import datetime

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from orchestrator.assets.silver import (
    FILTERED_EXACT,
    FILTERED_PREFIXES,
    silver_articles,
    silver_pageviews,
)
from orchestrator.config import DATA_DIR


@asset_check(asset=silver_pageviews, description="No null article titles.", blocking=True)
def silver_pageviews_no_null_titles(context) -> AssetCheckResult:
    dt = datetime.date.fromisoformat(context.partition_key)
    path = (
        DATA_DIR / f"silver/pageviews/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    df = pl.read_parquet(path, columns=["article"])
    null_count = df["article"].null_count()
    return AssetCheckResult(
        passed=null_count == 0,
        metadata={"null_count": null_count, "total_rows": len(df)},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset=silver_pageviews, description="All views between 1 and 1 billion.")
def silver_pageviews_views_in_range(context) -> AssetCheckResult:
    dt = datetime.date.fromisoformat(context.partition_key)
    path = (
        DATA_DIR / f"silver/pageviews/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    df = pl.read_parquet(path, columns=["views"])
    out_of_range = df.filter((pl.col("views") < 1) | (pl.col("views") > 1_000_000_000))
    return AssetCheckResult(
        passed=len(out_of_range) == 0,
        metadata={"out_of_range_count": len(out_of_range), "total_rows": len(df)},
        severity=AssetCheckSeverity.WARN,
    )


@asset_check(
    asset=silver_pageviews,
    description="No filtered pages (Main_Page, Special:*, Wikipedia:*, etc.) remain.",
    blocking=True,
)
def silver_pageviews_no_filtered_pages(context) -> AssetCheckResult:
    dt = datetime.date.fromisoformat(context.partition_key)
    path = (
        DATA_DIR / f"silver/pageviews/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    df = pl.read_parquet(path, columns=["article"])

    exact_matches = df.filter(pl.col("article").is_in(list(FILTERED_EXACT)))

    prefix_mask = pl.lit(False)
    for prefix in FILTERED_PREFIXES:
        prefix_mask = prefix_mask | pl.col("article").str.to_lowercase().str.starts_with(prefix)
    prefix_matches = df.filter(prefix_mask)

    violations = len(exact_matches) + len(prefix_matches)
    return AssetCheckResult(
        passed=violations == 0,
        metadata={
            "exact_match_violations": len(exact_matches),
            "prefix_violations": len(prefix_matches),
        },
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset=silver_articles,
    description="No null pageid or title in article metadata.",
    blocking=True,
)
def silver_articles_no_null_keys(context) -> AssetCheckResult:
    path = DATA_DIR / "silver/articles/articles.parquet"
    if not path.exists():
        return AssetCheckResult(passed=True, metadata={"reason": "file does not exist yet"})
    df = pl.read_parquet(path, columns=["pageid", "title"])
    null_pageid = df["pageid"].null_count()
    null_title = df["title"].null_count()
    return AssetCheckResult(
        passed=(null_pageid == 0 and null_title == 0),
        metadata={"null_pageid": null_pageid, "null_title": null_title, "total_rows": len(df)},
        severity=AssetCheckSeverity.ERROR,
    )
