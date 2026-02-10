import datetime
from pathlib import Path

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from orchestrator.assets.bronze import bronze_article_meta, bronze_daily_top

DATA_DIR = Path("data")
EXPECTED_COLUMNS = {"ingestion_date", "article", "views", "rank"}


@asset_check(asset=bronze_daily_top, description="Partition has at least one row.", blocking=True)
def bronze_daily_top_row_count(context) -> AssetCheckResult:
    dt = datetime.date.fromisoformat(context.partition_key)
    path = (
        DATA_DIR / f"bronze/daily_top/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    df = pl.read_parquet(path)
    row_count = len(df)
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": row_count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset=bronze_daily_top, description="All expected columns are present.", blocking=True)
def bronze_daily_top_expected_columns(context) -> AssetCheckResult:
    dt = datetime.date.fromisoformat(context.partition_key)
    path = (
        DATA_DIR / f"bronze/daily_top/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}.parquet"
    )
    df = pl.read_parquet(path)
    actual = set(df.columns)
    missing = EXPECTED_COLUMNS - actual
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"missing_columns": list(missing), "actual_columns": list(actual)},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset=bronze_article_meta,
    description="No duplicate pageids in article metadata.",
    blocking=True,
)
def bronze_article_meta_no_duplicate_pageids(context) -> AssetCheckResult:
    path = DATA_DIR / "bronze/article_meta/articles.parquet"
    if not path.exists():
        return AssetCheckResult(passed=True, metadata={"reason": "file does not exist yet"})
    df = pl.read_parquet(path, columns=["pageid"])
    total = len(df)
    unique = df["pageid"].n_unique()
    return AssetCheckResult(
        passed=total == unique,
        metadata={"total_rows": total, "unique_pageids": unique, "duplicates": total - unique},
        severity=AssetCheckSeverity.ERROR,
    )
