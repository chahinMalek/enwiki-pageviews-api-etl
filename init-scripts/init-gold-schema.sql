-- Create schemas for the gold layer pipeline
-- staging: holds Parquet data loaded by Dagster (source for dbt)
-- gold: holds dbt mart tables (queried by Metabase)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;
