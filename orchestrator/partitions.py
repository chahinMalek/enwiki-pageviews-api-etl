from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC",
)
