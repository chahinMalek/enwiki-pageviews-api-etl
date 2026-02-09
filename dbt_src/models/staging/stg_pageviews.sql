-- depends_on: {{ source('silver', 'pageviews') }}

select
    ingestion_date,
    article,
    views,
    rank
from read_parquet(
    '{{ var("data_dir") }}/silver/pageviews/**/*.parquet',
    hive_partitioning = false
)
