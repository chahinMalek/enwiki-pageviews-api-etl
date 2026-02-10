select
    ingestion_date,
    article,
    views,
    rank
from {{ source('staging', 'pageviews') }}
