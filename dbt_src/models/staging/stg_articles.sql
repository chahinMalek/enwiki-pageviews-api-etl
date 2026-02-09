-- depends_on: {{ source('silver', 'articles') }}

select
    pageid,
    title,
    description,
    extract,
    wikibase_item,
    type,
    first_seen_date
from read_parquet(
    '{{ var("data_dir") }}/silver/articles/articles.parquet'
)
