{{ config(
    materialized='table'
) }}


select
    pageid as article_id,
    title as article_title,
    description,
    extract as summary,
    wikibase_item,
    type as article_type,
    first_seen_date
from {{ ref('stg_articles') }}
