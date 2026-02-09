{{ config(
    materialized='incremental',
    unique_key=['article_id', 'view_date'],
    incremental_strategy='merge'
) }}


select
    sa.article_id,
    sp.ingestion_date as view_date,
    sp.views,
    sp.rank

from {{ ref('stg_pageviews') }} sp
inner join {{ ref('dim_articles') }} sa
    on sa.article_title = sp.article

{% if is_incremental() %}
where sp.ingestion_date > (select max(view_date) from {{ this }})
{% endif %}
