{{ config(
    materialized='table'
) }}


with latest_date as (
    select max(view_date) as max_date
    from {{ ref('fact_daily_pageviews') }}
),

recent_window as (
    select
        fdp.article_id,
        fdp.view_date,
        fdp.rank,
        fdp.views
    from {{ ref('fact_daily_pageviews') }} fdp
    cross join latest_date ld
    where fdp.view_date >= ld.max_date - interval '6 days'
),

latest_ranks as (
    select
        rw.article_id,
        rw.rank as latest_rank,
        rw.views as latest_views
    from recent_window rw
    cross join latest_date ld
    where rw.view_date = ld.max_date
),

avg_ranks as (
    select
        rw.article_id,
        avg(rw.rank) as avg_rank_7d
    from recent_window rw
    group by rw.article_id
)

select
    da.article_title,
    lr.latest_rank,
    lr.latest_views,
    ar.avg_rank_7d,
    round(ar.avg_rank_7d - lr.latest_rank, 1) as rank_improvement
from latest_ranks lr
inner join avg_ranks ar on ar.article_id = lr.article_id
inner join {{ ref('dim_articles') }} da on da.article_id = lr.article_id
