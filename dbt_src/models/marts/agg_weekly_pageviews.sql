{{ config(
    materialized='table'
) }}


with pageviews as (
    select
        fdp.article_id,
        fdp.view_date,
        fdp.views,
        fdp.rank,
        date_trunc('week', view_date) as week_start,
        min(fdp.rank) over (partition by fdp.article_id, date_trunc('week', view_date)) as best_rank_in_week

    from {{ ref('fact_daily_pageviews') }} fdp
)

select
    p.article_id,
    p.week_start,
    sum(p.views) as total_views,
    avg(p.views) as avg_daily_views,
    min(p.rank) as best_rank,
    count(*) filter (where p.rank = p.best_rank_in_week) as days_at_best_rank,
    avg(p.rank) as avg_rank,
    count(*) as days_in_top_1k

from pageviews p
group by p.article_id, p.week_start
