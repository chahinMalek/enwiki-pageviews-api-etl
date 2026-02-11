{{ config(
    materialized='table'
) }}


with latest_week as (
    select max(week_start) as max_week
    from {{ ref('agg_weekly_pageviews') }}
),

weekly_with_lag as (
    select
        aw.article_id,
        aw.week_start,
        aw.total_views as this_week_views,
        lag(aw.total_views) over (
            partition by aw.article_id order by aw.week_start
        ) as prev_week_views
    from {{ ref('agg_weekly_pageviews') }} aw
)

select
    da.article_title,
    wl.week_start,
    wl.this_week_views,
    wl.prev_week_views,
    wl.this_week_views - wl.prev_week_views as view_change,
    round(
        (wl.this_week_views - wl.prev_week_views)::numeric
        / nullif(wl.prev_week_views, 0) * 100,
        1
    ) as view_change_pct
from weekly_with_lag wl
cross join latest_week lw
inner join {{ ref('dim_articles') }} da on da.article_id = wl.article_id
where wl.week_start = lw.max_week
  and wl.prev_week_views is not null
