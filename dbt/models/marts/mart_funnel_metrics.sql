-- models/marts/mart_funnel_metrics.sql
-- Gold layer: conversion rate per funnel step
{{ config(materialized='table') }}

with sessions as (
    select * from {{ ref('stg_sessions') }}
),

pages as (
    select * from {{ ref('stg_pages') }}
)

select
    s.page,
    p.funnel_step,
    p.step_name,
    count(s.session_id)                                             as session_count,
    round(avg(s.duration_sec), 1)                                   as avg_duration_sec,
    sum(cast(s.clicked_cta as integer))                             as cta_clicks,
    sum(cast(s.converted as integer))                               as conversions,
    round(
        sum(cast(s.converted as integer)) * 100.0
        / nullif(count(s.session_id), 0), 1
    )                                                               as conversion_rate_pct
from sessions s
left join pages p on s.page = p.page
group by 1, 2, 3
order by p.funnel_step
