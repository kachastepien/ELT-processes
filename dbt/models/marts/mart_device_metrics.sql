-- models/marts/mart_device_metrics.sql
-- Gold layer: CTR and CVR breakdown by device type
{{ config(materialized='table') }}

with sessions as (
    select * from {{ ref('stg_sessions') }}
)

select
    device,
    count(session_id)                                               as session_count,
    round(avg(duration_sec), 1)                                     as avg_duration_sec,
    round(
        sum(cast(clicked_cta as integer)) * 100.0
        / nullif(count(session_id), 0), 1
    )                                                               as cta_ctr_pct,
    round(
        sum(cast(converted as integer)) * 100.0
        / nullif(count(session_id), 0), 1
    )                                                               as cvr_pct
from sessions
group by device
order by session_count desc
