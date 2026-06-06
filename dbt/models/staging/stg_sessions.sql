-- models/staging/stg_sessions.sql
-- Silver layer: validated, typed session records
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'sessions') }}
),

cleaned as (
    select
        session_id,
        user_id,
        page,
        lower(trim(device))                                          as device,
        case
            when duration_sec < 0 or duration_sec > 7200 then null
            else duration_sec
        end                                                          as duration_sec,
        clicked_cta,
        converted,
        try_cast(ts as timestamp)                                    as event_ts,
        coalesce(upper(country), 'UNKNOWN')                         as country,
        (duration_sec < 5)                                           as is_suspicious,
        current_timestamp                                            as _loaded_at
    from source
    where session_id is not null
      and user_id    is not null
)

select * from cleaned
