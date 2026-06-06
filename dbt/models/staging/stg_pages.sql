-- models/staging/stg_pages.sql
-- Silver layer: page metadata and funnel step definitions
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'pages') }}
)

select
    page_id,
    page,
    funnel_step,
    trim(step_name) as step_name
from source
where page_id is not null
