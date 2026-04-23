-- stg_ranking_history.sql
-- Silver layer: cleans and types bronze_ittf_ranking_history.
-- One row per player per week_date.

with source as (
    select * from {{ source('wtt_raw', 'bronze_ittf_ranking_history') }}
),

cleaned as (
    select
        -- TODO: implement cleaning transformations
        *
    from source
)

select * from cleaned
