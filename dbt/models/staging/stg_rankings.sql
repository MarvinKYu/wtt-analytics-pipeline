-- stg_rankings.sql
-- Silver layer: cleans and types bronze_ittf_rankings.
-- One row per player per snapshot_date.

with source as (
    select * from {{ source('wtt_raw', 'bronze_ittf_rankings') }}
),

cleaned as (
    select
        -- TODO: implement cleaning transformations
        *
    from source
)

select * from cleaned
