-- stg_matches.sql
-- Silver layer: cleans and types raw match rows from bronze_wtt_matches.
-- Parses game score string into structured fields.
-- Standardizes event_type to WTT | ITTF | OTHER.

with source as (
    select * from {{ source('wtt_raw', 'bronze_wtt_matches') }}
),

cleaned as (
    select
        -- TODO: implement cleaning transformations
        *
    from source
)

select * from cleaned
