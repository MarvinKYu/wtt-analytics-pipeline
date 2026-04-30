-- stg_ranking_history.sql
-- Silver layer: cleans and types bronze_ittf_ranking_history.
-- Deduplicates by (player_id, week_date) keeping the latest scrape.
-- One row per player per week_date.

with source as (
    select * from {{ source('wtt_raw', 'bronze_ittf_ranking_history') }}
),

deduped as (
    select *,
        row_number() over (
            partition by player_id, week_date
            order by scraped_at desc
        ) as rn
    from source
),

cleaned as (
    select
        cast(player_id as int64)                   as player_id,
        safe.parse_date('%Y-%m-%d', week_date)     as week_date,
        cast(week_number as int64)                 as week_number,
        cast(rank as int64)                        as rank,
        cast(rank_change as int64)                 as rank_change,
        cast(points as int64)                      as points,
        cast(scraped_at as timestamp)              as scraped_at
    from deduped
    where rn = 1
      and week_date is not null
      and week_date != ''
)

select * from cleaned
