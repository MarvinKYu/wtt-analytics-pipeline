-- stg_rankings.sql
-- Silver layer: cleans and types bronze_ittf_rankings.
-- Deduplicates by (player_id, snapshot_date) keeping the latest scrape.
-- One row per player per snapshot_date.

with source as (
    select * from {{ source('wtt_raw', 'bronze_ittf_rankings') }}
),

deduped as (
    select *,
        row_number() over (
            partition by player_id, snapshot_date
            order by scraped_at desc
        ) as rn
    from source
),

cleaned as (
    select
        cast(player_id as int64)                       as player_id,
        cast(rank as int64)                            as rank,
        cast(points as int64)                          as points,
        trim(name)                                     as name,
        trim(association)                              as association,
        trim(continent)                                as continent,
        trim(gender)                                   as gender,
        safe.parse_date('%Y-%m-%d', snapshot_date)     as snapshot_date,
        cast(scraped_at as timestamp)                  as scraped_at
    from deduped
    where rn = 1
      and player_id is not null
      and snapshot_date is not null
)

select * from cleaned
