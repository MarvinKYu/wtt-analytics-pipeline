-- stg_matches.sql
-- Silver layer: cleans and types raw match rows from bronze_wtt_matches.
-- Deduplicates by (player_id, match_id) keeping the latest scrape.
-- Standardizes event_type to WTT | ITTF | OTHER.

with source as (
    select * from {{ source('wtt_raw', 'bronze_wtt_matches') }}
),

deduped as (
    select *,
        row_number() over (
            partition by player_id, match_id
            order by scraped_at desc
        ) as rn
    from source
),

cleaned as (
    select
        cast(match_id as int64)       as match_id,
        cast(player_id as int64)      as player_id,
        trim(event_name)              as event_name,
        case
            when upper(trim(event_name)) like 'WTT%'
                then 'WTT'
            when upper(trim(event_name)) like 'ITTF%'
              or upper(trim(event_name)) like '%WORLD CHAMPIONSHIP%'
              or upper(trim(event_name)) like '%WORLD CUP%'
              or upper(trim(event_name)) like '%WORLD TEAM%'
                then 'ITTF'
            else 'OTHER'
        end                           as event_type,
        trim(sub_event)               as sub_event,
        trim(stage)                   as stage,
        trim(round)                   as round,
        -- Strip trailing country code " (ENG)" / " (CHN)" etc. so names match stg_rankings format
        regexp_replace(trim(name_a), r' \([A-Z]{2,4}\)$', '') as player_a_name,
        regexp_replace(trim(name_x), r' \([A-Z]{2,4}\)$', '') as player_x_name,
        cast(result_a_games as int64) as result_a_games,
        cast(result_x_games as int64) as result_x_games,
        trim(game_scores)             as game_scores,
        trim(winner_name)             as winner_name,
        cast(match_year as int64)     as match_year,
        cast(scraped_at as timestamp) as scraped_at
    from deduped
    where rn = 1
      and match_id is not null
      and match_year is not null
)

select * from cleaned
