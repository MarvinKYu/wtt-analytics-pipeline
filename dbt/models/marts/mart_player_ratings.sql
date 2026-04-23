-- mart_player_ratings.sql
-- Gold layer: final player ratings as computed by the Python rating engine.
-- Reads from mart_player_ratings_raw (written by replay.py).
-- Joins with latest ITTF rank for the over/underranked analysis.

with ratings as (
    select * from {{ source('wtt', 'mart_player_ratings_raw') }}
),

latest_rankings as (
    select *
    from {{ ref('stg_rankings') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_rankings') }})
),

-- TODO: join and compute implied_rank from rating ordering

final as (
    select
        -- player_id, player_name, nationality, gender,
        -- rating, rd, sigma, matches_played,
        -- ittf_rank, ittf_points,
        -- implied_rank (row_number() over (order by rating desc)),
        -- divergence (ittf_rank - implied_rank)
        1 as placeholder  -- remove when implementing
    from ratings
)

select * from final
