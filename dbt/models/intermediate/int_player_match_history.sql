-- int_player_match_history.sql
-- One row per player per match.
-- Joins stg_matches to stg_rankings to attach pre-match rank context.
-- Used as the input to rating_engine/replay.py.

with matches as (
    select * from {{ ref('stg_matches') }}
),

-- TODO: pivot to one row per player per match, join ranking snapshot

final as (
    select
        -- player_id, opponent_id, match_date, event_name, event_type,
        -- player_games_won, opponent_games_won, result,
        -- player_rank_at_match_time, opponent_rank_at_match_time
        1 as placeholder  -- remove this line when implementing
    from matches
)

select * from final
