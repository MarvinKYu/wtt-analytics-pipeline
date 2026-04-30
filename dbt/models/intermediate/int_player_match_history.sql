-- int_player_match_history.sql
-- One row per player per match.
-- Resolves each scraped row (queried player + match) into player_id, opponent_id, result.
-- Name-to-player_id lookup is derived from the latest stg_rankings snapshot.
-- Used as input to rating_engine/replay.py.

with
matches as (
    select * from {{ ref('stg_matches') }}
),

rankings as (
    select * from {{ ref('stg_rankings') }}
),

-- Latest name for each player_id
player_names as (
    select player_id, name
    from (
        select
            player_id,
            name,
            row_number() over (partition by player_id order by snapshot_date desc) as rn
        from rankings
    )
    where rn = 1
),

-- Lowercase name → player_id lookup (min player_id on rare name collisions)
name_to_id as (
    select
        lower(name) as name_lower,
        min(player_id) as player_id
    from player_names
    group by lower(name)
),

-- Determine each queried player's position (A or X) and compute result
positioned as (
    select
        m.match_id,
        m.player_id,
        m.match_year,
        m.event_name,
        m.event_type,
        case
            when lower(pn.name) = lower(m.player_a_name)
                then case when m.result_a_games > m.result_x_games then 'WIN' else 'LOSS' end
            when lower(pn.name) = lower(m.player_x_name)
                then case when m.result_x_games > m.result_a_games then 'WIN' else 'LOSS' end
            else null
        end as result,
        case
            when lower(pn.name) = lower(m.player_a_name) then lower(m.player_x_name)
            when lower(pn.name) = lower(m.player_x_name) then lower(m.player_a_name)
            else null
        end as opponent_name_lower
    from matches m
    left join player_names pn on pn.player_id = m.player_id
),

final as (
    select
        p.match_id,
        p.player_id,
        ni.player_id  as opponent_id,
        p.match_year,
        p.result,
        p.event_name,
        p.event_type
    from positioned p
    left join name_to_id ni on ni.name_lower = p.opponent_name_lower
    -- exclude rows where position could not be determined (name mismatch between
    -- rankings page and match history page formatting)
    where p.result is not null
)

select * from final
