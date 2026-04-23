-- mart_rating_vs_ranking.sql
-- Gold layer: the headline output — over/underranked analysis.
-- Filters to players with RD < 200 (high-confidence ratings only).
-- Positive divergence = overranked by ITTF vs model.
-- Negative divergence = underranked by ITTF vs model.

with base as (
    select * from {{ ref('mart_player_ratings') }}
),

filtered as (
    select *
    from base
    where rd < 200  -- only surface high-confidence ratings
),

-- TODO: add divergence categorization and percentile bands

final as (
    select
        -- player_id, player_name, nationality,
        -- ittf_rank, implied_rank, divergence,
        -- divergence_category ('overranked' | 'underranked' | 'aligned'),
        -- rating, rd
        1 as placeholder  -- remove when implementing
    from filtered
)

select * from final
