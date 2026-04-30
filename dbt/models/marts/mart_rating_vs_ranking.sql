-- mart_rating_vs_ranking.sql
-- Gold layer: over/underranked analysis filtered to RD < 200 (high-confidence only).
-- divergence = ittf_rank - implied_rank
--   positive  → ITTF ranks player higher than the model does (overranked by ITTF)
--   negative  → ITTF ranks player lower than the model does (underranked by ITTF)
-- Threshold of ±10 for aligned vs over/underranked (within noise for top-100 field).

with
base as (
    select * from {{ ref('mart_player_ratings') }}
),

filtered as (
    select *
    from base
    where rd < 200
),

final as (
    select
        player_id,
        name,
        association,
        gender,
        ittf_rank,
        implied_rank,
        divergence,
        case
            when divergence >  10 then 'overranked'
            when divergence < -10 then 'underranked'
            else 'aligned'
        end                                                                   as divergence_category,
        rating,
        rd,
        matches_played,
        round(percent_rank() over (partition by gender order by rating desc) * 100, 1) as rating_percentile,
        computed_at
    from filtered
)

select * from final
