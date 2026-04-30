-- mart_player_ratings.sql
-- Gold layer: final player ratings joined with the latest ITTF snapshot.
-- implied_rank is computed per gender, ordered by rating descending.
-- divergence = ittf_rank - implied_rank: positive means ITTF ranks the player
-- higher than the model does (overranked by ITTF).

with
ratings as (
    select * from {{ source('wtt', 'mart_player_ratings_raw') }}
),

latest_rankings as (
    select
        player_id,
        name,
        association,
        gender,
        rank    as ittf_rank,
        points  as ittf_points
    from {{ ref('stg_rankings') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_rankings') }})
),

joined as (
    select
        r.player_id,
        lr.name,
        lr.association,
        lr.gender,
        r.rating,
        r.rd,
        r.sigma,
        r.matches_played,
        lr.ittf_rank,
        lr.ittf_points,
        r.computed_at
    from ratings r
    inner join latest_rankings lr on lr.player_id = r.player_id
),

with_implied_rank as (
    select
        *,
        row_number() over (partition by gender order by rating desc) as implied_rank
    from joined
),

final as (
    select
        player_id,
        name,
        association,
        gender,
        rating,
        rd,
        sigma,
        matches_played,
        ittf_rank,
        ittf_points,
        implied_rank,
        ittf_rank - implied_rank as divergence,
        computed_at
    from with_implied_rank
)

select * from final
