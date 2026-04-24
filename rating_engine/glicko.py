"""
glicko.py

Python port of the RallyBase Glicko-lite rating engine (rallybase-glicko.ts).
Validated on 359,724 USATT matches. Brier score: 0.176 (vs 0.191 USATT baseline).

v1 Release Parameters (locked — do not change without re-validation):
  default_rating           = 1200.0
  default_rd               = 300.0
  default_sigma            = 0.06
  min_rating               = 100.0
  max_rd                   = 350.0
  min_rd                   = 40.0
  base_k                   = 120.0
  junior_rd_min            = 220.0
  inactivity_rd_growth_c   = 100.0
  winner_nonnegative       = True
  enable_score_modifier    = False

See: v1_release_report.md for full algorithm spec and design rationale.
"""

import math
from dataclasses import dataclass, replace

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_RATING = 1200.0
DEFAULT_RD = 300.0
DEFAULT_SIGMA = 0.06
MIN_RATING = 100.0
MAX_RD = 350.0
MIN_RD = 40.0
BASE_K = 120.0
JUNIOR_RD_MIN = 220.0
INACTIVITY_RD_GROWTH_C = 100.0


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class PlayerState:
    player_id: int
    rating: float = DEFAULT_RATING
    rd: float = DEFAULT_RD
    sigma: float = DEFAULT_SIGMA
    matches_played: int = 0
    last_active_day: int | None = None
    is_junior: bool = False


@dataclass
class MatchResult:
    winner_id: int
    loser_id: int
    match_day: int          # integer days since epoch (epoch = first match date in dataset)
    match_type: str = "tournament"   # "tournament" | "casual_rated" | "casual_unrated"


# ── Core functions ──────────────────────────────────────────────────────────────

def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def inflate_rd_for_inactivity(rd: float, days_inactive: int, is_junior: bool) -> float:
    """rd' = clamp(sqrt(rd^2 + c * days_inactive), MIN_RD, MAX_RD), with junior floor."""
    c = INACTIVITY_RD_GROWTH_C * (1.25 if is_junior else 1.0)
    inflated = math.sqrt(rd ** 2 + c * max(days_inactive, 0))
    clamped = clamp(inflated, MIN_RD, MAX_RD)
    return max(clamped, JUNIOR_RD_MIN) if is_junior else clamped


def expected_win_prob(rating_a: float, rating_b: float) -> float:
    """Standard Elo logistic: 1 / (1 + 10^(-(rA - rB) / 400))"""
    return 1.0 / (1.0 + 10.0 ** (-((rating_a - rating_b) / 400.0)))


def effective_k(player: PlayerState) -> float:
    """Scales BASE_K by RD, sigma, new-player boost, junior boost."""
    rd_factor = clamp(player.rd / 200.0, 0.6, 1.8)
    sigma_factor = clamp(player.sigma / DEFAULT_SIGMA, 0.7, 1.6)
    new_boost = 1.25 if player.matches_played < 20 else 1.0
    junior_boost = 1.1 if player.is_junior else 1.0
    return BASE_K * rd_factor * sigma_factor * new_boost * junior_boost


def _update_rd(rd: float, matches_played: int) -> float:
    multiplier = 0.92 if matches_played < 30 else 0.97
    return clamp(rd * multiplier, MIN_RD, MAX_RD)


def _update_sigma(sigma: float, delta: float) -> float:
    multiplier = 1.005 if abs(delta) > BASE_K else 0.995
    return clamp(sigma * multiplier, 0.03, 0.2)


def update_match(
    winner: PlayerState,
    loser: PlayerState,
    match: MatchResult,
) -> tuple[PlayerState, PlayerState]:
    """
    Process one match. Returns updated (winner, loser) states.
    Applies: inactivity inflation → win prob → effective_k → base delta
             → winner non-negative clamp → rating floor → RD/sigma update.
    """
    winner_days_inactive = 0 if winner.last_active_day is None else match.match_day - winner.last_active_day
    loser_days_inactive = 0 if loser.last_active_day is None else match.match_day - loser.last_active_day

    winner_rd = inflate_rd_for_inactivity(winner.rd, winner_days_inactive, winner.is_junior)
    loser_rd = inflate_rd_for_inactivity(loser.rd, loser_days_inactive, loser.is_junior)

    winner_expected = expected_win_prob(winner.rating, loser.rating)
    loser_expected = expected_win_prob(loser.rating, winner.rating)

    winner_k = effective_k(replace(winner, rd=winner_rd))
    loser_k = effective_k(replace(loser, rd=loser_rd))

    winner_delta = max(0.0, winner_k * (1.0 - winner_expected))
    loser_delta = loser_k * (0.0 - loser_expected)

    new_winner = replace(
        winner,
        rating=max(MIN_RATING, winner.rating + winner_delta),
        rd=_update_rd(winner_rd, winner.matches_played),
        sigma=_update_sigma(winner.sigma, winner_delta),
        matches_played=winner.matches_played + 1,
        last_active_day=match.match_day,
    )
    new_loser = replace(
        loser,
        rating=max(MIN_RATING, loser.rating + loser_delta),
        rd=_update_rd(loser_rd, loser.matches_played),
        sigma=_update_sigma(loser.sigma, loser_delta),
        matches_played=loser.matches_played + 1,
        last_active_day=match.match_day,
    )
    return new_winner, new_loser
