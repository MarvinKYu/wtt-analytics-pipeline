-- assert_winner_nonnegative.sql
-- Asserts that no player has a negative rating after a win.
-- Corresponds to the winner_nonnegative = True invariant in glicko.py.

select *
from {{ model }}
where rating < 0
