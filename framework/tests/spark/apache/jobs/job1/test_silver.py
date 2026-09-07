import pytest

from tests.spark.apache.compare import compare_silver_to_expected

# (seed_from, iters, compare_to): king_and_queen_built seeds the target table
# from iteration `seed_from`'s *expected* output (seed_from=0 -> no seed, the
# from-empty-table bulk-load path), then runs one real SCD1/SCD2.update()
# call per iteration number in `iters`, in order, against that same table --
# each call's output feeding the next, with no reseeding in between.
# Comparison is always against iteration `compare_to`'s expected state.
#
# Most entries below are single-hop (seed from N-1's expected, run
# iteration N once) -- independent, seeded-from-expected steps that can run
# in any order, individually, or in parallel, see king_and_queen_built's
# docstring in conftest.py for why that's safe. The last two are deliberate
# multi-batch scenarios: chaining several real merges back-to-back (no
# oracle reseed between them) to check that a run of consecutive batches
# lands on the same state a fully independent, single-hop test would. Kept
# to a couple of scenarios, not every iteration, to avoid the O(n^2)
# full-chain replay this fixture was redesigned away from.
_SCENARIOS = [
    (0, [1], 1),
    (1, [2], 2),
    (2, [3], 3),
    (3, [4], 4),
    (4, [5], 5),
    (5, [6], 6),
    (6, [7], 7),
    (7, [8], 8),
    (8, [9], 9),
    # iter10: queen has only a delete op (nothing to delete against — a
    # no-op batch for queen). iter11: queen has no data at all this
    # iteration — king_and_queen_built skips it entirely, leaving queen's
    # rows from iter10 untouched in the target table. Both still have real
    # oracle SQL (tests/spark/expected/silver/{scd1,scd2}/iter{10,11}.sql),
    # same as 1-9.
    (9, [10], 10),
    (10, [11], 11),
    (3, [4, 5, 6, 7], 7),
    (0, [1, 2, 3, 4, 5, 6, 7, 8, 9], 9),
]


def _assert_scenario_consistent(seed_from, iters, compare_to):
    assert iters == list(range(seed_from + 1, iters[-1] + 1)), "iters must be the contiguous range right after seed_from"
    assert iters[-1] == compare_to, "compare_to must be iters' own last element"


@pytest.mark.order(10)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_silver_king_and_queen_scd2(local_spark, king_and_queen_built, seed_from, iters, compare_to):
    _assert_scenario_consistent(seed_from, iters, compare_to)
    scd2 = king_and_queen_built(seed_from, iters, "scd2")
    compare_silver_to_expected(local_spark, table=scd2.table, cdc="scd2", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(11)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_silver_king_and_queen_scd1(local_spark, king_and_queen_built, seed_from, iters, compare_to):
    _assert_scenario_consistent(seed_from, iters, compare_to)
    scd1 = king_and_queen_built(seed_from, iters, "scd1")
    compare_silver_to_expected(local_spark, table=scd1.table, cdc="scd1", iter=compare_to, topic="king_and_queen")
