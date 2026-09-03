import pytest

from tests.local.compare import compare_silver_to_expected

# (jobs, compare_to): jobs are applied in order, each via its own
# SCD.update() call; compare_to is the expected/**/job{N}.sql this
# scenario's final state should match. Every contiguous prefix of job1
# through job9 (the full range Task 8 derives) — exercises both a
# from-empty-table bulk load (jobs=[1]) and the incremental-merge-into-an-
# already-populated-table code path every jobs=[..., N] entry beyond the
# first job adds. A scenario that *skips* job1 (e.g. jobs=[2, 3], comparing
# to job03) is a different starting point, not a longer chain — deliberately
# not included here since job2 alone was never verified as its own valid
# starting schema (job1 is what establishes the base columns every later
# job's autoMerge-handled drift builds on); add such an entry only after
# confirming job2's raw data is independently mergeable into an empty table.
# See king_and_queen_built's "Why job-sequential" note for why the job
# boundary (not the raw landing-batch boundary) is what needs sequencing.
_SCENARIOS = [
    ([1], 1),
    ([1, 2], 2),
    ([1, 2, 3], 3),
    ([1, 2, 3, 4], 4),
    ([1, 2, 3, 4, 5], 5),
    ([1, 2, 3, 4, 5, 6], 6),
    ([1, 2, 3, 4, 5, 6, 7], 7),
    ([1, 2, 3, 4, 5, 6, 7, 8], 8),
    ([1, 2, 3, 4, 5, 6, 7, 8, 9], 9),
]


@pytest.mark.order(10)
@pytest.mark.parametrize("jobs,compare_to", _SCENARIOS)
def test_silver_king_and_queen_scd2(local_spark, king_and_queen_built, jobs, compare_to):
    built = king_and_queen_built(jobs)
    compare_silver_to_expected(local_spark, table=built["scd2"].table, cdc="scd2", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(11)
@pytest.mark.parametrize("jobs,compare_to", _SCENARIOS)
def test_silver_king_and_queen_scd1(local_spark, king_and_queen_built, jobs, compare_to):
    built = king_and_queen_built(jobs)
    compare_silver_to_expected(local_spark, table=built["scd1"].table, cdc="scd1", iter=compare_to, topic="king_and_queen")
