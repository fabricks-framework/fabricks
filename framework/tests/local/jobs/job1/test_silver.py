import pytest

from tests.local.compare import compare_silver_to_expected

# job1 through job9 (the full range Task 8 derives), applied in order, each
# via its own SCD.update() call against whatever state the previous job left
# behind — exercises both a from-empty-table bulk load (job 1) and the
# incremental-merge-into-an-already-populated-table code path every job
# after it adds. Starting anywhere but job1 is a different starting point,
# not a shorter chain — deliberately not exercised since job2 alone was
# never verified as its own valid starting schema (job1 is what establishes
# the base columns every later job's autoMerge-handled drift builds on).
# See king_and_queen_built's "Why job-sequential" note for why the job
# boundary (not the raw landing-batch boundary) is what needs sequencing.
_JOBS = list(range(1, 10))


@pytest.mark.order(10)
def test_silver_king_and_queen(local_spark, king_and_queen_built):
    # scd2 and scd1 must both be compared right after job N is applied, not
    # in two separate loops over 1..9 - king_and_queen_built(N) advances the
    # one shared, live silver table, so a comparison made after the table
    # has moved on to a later job would be comparing against the wrong
    # (later) state (verified: reproduced exactly, splitting these into two
    # loops made the second loop's iter=1 comparison see job9's final state).
    for job_num in _JOBS:
        built = king_and_queen_built(job_num)
        compare_silver_to_expected(local_spark, table=built["scd2"].table, cdc="scd2", iter=job_num, topic="king_and_queen")
        compare_silver_to_expected(local_spark, table=built["scd1"].table, cdc="scd1", iter=job_num, topic="king_and_queen")
