"""Reproduces https://github.com/fabricks-framework/fabricks/issues/203
(point 2): the rectify chain's `nxt` self-join against
`__rectified_next_operation` (find, for a key, whether it has a row at the
next "reload" batch's timestamp) was planned as a `SortMergeJoin` -- a
shuffle+sort of the full table on both sides -- even though the candidate
set on the `nxt` side is implicitly restricted to rows at a (normally
small, periodic) set of reload-batch timestamps.

`ctes/rectify.sql.jinja` now pre-filters that candidate set explicitly
(`__rectified_reload_candidates`, a valid pushdown of the join's own
equality condition -- zero semantic change) and hints both the `t` and
`nxt` sides of the join to broadcast. This checks the resulting physical
plan uses `BroadcastHashJoin` for that join instead of `SortMergeJoin`.
"""

import contextlib
import io

from pyspark.sql.types import Row

from fabricks.cdc import SCD2


def _final_plan_text(df):
    # explain(mode="formatted") prints an "== Initial Plan ==" section (
    # Spark's static pre-AQE guess -- can still show SortMergeJoin even with
    # a broadcast hint present) and an "== Final Plan ==" section (what
    # actually executed, only available once the query has run at least
    # once). Only the Final Plan section reflects real physical execution.
    df.collect()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    plan = buf.getvalue()
    final = plan.split("== Final Plan ==", 1)[1]
    return final.split("== Initial Plan ==", 1)[0]


def test_rectify_reload_lookup_uses_broadcast_not_sort_merge_join(local_spark):
    cdc = SCD2("cdc", "rectify_broadcast", spark=local_spark)

    seed_rows = [Row(id=i, name=f"n{i}", __timestamp="2024-01-01 00:00:00") for i in range(50)]
    cdc.update(local_spark.createDataFrame(seed_rows), keys="id", add_key=True)

    batch_rows = [Row(id=i, name=f"n{i}v2", __timestamp="2024-01-02 00:00:00") for i in range(50, 60)]
    batch_df = local_spark.createDataFrame(batch_rows)

    df = cdc.get_data(batch_df, mode="update", keys="id", add_key=True, rectify=True)
    final_plan = _final_plan_text(df)

    assert "SortMergeJoin" not in final_plan, (
        f"expected no SortMergeJoin in the executed rectify chain (issue #203 point 2):\n{final_plan}"
    )
    assert "BroadcastHashJoin" in final_plan, f"expected the reload-lookup join to broadcast:\n{final_plan}"
