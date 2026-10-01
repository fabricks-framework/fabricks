"""Reproduces https://github.com/fabricks-framework/fabricks/issues/203
(point 2): the rectify chain's `nxt` self-join against `__rectified_next_operation` was planned
as a `SortMergeJoin` (shuffle and sort of the full table on both sides).

Invariant: the executed plan uses `BroadcastHashJoin` for that join, not `SortMergeJoin`.
"""

import contextlib
import io

from pyspark.sql.types import Row

from fabricks.cdc import SCD2


def _final_plan_text(df):
    # The Initial Plan is a pre-AQE guess that can show SortMergeJoin despite a broadcast hint; only Final Plan counts.
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

    # ids 45-49 overlap the seeded keys, so the rectify join actually has existing history to reconcile
    batch_rows = [Row(id=i, name=f"n{i}v2", __timestamp="2024-01-02 00:00:00") for i in range(45, 60)]
    batch_df = local_spark.createDataFrame(batch_rows)

    df = cdc.get_data(batch_df, mode="update", keys="id", add_key=True, rectify=True)
    final_plan = _final_plan_text(df)

    assert "SortMergeJoin" not in final_plan, (
        f"expected no SortMergeJoin in the executed rectify chain (issue #203 point 2):\n{final_plan}"
    )
    assert "BroadcastHashJoin" in final_plan, f"expected the reload-lookup join to broadcast:\n{final_plan}"


def test_rectify_with_overlapping_keys_versions_only_the_changed_keys(local_spark):
    cdc = SCD2("cdc", "rectify_overlap", spark=local_spark)
    cdc.update(
        local_spark.createDataFrame([Row(id=i, name=f"n{i}", __timestamp="2024-01-01 00:00:00") for i in range(3)]),
        keys="id",
        add_key=True,
    )

    cdc.update(
        local_spark.createDataFrame(
            [
                Row(id=1, name="n1v2", __timestamp="2024-01-02 00:00:00"),
                Row(id=3, name="n3", __timestamp="2024-01-02 00:00:00"),
            ]
        ),
        keys="id",
        add_key=True,
        rectify=True,
    )

    rows = cdc.table.dataframe.select("id", "name", "__is_current").orderBy("id", "__is_current").collect()
    assert [(r.id, r.name, r["__is_current"]) for r in rows] == [
        (0, "n0", True),
        (1, "n1", False),
        (1, "n1v2", True),
        (2, "n2", True),
        (3, "n3", True),
    ]
