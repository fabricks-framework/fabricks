"""https://github.com/fabricks-framework/fabricks/issues/66: a staging source
that legitimately goes fully empty has no per-key rows left to emit a
"delete" for -- the caller shouldn't have to enumerate every existing
primary key just to say "the source table is now empty". A single
`__operation == 'truncate'` sentinel row closes every currently-open record
instead. It's rewritten to a plain 'reload' row with a dummy key
(fabricks/cdc/templates/ctes/base.sql.jinja) so it reuses rectify's existing
per-key "not found in next reload" reconciliation
(ctes/rectify.sql.jinja) rather than a new all-or-nothing code path -- that's
what lets it interleave correctly with ordinary upserts across separate
incremental batches, proven by
test_truncate_interleaves_with_incremental_upserts below.
"""

from fabricks.cdc import SCD2


def test_truncate_closes_all_current_rows(local_spark):
    scd2 = SCD2("cdc", "truncate", "test", spark=local_spark)
    df1 = local_spark.createDataFrame(
        [(1, "a", "upsert", "2022-01-01 00:00:00"), (2, "b", "upsert", "2022-01-01 00:00:00")],
        ["id", "name", "__operation", "__timestamp"],
    )
    scd2.update(df1, keys="id", add_key=True)

    df2 = local_spark.createDataFrame([(None, None, "truncate", "2022-01-02 00:00:00")], df1.schema)
    scd2.update(df2, keys="id", add_key=True)

    rows = scd2.table.dataframe.collect()
    assert len(rows) == 2, "the truncate sentinel row itself must not become a new record"
    assert all(not row["__is_current"] for row in rows), "every prior row must be closed"


def test_truncate_interleaves_with_incremental_upserts(local_spark):
    scd2 = SCD2("cdc", "truncate_interleaved", "test", spark=local_spark)
    columns = ["id", "name", "__operation", "__timestamp"]

    df1 = local_spark.createDataFrame(
        [(1, "a", "upsert", "2022-01-01 00:00:00"), (2, "b", "upsert", "2022-01-01 00:00:00")], columns
    )
    scd2.update(df1, keys="id", add_key=True)

    scd2.update(
        local_spark.createDataFrame([(None, None, "truncate", "2022-01-02 00:00:00")], df1.schema),
        keys="id",
        add_key=True,
    )
    rows = scd2.table.dataframe.where("id in (1, 2)").collect()
    assert all(not row["__is_current"] for row in rows), "rows before the truncate must be closed"

    scd2.update(
        local_spark.createDataFrame([(3, "c", "upsert", "2022-01-03 00:00:00")], columns), keys="id", add_key=True
    )

    rows = scd2.table.dataframe.where("id in (1, 2)").collect()
    assert all(not row["__is_current"] for row in rows), "the truncate must not be undone by a later batch"
    row_3 = scd2.table.dataframe.where("id = 3").collect()[0]
    assert row_3["__is_current"], "an upsert after the truncate must open a new current row"
