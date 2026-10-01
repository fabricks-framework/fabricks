"""Generated-SQL coverage for Processor.get_query() and Merger.get_merge_query(), the query-generation
half of CDC behavior (the Apache tier compares materialized results only).

CDC objects use a bare MagicMock for Spark; `src` is a `MagicMock(spec=DataFrame)` so get_src()'s isinstance check
passes while `.columns` stays a plain list. Assertions anchor to CTE names unique to each cdc type (e.g.
`__scd0_next_operation`), not the final projection, so they hold whichever of __key/__hash/__operation is selected.
"""

import re
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.cdc import SCD0, SCD1, SCD2, NoCDC
from tests.unit.config._helpers import fake_spark, src

_SCD_MARKERS = {
    "nocdc": (),
    "scd0": ("__scd0_next_operation",),
    "scd1": ("__scd1_next_operation",),
    "scd2": ("__scd2_next_timestamp", "__valid_from", "__valid_to"),
}
_ALL_MARKERS = {m for markers in _SCD_MARKERS.values() for m in markers}


def _cdc(cls, spark=None):
    return cls("cdc", "query_gen", spark=spark or fake_spark())


def _assert_only_markers_for(cdc_type, sql):
    own_markers = _SCD_MARKERS[cdc_type]
    for marker in own_markers:
        assert marker in sql, f"expected {marker!r} in {cdc_type} query"
    for marker in _ALL_MARKERS - set(own_markers):
        assert marker not in sql, f"did not expect {marker!r} in {cdc_type} query"


def test_get_query_complete_mode_nocdc_has_no_scd_markers():
    cdc = _cdc(NoCDC)
    sql = cdc.get_query(src(["id", "name"]), mode="complete")
    _assert_only_markers_for("nocdc", sql)


def test_get_query_complete_mode_scd0_has_only_scd0_markers():
    cdc = _cdc(SCD0)
    sql = cdc.get_query(src(["id", "name"]), mode="complete")
    _assert_only_markers_for("scd0", sql)


def test_get_query_complete_mode_scd1_has_only_scd1_markers():
    cdc = _cdc(SCD1)
    sql = cdc.get_query(src(["id", "name"]), mode="complete")
    _assert_only_markers_for("scd1", sql)


def test_get_query_complete_mode_scd2_has_only_scd2_markers():
    cdc = _cdc(SCD2)
    sql = cdc.get_query(src(["id", "name"]), mode="complete")
    _assert_only_markers_for("scd2", sql)


def test_get_query_order_duplicate_by_adds_dedup_cte():
    # order_duplicate_by forces deduplicate_key=True even for nocdc, which otherwise has no dedup.
    cdc = _cdc(NoCDC)

    sql = cdc.get_query(src(["id", "name"]), mode="complete", order_duplicate_by={"name": "asc"})

    flat = " ".join(sql.split())
    assert "__deduplicated_key" in flat
    assert "PARTITION BY `__key` ORDER BY `name` ASC) = 1" in flat, "the ordering must feed the per-key dedup window"


def test_get_query_without_order_duplicate_by_has_no_dedup_cte():
    cdc = _cdc(NoCDC)

    sql = cdc.get_query(src(["id", "name"]), mode="complete")

    assert "__deduplicated_key" not in sql


def test_get_query_scd2_update_mode_takes_incremental_branch():
    # slice="update" is passed directly so Table.rows/registered need not behave like a real table;
    # fix_context()'s spark.sql(...).collect()[0] probe still needs a deterministic Row.
    spark = fake_spark()
    spark.sql.return_value.collect.return_value = [Row(slices=["s.__timestamp > '2024-01-01'"], sources=None)]
    cdc = _cdc(SCD2, spark=spark)

    sql = cdc.get_query(src(["id", "name"]), mode="update", slice="update")

    assert "__merge_condition" in sql
    assert "__complete" not in sql


def _query(cls, columns=("id", "name"), **kwargs):
    spark = fake_spark()
    spark.sql.return_value.collect.return_value = [Row(slices="( s.__timestamp > '2024-01-01' )", sources=None)]
    return " ".join(_cdc(cls, spark=spark).get_query(src(list(columns), is_empty=False), **kwargs).split())


def test_get_query_slice_latest_renders_the_sliced_cte():
    sql = _query(SCD1, columns=("id", "name", "__timestamp"), mode="complete", slice="latest")

    assert "`__sliced` AS (" in sql, "the CTE must be defined, not merely referenced"


def test_get_query_without_a_slice_has_no_sliced_cte():
    assert "`__sliced` AS (" not in _query(SCD1, columns=("id", "name", "__timestamp"), mode="complete")


def test_get_query_scd2_correct_valid_from_uses_the_1900_sentinel_only_when_asked():
    assert "1900-01-01" in _query(SCD2, mode="complete", correct_valid_from=True)
    assert "1900-01-01" not in _query(SCD2, mode="complete", correct_valid_from=False)


def test_get_query_update_mode_takes_the_incremental_branch_for_scd1_and_nocdc_too():
    for cls in (SCD1, NoCDC):
        update = _query(cls, columns=("id", "name", "__timestamp"), mode="update", slice="update")
        complete = _query(cls, columns=("id", "name", "__timestamp"), mode="complete")

        assert "__merge_condition" in update, cls.__name__
        assert "__merge_condition" not in complete, cls.__name__


def _merge_src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    return df


def _merge_cdc(cls):
    spark = fake_spark()
    spark.catalog.tableExists.return_value = True  # Table.registered
    spark.sql.return_value.collect.return_value = [[0]]  # Table.rows (merger.py:42)
    return cls("cdc", "merge_gen", spark=spark)


def test_get_merge_query_scd0_has_no_when_matched_clause():
    # scd0 only inserts unmatched keys; an existing key is never touched again.
    cdc = _merge_cdc(SCD0)

    sql = cdc.get_merge_query(_merge_src(["id", "__merge_key", "__merge_condition", "__key"]))

    assert "merge into" in sql.lower()
    assert "when matched" not in sql.lower()
    assert "when not matched" in sql.lower()


_COLUMNS = ["id", "__merge_key", "__merge_condition", "__key", "__hash"]


def _merge_clauses(sql: str) -> list[tuple[str, str]]:
    """(merge condition, action) of every WHEN clause, in order, e.g. ('delete', 'DELETE')."""
    flat = " ".join(sql.split())
    found = re.findall(r"WHEN (?:NOT )?MATCHED AND `__merge_condition` = '(\w+)' THEN (\w+)", flat, re.IGNORECASE)
    return [(condition, action.upper()) for condition, action in found]


def test_get_merge_query_nocdc_hard_deletes():
    sql = _merge_cdc(NoCDC).get_merge_query(_merge_src(_COLUMNS))

    assert _merge_clauses(sql) == [("upsert", "UPDATE"), ("delete", "DELETE"), ("upsert", "INSERT")]
    assert "__is_deleted" not in sql


def test_get_merge_query_scd1_soft_deletes_when_is_deleted_column_present():
    sql = _merge_cdc(SCD1).get_merge_query(_merge_src([*_COLUMNS, "__is_deleted"]))

    assert _merge_clauses(sql) == [("upsert", "UPDATE"), ("delete", "UPDATE"), ("upsert", "INSERT")]
    assert "`__is_current` = FALSE , `__is_deleted` = TRUE" in " ".join(sql.split())


def test_get_merge_query_scd1_hard_deletes_when_is_deleted_column_absent():
    sql = _merge_cdc(SCD1).get_merge_query(_merge_src(_COLUMNS))

    assert _merge_clauses(sql) == [("upsert", "UPDATE"), ("delete", "DELETE"), ("upsert", "INSERT")]
    assert "__is_deleted" not in sql


def test_get_merge_query_scd2_always_closes_versions_and_never_deletes():
    sql = _merge_cdc(SCD2).get_merge_query(_merge_src(_COLUMNS))

    assert _merge_clauses(sql) == [("update", "UPDATE"), ("delete", "UPDATE"), ("insert", "INSERT")]
    assert "`__is_current` = FALSE" in " ".join(sql.split())
