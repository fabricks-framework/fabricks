"""Generated-SQL coverage for Processor.get_query() and Merger.get_merge_query(), the query-generation
half of CDC behavior (the Apache tier compares materialized results only).

CDC objects are built directly with a bare MagicMock in place of Spark. `src` is a
`MagicMock(spec=DataFrame)`: Configurator.get_src() needs a real `isinstance(src, DataFrameLike)`, and
a spec'd mock passes it while `.columns` stays a plain list. Assertions anchor to CTE/column names that
are unique to each cdc type in the jinja templates (e.g. `__scd0_next_operation`,
`__scd2_next_timestamp`), not to the final projection, so they hold whichever of
__key/__hash/__operation lands in the final SELECT.
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
    # order_duplicate_by forces deduplicate_key=True unconditionally
    # (Processor.get_query_context), even for nocdc, which otherwise has no
    # deduplication at all - isolates the CTE's presence cleanly.
    cdc = _cdc(NoCDC)

    sql = cdc.get_query(src(["id", "name"]), mode="complete", order_duplicate_by={"name": "asc"})

    assert "__deduplicated_key" in sql
    assert re.search(r"`?name`?\s+asc", sql, re.IGNORECASE)


def test_get_query_without_order_duplicate_by_has_no_dedup_cte():
    cdc = _cdc(NoCDC)

    sql = cdc.get_query(src(["id", "name"]), mode="complete")

    assert "__deduplicated_key" not in sql


def test_get_query_scd2_update_mode_takes_incremental_branch():
    # mode="update" always renders scd2's __merge_condition branch instead
    # of the complete-mode __complete branch, regardless of has_rows (which
    # only gates an inner sub-CTE) - forcing slice="update" directly as a
    # kwarg (mirroring what Gold/Silver's own build_cdc_context would compute)
    # sidesteps needing Table.rows/registered to behave like a real table.
    # fix_context()'s own spark.sql(...).collect()[0] probe (the
    # slice-filter's row/source count) is what needs a deterministic Row.
    spark = fake_spark()
    spark.sql.return_value.collect.return_value = [Row(slices=["s.__timestamp > '2024-01-01'"], sources=None)]
    cdc = _cdc(SCD2, spark=spark)

    sql = cdc.get_query(src(["id", "name"]), mode="update", slice="update")

    assert "__merge_condition" in sql
    assert "__complete" not in sql


# ============================= get_merge_query ==============================


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
    # scd0's merge template has only "when not matched ... insert" - a key
    # already in the target is never touched again by a later merge.
    cdc = _merge_cdc(SCD0)

    sql = cdc.get_merge_query(_merge_src(["id", "__merge_key", "__merge_condition", "__key"]))

    assert "merge into" in sql.lower()
    assert "when matched" not in sql.lower()
    assert "when not matched" in sql.lower()


def test_get_merge_query_nocdc_hard_deletes():
    cdc = _merge_cdc(NoCDC)

    sql = cdc.get_merge_query(_merge_src(["id", "__merge_key", "__merge_condition", "__key"]))

    assert "merge into" in sql.lower()
    assert "when matched" in sql.lower()
    assert "delete" in sql.lower()
    assert "__is_deleted" not in sql


def test_get_merge_query_scd1_soft_deletes_when_is_deleted_column_present():
    cdc = _merge_cdc(SCD1)

    sql = cdc.get_merge_query(
        _merge_src(["id", "__merge_key", "__merge_condition", "__key", "__hash", "__is_deleted"])
    )

    assert "merge into" in sql.lower()
    assert "__is_deleted" in sql
    assert "__is_current" in sql


def test_get_merge_query_scd1_hard_deletes_when_is_deleted_column_absent():
    cdc = _merge_cdc(SCD1)

    sql = cdc.get_merge_query(_merge_src(["id", "__merge_key", "__merge_condition", "__key", "__hash"]))

    assert "merge into" in sql.lower()
    assert "when matched" in sql.lower()
    assert "delete" in sql.lower()
    assert "__is_deleted" not in sql


def test_get_merge_query_scd2_always_soft_deletes_via_is_current():
    cdc = _merge_cdc(SCD2)

    sql = cdc.get_merge_query(_merge_src(["id", "__merge_key", "__merge_condition", "__key", "__hash"]))

    assert "merge into" in sql.lower()
    assert "__valid_to" in sql
    assert "__is_current" in sql
