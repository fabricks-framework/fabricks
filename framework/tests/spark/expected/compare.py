"""Real-Spark (Apache-tier) comparison helpers against the shared expected/
oracle (the "expected-state oracle" — see CONTEXT.md).

See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #7.

The QUALIFY-rewrite and schema-inference logic below (`_make_spark_compatible`/
`_expected_scd2_schema`) is pure Python — no Spark session needed to exercise
it — kept testable from `tests/unit/plain/test_compare.py` without paying for
a JVM. The Spark-dependent functions below import `pyspark.sql`/
`fabricks.metastore.table`/`fabricks.utils.dataframe` lazily, inside their own
bodies, so importing this module at all doesn't require a real
`fabricks.context` (same lazy-import pattern `tests/spark/apache/cdc_harness.py`
uses for its own heavy `fabricks.cdc` import).
"""

from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import re
from typing import TYPE_CHECKING

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from tests.spark.test_data import SPARK_TEST_ROOT, read_ndjson

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from fabricks.metastore.table import Table

EXPECTED_ROOT = SPARK_TEST_ROOT / "expected"


def read_expected_rows(iteration: int) -> list[dict]:
    rows = read_ndjson(EXPECTED_ROOT / "scd2" / f"iter{iteration:02}.jsonl")
    for row in rows:
        row["__valid_from"] = _utc_timestamp(row["__valid_from"])
        row["__valid_to"] = _utc_timestamp(row["__valid_to"])
        if isinstance(row.get("newField"), str):
            row["newField"] = {"true": True, "false": False, "null": None}[row["newField"]]
    return rows


def _utc_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def assert_dfs_equal(df: DataFrame, df_expected: DataFrame) -> None:
    from pandas.testing import assert_frame_equal
    from pyspark.sql.functions import expr

    from fabricks.utils.dataframe import boolean_as_string, decimal_to_double, timestamp_as_string, value_to_none

    cols = df_expected.columns
    order_by = "id"
    if "__valid_from" in df.columns:
        order_by = f"concat_ws('|', {order_by}, __valid_from, __valid_to)"
    elif "valid_from" in df.columns:
        order_by = f"concat_ws('|', {order_by}, valid_from, valid_to)"

    def _transform(df_: DataFrame) -> DataFrame:
        df_ = df_.withColumn("order_by", expr(order_by)).orderBy("order_by").select(cols)
        df_ = df_.transform(decimal_to_double)
        df_ = df_.transform(timestamp_as_string)
        df_ = df_.transform(boolean_as_string)
        return df_.transform(value_to_none)

    df = _transform(df)
    p_df = df.toPandas()

    df_expected = _transform(df_expected)
    p_df_expected = df_expected.toPandas()

    assert_frame_equal(p_df, p_df_expected, check_dtype=False)


# OSS Apache Spark (this local test container) has no QUALIFY clause support
# -- it's a Databricks SQL extension. tests/spark/expected/scd1/iter*.sql (the
# correctness oracle, unmodified) all use it: `select * except (...) from
# <src> qualify row_number() over (...) = N`. Verified empirically against
# this container's real Spark session: `select * except (b) from t` parses
# fine (OSS Spark does support star-except), but `... qualify rn = 1` raises
# PARSE_SYNTAX_ERROR -- QUALIFY alone is the unsupported part. sqlglot's
# generic databricks->spark QUALIFY elimination (verified via
# sqlglot.transpile(sql, read="databricks", write="spark")) rewrites this
# correctly for an explicit column list, but for a `select *` projection it
# leaves its own window-function helper column exposed in the outer `SELECT
# *`, corrupting the view's column set -- so a hand-rolled rewrite is used
# instead, run only at load time against our local Spark session; the
# checked-in oracle file itself is never touched.
# ponytail: handles exactly the one recurring shape verified identical across
# all 9 scd1 oracle files (`select * except (...) from <src> qualify
# row_number() over (...) = N`), not a general QUALIFY-eliminating SQL
# parser -- widen the regex (or reach for a real AST-based rewrite) if a
# future oracle file ever uses a different QUALIFY shape.
_QUALIFY_RE = re.compile(
    r"select \*\s*except \((?P<except_cols>[^)]*)\)\s*from (?P<src>\S+) "
    r"qualify (?P<rn_expr>row_number\(\) over \(.*?\)) = (?P<rn_val>\d+)\s*\Z",
    re.IGNORECASE | re.DOTALL,
)


def _make_spark_compatible(sql: str) -> str:
    match = _QUALIFY_RE.search(sql)
    if not match:
        return sql

    exc = match.group("except_cols")
    src = match.group("src")
    rn_expr = match.group("rn_expr")
    rn_val = match.group("rn_val")
    replacement = (
        f"select * except ({exc}, __qualify_rn) from (\n"
        f"    select *, {rn_expr} as __qualify_rn\n"
        f"    from {src}\n"
        f") where __qualify_rn = {rn_val}"
    )
    return sql[: match.start()] + replacement + sql[match.end() :]


_EXPECTED_SCD2_BASE_SCHEMA = StructType(
    [
        StructField("__valid_from", TimestampType(), True),
        StructField("__valid_to", TimestampType(), True),
        # LongType, not IntegerType: raw bronze JSON's plain integer `id`
        # values infer to bigint under spark.read.json() (confirmed: this
        # container's real Spark session, `id (int -> bigint)`). Comparison
        # via assert_dfs_equal never noticed the mismatch (int vs. bigint
        # values compare equal after its own normalization), but seeding a
        # table from this schema (CDC scenario seeding) writes the *type*
        # verbatim, producing a genuine int-vs-bigint schema difference the
        # next update_schema() call detects and "fixes" for no reason.
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)

# newField: added via a per-file StructField, not folded into the base schema
# above, and not unconditionally on every iteration's NDJSON, unlike the now-
# deleted Databricks-cluster suite's equivalent schema (commit 8a7cb451).
# That was correct for the Databricks-cluster suite, where
# king_and_queen's real job config declares a fixed bronze schema
# up front (so bronze.king already carries a NULL newField column from its
# very first landing batch, before iter2 ever supplies a real value) -- but
# this plan has no job config/parsers layer at all (see run_cdc_scenario
# in conftest.py: raw spark.read.json() per iteration's own NDJSON file, one column
# set per file, autoMerge only ever *adding* columns once a later iteration's data
# introduces them). So a iters=[1]-only target genuinely has no
# newField column at all (verified empirically: UNRESOLVED_COLUMN against
# this container's real Spark session when the expected side forced newField
# into iter1's comparison) -- matching iter1's own raw fixture, which has no
# "newField" key in any row (verified: tests/spark/apache/fixtures/iter1/bronze_*
# .jsonl and the original tests/spark/fixtures/iter1/king/**/*.jsonl landing fixture
# both lack the key entirely; iter2 onward always carry it). Detecting the
# key's presence per-file (rather than hardcoding "iter1 is the exception")
# keeps this correct if a future iteration's fixture composition changes.
# BooleanType, not StringType: every raw fixture's `newField` value is a
# genuine JSON boolean (`true`/`false`, verified across all of iter2-9's
# fixtures, never a string) -- same int-vs-bigint reasoning as `id` above,
# this only mattered once this schema started feeding CDC scenario seeding.
_NEW_FIELD = StructField("newField", BooleanType(), True)


def _expected_scd2_schema(rows: list[dict]) -> StructType:
    if any("newField" in row for row in rows):
        return StructType([*_EXPECTED_SCD2_BASE_SCHEMA.fields, _NEW_FIELD])
    return _EXPECTED_SCD2_BASE_SCHEMA


def create_expected_views(spark: SparkSession, cdc: str) -> None:
    views_dir = EXPECTED_ROOT / cdc

    # tests/spark/databricks/utils.py, which this module's create_expected_views
    # once mirrored fixes into (commits 8a7cb451, d4f2498e), was deleted in
    # bdda89d6 ("remove old tests") - this is the sole create_expected_views now.

    if cdc == "scd2":
        # Only iter1's file is hand-authored NDJSON data — iter2.sql onward are
        # still real SQL, each unioning its own new VALUES rows with `select
        # ... from expected.scd2_iter{N-1} where not __is_current`
        # (verified: iter3.sql references iter2, iter2.sql references iter1 — a
        # genuine sequential chain). Create iter1's NDJSON root, then fall
        # through to the SQL loop below for iter2 onward in ascending order —
        # an early `return` here would silently skip
        # expected.scd2_iter{2..9} entirely, since Task 9's
        # run_cdc_scenario only calls create_expected_views for
        # "scd2"/"scd1", not per iteration.
        for ndjson_file in sorted(views_dir.glob("*.jsonl")):
            # str(int(...)): strip the filename's leading zero (iter01.jsonl ->
            # "01") so it matches the un-padded table name (scd2_iter1)
            # the scd1/iter*.sql oracle views reference -- without it,
            # "scd2_iter01" is created but "scd2_iter1" (what iter1.sql
            # selects from) is never found.
            match = re.search(r"\d+", ndjson_file.stem)
            assert match, f"no iteration number in {ndjson_file.name}"
            iter_num = str(int(match.group()))
            rows = read_expected_rows(int(iter_num))
            df = spark.createDataFrame(rows, schema=_expected_scd2_schema(rows))
            expected_table = f"expected.scd2_iter{iter_num}"
            expected_cache = os.environ.get("FABRICKS_TEST_EXPECTED_CACHE")
            if expected_cache:
                cache_path = Path(expected_cache) / f"scd2_iter{iter_num}"
                if not (cache_path / "_delta_log").exists():
                    df.write.format("delta").save(str(cache_path))
                spark.sql(f"create table {expected_table} using delta location '{cache_path}'")
            else:
                df.write.mode("overwrite").saveAsTable(expected_table)

    if cdc == "scd0":
        # No dedicated oracle files -- generated straight from
        # expected.scd2_iter{N} (same data expected.scd1_iter{N}
        # is itself derived from). CDC merge correctness doesn't depend on
        # which database a table lives in, so scd0 is proven here exactly
        # like scd1/scd2 are, no separate rename/duplicate oracle needed.
        # First-insert-per-key wins, so iterN's view is iterN-1's own rows
        # plus only the *new* ids iterN's snapshot introduces.
        for iter_num in range(1, 12):
            snapshot = f"""
                select id, name, doubleField, __is_current
                from expected.scd2_iter{iter_num}
            """
            if iter_num == 1:
                body = f"select id, name, doubleField from ({snapshot}) s1 where __is_current"
            else:
                prev = f"expected.scd0_iter{iter_num - 1}"
                body = f"""
                    select id, name, doubleField from {prev}
                    union all
                    select id, name, doubleField from ({snapshot}) s1
                    left anti join {prev} s0 on s1.id = s0.id
                    where __is_current
                """
            spark.sql(f"create or replace view expected.scd0_iter{iter_num} as {body}")
        return

    for sql_file in sorted(views_dir.glob("*.sql")):
        spark.sql(_make_spark_compatible(sql_file.read_text()))


def load_expected(spark: SparkSession, cdc: str, iter: int) -> DataFrame:
    return spark.read.table(f"expected.{cdc}_iter{iter}")


def compare_to_expected(spark: SparkSession, table: Table, cdc: str, iter: int, topic: str) -> None:
    df = table.dataframe

    expected_df = load_expected(spark, cdc, iter)
    if topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)
