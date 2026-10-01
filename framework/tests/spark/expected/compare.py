"""Real-Spark (Apache-tier) comparison helpers against the shared expected/
oracle (the "expected-state oracle" — see CONTEXT.md).

See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #7.

The QUALIFY-rewrite and schema-inference logic (`make_spark_compatible`/
`expected_scd2_schema`) is pure Python and lives in tests/support/expected_sql.py
so the plain tier can test it without a JVM. The Spark-dependent functions below import `pyspark.sql`/
`fabricks.metastore.table`/`fabricks.utils.dataframe` lazily, inside their own
bodies, so importing this module at all doesn't require a real
`fabricks.context` (same lazy-import pattern `tests/spark/apache/cdc_harness.py`
uses for its own heavy `fabricks.cdc` import).
"""

from __future__ import annotations

import os
from pathlib import Path
import re
from typing import TYPE_CHECKING

from tests.support.expected_sql import expected_scd2_schema, make_spark_compatible
from tests.support.fixture_data import EXPECTED_ROOT, read_expected_rows

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from fabricks.metastore.table import Table


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
            df = spark.createDataFrame(rows, schema=expected_scd2_schema(rows))
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
        spark.sql(make_spark_compatible(sql_file.read_text()))


def load_expected(spark: SparkSession, cdc: str, iter: int) -> DataFrame:
    return spark.read.table(f"expected.{cdc}_iter{iter}")


def compare_to_expected(spark: SparkSession, table: Table, cdc: str, iter: int, topic: str) -> None:
    df = table.dataframe

    expected_df = load_expected(spark, cdc, iter)
    if topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)
