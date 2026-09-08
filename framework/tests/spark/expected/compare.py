"""Real-Spark (Apache-tier) comparison helpers against the shared expected/ oracle.

See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #7.
"""

from datetime import UTC, datetime
import json
from pathlib import Path
import re

from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from fabricks.metastore.table import Table
from fabricks.utils.dataframe import boolean_as_string, decimal_to_double, timestamp_as_string, value_to_none


def assert_dfs_equal(df: DataFrame, df_expected: DataFrame) -> None:
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

_EXPECTED_ROOT = Path(__file__).resolve().parent  # this file now lives inside tests/spark/expected/ itself

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
        # table from this schema (seed_table) writes the *type*
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
# above, and NOT unconditionally on every iteration's NDJSON the way
# tests/spark/databricks/utils.py's _EXPECTED_SCD2_SCHEMA does it (commit
# 8a7cb451). That's correct for the Databricks-cluster suite, where
# king_and_queen's real job config declares a fixed bronze schema
# up front (so bronze.king already carries a NULL newField column from its
# very first landing batch, before iter2 ever supplies a real value) -- but
# this plan has no job config/parsers layer at all (see king_and_queen_built
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
# this only mattered once this schema started feeding seed_table.
_NEW_FIELD = StructField("newField", BooleanType(), True)


def _expected_scd2_schema(rows: list[dict]) -> StructType:
    if any("newField" in row for row in rows):
        return StructType([*_EXPECTED_SCD2_BASE_SCHEMA.fields, _NEW_FIELD])
    return _EXPECTED_SCD2_BASE_SCHEMA


def create_expected_views(spark: SparkSession, cdc: str) -> None:
    views_dir = _EXPECTED_ROOT / cdc

    if cdc == "scd2":
        # Only iter1's file is hand-authored NDJSON data — iter2.sql onward are
        # still real SQL, each unioning its own new VALUES rows with `select
        # ... from expected.scd2_iter{N-1} where not __is_current`
        # (verified: iter3.sql references iter2, iter2.sql references iter1 — a
        # genuine sequential chain). Create iter1's NDJSON root, then fall
        # through to the SQL loop below for iter2 onward in ascending order —
        # an early `return` here would silently skip
        # expected.scd2_iter{2..9} entirely, since Task 9's
        # king_and_queen_built only calls create_expected_views for
        # "scd2"/"scd1", not per iteration. See Task 3's
        # mirrored fix in tests/spark/databricks/utils.py's create_expected_views.
        for ndjson_file in sorted(views_dir.glob("*.jsonl")):
            # str(int(...)): strip the filename's leading zero (iter01.jsonl ->
            # "01") so it matches the un-padded table name (scd2_iter1)
            # the scd1/iter*.sql oracle views reference. Same fix as
            # tests/spark/databricks/utils.py's create_expected_views (commit
            # d4f2498e) -- without it, "scd2_iter01" is created but
            # "scd2_iter1" (what iter1.sql selects from) is never found.
            iter_num = str(int(re.search(r"\d+", ndjson_file.stem).group()))
            rows = [json.loads(line) for line in ndjson_file.read_text().splitlines()]
            for row in rows:
                # __valid_from/__valid_to are plain "YYYY-MM-DD HH:MM:SS"
                # strings in the NDJSON; TimestampType.toInternal() requires
                # an actual datetime (it calls .utctimetuple()/.timetuple()),
                # so a raw string fails schema conversion. tzinfo=utc pins
                # these as instants rather than leaving them naive (which
                # would otherwise be localized using the JVM/driver's local
                # timezone) — mirrors tests/spark/databricks/utils.py's identical
                # parsing for the same NDJSON files.
                row["__valid_from"] = datetime.strptime(row["__valid_from"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=UTC
                )
                row["__valid_to"] = datetime.strptime(row["__valid_to"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=UTC
                )
                # newField was committed to these NDJSON files as a mix of
                # stringified literals ("true"/"false"/"null") and genuine
                # JSON null (verified: grep across every iter0N.jsonl) --
                # predates this schema using a real BooleanType (it used to
                # be StringType, which silently accepted any string, and
                # assert_dfs_equal's own string-normalizing comparison never
                # noticed). Normalize here rather than re-loosen the schema
                # back to StringType, which would just resurface the same
                # spurious update_schema()-triggering type mismatch this
                # schema exists to avoid (see _EXPECTED_SCD2_BASE_SCHEMA).
                if isinstance(row.get("newField"), str):
                    row["newField"] = {"true": True, "false": False, "null": None}[row["newField"]]
            df = spark.createDataFrame(rows, schema=_expected_scd2_schema(rows))
            df.write.mode("overwrite").saveAsTable(f"expected.scd2_iter{iter_num}")

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


def load_expected_scd2_seed(spark: SparkSession, iter: int) -> DataFrame:
    """Like load_expected(spark, "scd2", iter), but also adds `__timestamp`
    (copied from `__valid_from`) — needed only for seeding an SCD2 table,
    not for comparison. Same underlying gap as load_expected_scd1_seed's:
    `processor.py` persists `__timestamp` as a real output column whenever
    `has_timestamp` is true, regardless of cdc type — `expected`'s own SCD2
    schema never had it (current.sql.jinja's `__current` CTE derives SCD2's
    `__timestamp` from `__valid_from` when reading a *real* target back, so
    it isn't needed there), but a seeded table missing it looks like a
    genuine schema difference to king_and_queen_built's own
    "does the incoming batch have a column the target doesn't" check —
    triggering a real (if harmless) `update_schema()` call on every iteration
    instead of skipping it, exactly what that check exists to avoid.
    """
    return load_expected(spark, "scd2", iter).selectExpr("*", "__valid_from as __timestamp")


def load_expected_scd1_seed(spark: SparkSession, iter: int) -> DataFrame:
    """Like load_expected(spark, "scd1", iter), but keeps `__valid_from`
    (renamed `__timestamp`) instead of dropping it — needed only for seeding
    an SCD1 table (see seed_table), not for comparison.

    `expected.scd1_iter{N}`'s own schema has no `__timestamp` column
    (tests/spark/expected/scd1/iter{N}.sql: `select * except (__valid_from,
    __valid_to) from expected.scd2_iter{N} qualify row_number() over
    (partition by id order by __valid_to desc) = 1`) — fine for *comparing*
    against a real table (compare_to_expected only ever selects
    `expected`'s own columns), but not for *seeding* one: a real SCD1
    target persists `__timestamp` (fabricks/cdc/templates/ctes/base.sql.jinja:
    `{% elif cdc == "scd1" %} __timestamp,`), and current.sql.jinja's
    `__current` CTE reads that stored value back on the *next* iteration's merge
    to compare against a later reload's own timestamp (rectify.sql.jinja) —
    deciding whether a row missing from that reload should become
    non-current. Without it, a seeded table can never trigger that
    comparison (confirmed empirically: iter2's reload correctly drops iter1's
    superseded king rows against a *real* iter1-then-iter2 replay, but not
    against a table seeded from `load_expected(spark, "scd1", 1)` alone).

    Derived from the same `expected.scd2_iter{N}` root SCD1's own
    oracle view already reads, replicating its row_number-over-id-by-
    __valid_to-desc "latest known state per id" logic without OSS
    Spark's unsupported QUALIFY clause (see `_make_spark_compatible`).
    """
    scd2_df = spark.read.table(f"expected.scd2_iter{iter}")
    scd2_df.createOrReplaceTempView(f"__seed_scd2_src_{iter}")
    return spark.sql(f"""
        select * except (__valid_from, __valid_to, __rn), __valid_from as __timestamp
        from (
            select *, row_number() over (partition by id order by __valid_to desc) as __rn
            from __seed_scd2_src_{iter}
        )
        where __rn = 1
    """)


def compare_to_expected(spark: SparkSession, table: Table, cdc: str, iter: int, topic: str) -> None:
    df = table.dataframe

    expected_df = load_expected(spark, cdc, iter)
    if topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)


def seed_table(table: Table, expected_df: DataFrame, keys: list[str]) -> None:
    """Seed a not-yet-existing CDC target table directly from a prior iteration's known-
    correct `expected` output, instead of replaying every earlier iteration through
    the real merge code first. This is what makes each iteration's test independent
    and order-free: a bug in iteration N's merge can never corrupt what iteration N+1's
    test sees, since N+1 seeds from N's *expected* state, not N's actual one.

    `expected`'s own schema needs two columns added to match every column a
    real merge target ends up with — pass `load_expected_scd2_seed`/
    `load_expected_scd1_seed`'s output (which already add `__timestamp`),
    not plain `load_expected(...)`'s, when seeding either table; see those
    functions' own docstrings for why:

    - `__key`: confirmed via fabricks/cdc/base/merger.py's
      get_merge_context() (`on t.__key == s.__merge_key`) and
      fabricks/cdc/templates/macros/hash.sql.jinja's `add_key(fields)`
      macro: `md5(array_join(array(<fields>::string), '*', '-1'))`, computed
      here identically over `keys` (the real merge always calls this with
      `keys + ["__source"]` once `has_source` is true — pass the full field
      list, not just the business key).
    - `__operation`: `processor.py`'s `has_operation = add_operation or
      "__operation" in inputs` is true whenever the incoming batch has it
      (it always does here), so it IS a real persisted output column —
      confirmed via `outputs.append("__operation")`. Its *stored* value
      doesn't matter for correctness though: current.sql.jinja's `__current`
      CTE overwrites it unconditionally to the literal `'current'` (or
      `'delete'` if `has_no_data`) whenever a real target row is read back
      for the next iteration's merge, never reading what's actually stored — so
      seeding the literal `'current'` here matches exactly what a real
      target row would resolve to regardless.

    Neither is optional cosmetics: without both, a seeded table's column set
    genuinely differs from a real target's, which king_and_queen_built's own
    "does the incoming batch have a column the target doesn't" check
    correctly (if unhelpfully) detects as real schema drift — triggering an
    `update_schema()` call on every single iteration instead of only where the
    fixture data genuinely introduces one (iter2's `newField`).

    `__hash` needs no seeding: fabricks/cdc/templates/merges/scd2.sql.jinja
    never reads it back off the target (`t.__hash` appears nowhere in any
    merge template) — it's written fresh from the incoming source rows on
    every call, never compared against a stored value.

    Writes to `table.delta_path` + `table.register()` (not `saveAsTable`,
    which would let Spark pick its own default warehouse location) so the
    seeded table lives at exactly the path `Table.create()` itself would use
    — the same relative-vs-absolute path pitfall Task 9 already hit once
    (conf.fabricks.yml's storage paths) would otherwise silently reappear
    here as a `DELTA_TABLE_NOT_FOUND` on the very next `.update()` call.
    """
    key_cols = ", ".join(f"cast(`{k}` as string)" for k in keys)
    seeded = expected_df.selectExpr(
        "*", f"md5(array_join(array({key_cols}), '*', '-1')) as __key", "'current' as __operation"
    )
    seeded.write.format("delta").mode("overwrite").save(str(table.delta_path))
    table.register()
