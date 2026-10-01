"""Pure helpers for the Apache tier's expected-state oracle: the QUALIFY rewrite for OSS Spark and the expected
SCD2 schema. No Spark session needed, so the plain tier tests them (tests/unit/plain/test_compare.py)."""

import re

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

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


def make_spark_compatible(sql: str) -> str:
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


def expected_scd2_schema(rows: list[dict]) -> StructType:
    if any("newField" in row for row in rows):
        return StructType([*_EXPECTED_SCD2_BASE_SCHEMA.fields, _NEW_FIELD])
    return _EXPECTED_SCD2_BASE_SCHEMA
