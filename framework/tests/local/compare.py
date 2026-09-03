"""Local (spark-injected) equivalents of tests/databricks/compare.py's
SPARK-global-coupled comparison helpers. assert_dfs_equal itself doesn't
touch the SPARK global, so it's reused unchanged.

See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #7.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from fabricks.metastore.table import Table
from tests.databricks.compare import assert_dfs_equal

_EXPECTED_ROOT = Path(__file__).resolve().parents[1] / "expected"  # tests/expected/ — shared sibling, see Task 3

# OSS Apache Spark (this local test container) has no QUALIFY clause support
# -- it's a Databricks SQL extension. tests/expected/silver/scd1/job*.sql (the
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
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)

# newField: added via a per-file StructField, not folded into the base schema
# above, and NOT unconditionally on every job's NDJSON the way
# tests/databricks/utils.py's _EXPECTED_SCD2_SCHEMA does it (commit
# 8a7cb451). That's correct for the Databricks-cluster suite, where
# king_and_queen's real Silver job config declares a fixed bronze schema
# up front (so bronze.king already carries a NULL newField column from its
# very first landing batch, before job2 ever supplies a real value) -- but
# this plan has no job config/parsers layer at all (see king_and_queen_built
# in conftest.py: raw spark.read.json() per job's own NDJSON file, one column
# set per file, autoMerge only ever *adding* columns once a later job's data
# introduces them). So a jobs=[1]-only silver table genuinely has no
# newField column at all (verified empirically: UNRESOLVED_COLUMN against
# this container's real Spark session when the expected side forced newField
# into job1's comparison) -- matching job1's own raw fixture, which has no
# "newField" key in any row (verified: tests/local/fixtures/job1/bronze_*
# .jsonl and the original tests/data/job1/king/**/*.jsonl landing fixture
# both lack the key entirely; job2 onward always carry it). Detecting the
# key's presence per-file (rather than hardcoding "job1 is the exception")
# keeps this correct if a future job's fixture composition changes.
_NEW_FIELD = StructField("newField", StringType(), True)


def _expected_scd2_schema(rows: list[dict]) -> StructType:
    if any("newField" in row for row in rows):
        return StructType(_EXPECTED_SCD2_BASE_SCHEMA.fields + [_NEW_FIELD])
    return _EXPECTED_SCD2_BASE_SCHEMA


def create_expected_views(spark: SparkSession, step: str, cdc: str) -> None:
    views_dir = _EXPECTED_ROOT / step / cdc

    if step == "silver" and cdc == "scd2":
        # Only job1's file is hand-authored NDJSON data — job2.sql onward are
        # still real SQL, each unioning its own new VALUES rows with `select
        # ... from expected.silver_scd2_job{N-1} where not __is_current`
        # (verified: job3.sql references job2, job2.sql references job1 — a
        # genuine sequential chain). Create job1's NDJSON root, then fall
        # through to the SQL loop below for job2 onward in ascending order —
        # an early `return` here would silently skip
        # expected.silver_scd2_job{2..9} entirely, since Task 9's
        # king_and_queen_built only calls create_expected_views for
        # ("silver", "scd2")/("silver", "scd1"), not per job. See Task 3's
        # mirrored fix in tests/databricks/utils.py's create_expected_views.
        for ndjson_file in sorted(views_dir.glob("*.jsonl")):
            # str(int(...)): strip the filename's leading zero (job01.jsonl ->
            # "01") so it matches the un-padded table name (silver_scd2_job1)
            # the scd1/job*.sql oracle views reference. Same fix as
            # tests/databricks/utils.py's create_expected_views (commit
            # d4f2498e) -- without it, "silver_scd2_job01" is created but
            # "silver_scd2_job1" (what job1.sql selects from) is never found.
            job_num = str(int(re.search(r"\d+", ndjson_file.stem).group()))
            rows = [json.loads(line) for line in ndjson_file.read_text().splitlines()]
            for row in rows:
                # __valid_from/__valid_to are plain "YYYY-MM-DD HH:MM:SS"
                # strings in the NDJSON; TimestampType.toInternal() requires
                # an actual datetime (it calls .utctimetuple()/.timetuple()),
                # so a raw string fails schema conversion. tzinfo=utc pins
                # these as instants rather than leaving them naive (which
                # would otherwise be localized using the JVM/driver's local
                # timezone) — mirrors tests/databricks/utils.py's identical
                # parsing for the same NDJSON files.
                row["__valid_from"] = datetime.strptime(row["__valid_from"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
                row["__valid_to"] = datetime.strptime(row["__valid_to"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
            df = spark.createDataFrame(rows, schema=_expected_scd2_schema(rows))
            df.write.mode("overwrite").saveAsTable(f"expected.silver_scd2_job{job_num}")

    for sql_file in sorted(views_dir.glob("*.sql")):
        spark.sql(_make_spark_compatible(sql_file.read_text()))


def compare_silver_to_expected(spark: SparkSession, table: Table, cdc: str, iter: int, topic: str) -> None:
    df = table.dataframe

    expected_df = spark.read.table(f"expected.silver_{cdc}_job{iter}")
    if topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)
