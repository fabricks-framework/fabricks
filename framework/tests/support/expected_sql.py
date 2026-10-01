"""Pure helpers for the Apache tier's expected-state oracle: the QUALIFY rewrite for OSS Spark and the expected
SCD2 schema. No Spark session needed, so the plain tier tests them (tests/unit/plain/test_compare.py)."""

import re

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

# OSS Spark has no QUALIFY (Databricks extension); sqlglot's rewrite leaks a helper column into `select *`.
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
        # LongType: spark.read.json() infers bigint, and seeding from an int schema makes update_schema() see a diff.
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)

# Added per file, not in the base schema: iter1's raw fixtures have no newField key, so its target has no such column.
# BooleanType: every raw fixture carries a JSON boolean.
_NEW_FIELD = StructField("newField", BooleanType(), True)


def expected_scd2_schema(rows: list[dict]) -> StructType:
    if any("newField" in row for row in rows):
        return StructType([*_EXPECTED_SCD2_BASE_SCHEMA.fields, _NEW_FIELD])
    return _EXPECTED_SCD2_BASE_SCHEMA
