"""Reproductions for suspected bugs found in a test review.

Each test encodes the correct behavior; `xfail(strict=True)` marks the ones that currently fail so a
fix turns them into an XPASS failure that forces the marker to be removed.
"""

from pyspark.sql.types import StringType
import pytest
import sqlglot

from tests.unit.config.test_ddl_option_mapping import _fake_df, _generated_sql, _make_table
from tests.unit.config.test_dependencies import _gold_job


@pytest.mark.xfail(strict=True, reason="gold.py get_dependencies lowercases wait_for but not parents (#233)")
def test_gold_wait_for_covered_by_mixed_case_parent_is_dropped():
    job = _gold_job(parents=["Gold.Fact_Other"], wait_for=["gold.fact_other"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "gold.fact_other")], (
        "a wait_for entry that names an existing parent must not add a second dependency"
    )


@pytest.mark.xfail(strict=True, reason="table.py _get_ddl_columns does not escape quotes in comments (#234)")
def test_column_comment_with_apostrophe_produces_parseable_ddl():
    table, mock_spark, df = _make_table(_fake_df(dummy=StringType()))

    table._create(df=df, comments={"dummy": "it's a dummy"})

    sql = _generated_sql(mock_spark)
    sqlglot.parse_one(sql, read="spark")


def test_column_comment_without_apostrophe_parses_control():
    table, mock_spark, df = _make_table(_fake_df(dummy=StringType()))

    table._create(df=df, comments={"dummy": "a dummy"})

    sqlglot.parse_one(_generated_sql(mock_spark), read="spark")
