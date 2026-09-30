"""Static dependency extraction: notebooks (ast + sqlglot, fabricks.utils.notebook) and plain SQL
(fabricks.utils.sqlglot). Pure Python, no Spark. The wiring that decides when the static parser is used lives in
tests/unit/config/test_dependencies.py."""

import json
from pathlib import Path

import pytest

from fabricks.utils.notebook import get_notebook_dependencies
from fabricks.utils.sqlglot import get_tables

_ALLOWED = ["bronze", "silver", "gold"]
_FIXTURE_IPYNB = Path(__file__).parent / "fixtures" / "notebooks" / "dependency.ipynb"
_FIXTURE_SQL = Path(__file__).parent / "fixtures" / "sql" / "dependency.sql"


def _nb(*cell_sources: str) -> str:
    return json.dumps(
        {
            "cells": [{"cell_type": "code", "source": src} for src in cell_sources],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


# --- notebooks: each case is (cells, expected dependencies); order is not significant, duplicates are.

_RESOLVES = {
    "literal_spark_sql_call": (['spark.sql("select * from gold.dim_time")'], ["gold.dim_time"]),
    "variable_constant_spark_sql": (
        ['query = "select * from silver.customer_scd2__current"\nspark.sql(query)'],
        ["silver.customer_scd2__current"],
    ),
    "spark_table_literal": (['spark.table("gold.fact_sales")'], ["gold.fact_sales"]),
    "spark_read_table_and_format_chain": (
        ['spark.read.table("silver.orders")\nspark.read.format("delta").table("silver.orders_v2")'],
        ["silver.orders", "silver.orders_v2"],
    ),
    "spark_readstream_table": (['spark.readStream.format("delta").table("bronze.events")'], ["bronze.events"]),
    "delta_table_for_name": (
        ['from delta.tables import DeltaTable\nDeltaTable.forName(spark, "gold.dim_customer")'],
        ["gold.dim_customer"],
    ),
    "delta_table_is_delta_table_is_not_a_dependency": (
        [
            "from delta import DeltaTable\n"
            "if DeltaTable.isDeltaTable(spark, table_uri):\n"
            "    pass\n"
            'spark.table("gold.fact_sales")'
        ],
        ["gold.fact_sales"],
    ),
    "percent_sql_magic_cell_parsed_as_sql": (
        ["%sql --- comment on the magic line\nselect * from gold.dim_region"],
        ["gold.dim_region"],
    ),
    "fully_commented_cell_excluded": (
        ['# df = spark.sql("select * from gold.should_not_appear")', 'spark.sql("select * from gold.real_table")'],
        ["gold.real_table"],
    ),
    "mixed_commented_and_active_line": (
        ['# old = spark.sql("select * from gold.old_table")\nnew = spark.sql("select * from gold.new_table")'],
        ["gold.new_table"],
    ),
    "pip_and_sh_magic_cells_skipped": (
        ["%pip install some-package", "%sh echo hello", 'spark.sql("select * from gold.after_magics")'],
        ["gold.after_magics"],
    ),
    "multi_statement_script_sql": (
        [
            "spark.sql('''\n"
            "insert into gold.summary select * from silver.staging;\n"
            "merge into gold.summary_v2 using silver.staging_v2 s on true when matched then update set *;\n"
            "''')"
        ],
        ["gold.summary", "silver.staging", "gold.summary_v2", "silver.staging_v2"],
    ),
    "identifier_templating_with_literal_kwarg_resolves": (
        ['spark.sql("select * from identifier({tbl})", tbl="gold.dim_time")'],
        ["gold.dim_time"],
    ),
    "allowed_databases_filtering": (
        ['spark.sql("select * from gold.dim_time cross join external_system.raw_feed")'],
        ["gold.dim_time"],
    ),
}

# Any single unresolvable construct (or no dependency at all) makes the whole result untrustworthy, so the parser
# returns None and the caller falls back to executing the notebook.
_FALLS_BACK = {
    "cte_excluded": ['spark.sql("with cte as (select 1 as x) select * from cte")'],
    "unresolvable_fstring": [
        'table_name = "gold.dynamic_target"\n'
        'spark.sql(f"select * from {table_name}")\n'
        'spark.sql("select * from gold.also_present")'
    ],
    "unresolvable_variable_table_arg": ['df = spark.table(table)\nspark.sql("select * from gold.also_present")'],
    "identifier_with_non_literal_kwarg": [
        "def helper(x):\n"
        "    return x\n"
        'spark.sql("select * from identifier({tbl})", tbl=helper("gold.dim_time"))\n'
        'spark.sql("select * from gold.also_present")'
    ],
    "run_magic": ["%run ./other_notebook", 'spark.sql("select * from gold.also_present")'],
    "zero_dependencies": ["df = some_frame.filter(some_frame.x > 1).join(other_frame, 'id')"],
}


@pytest.mark.parametrize(("cells", "expected"), _RESOLVES.values(), ids=_RESOLVES.keys())
def test_notebook_dependencies_are_extracted(cells, expected):
    deps = get_notebook_dependencies(_nb(*cells), allowed_databases=_ALLOWED)

    assert deps is not None
    assert sorted(deps) == sorted(expected)


@pytest.mark.parametrize("cells", _FALLS_BACK.values(), ids=_FALLS_BACK.keys())
def test_notebook_dependencies_trigger_fallback(cells):
    assert get_notebook_dependencies(_nb(*cells), allowed_databases=_ALLOWED) is None


def test_fixture_notebook_with_every_form_triggers_fallback():
    # dependency.ipynb exercises every supported read form (spark.sql, spark.table,
    # spark.read/readStream.table, DeltaTable.forName, %sql, identifier({param}), comments,
    # non-sql magics) alongside every known out-of-scope form (f-string SQL, a helper-returned
    # table name). Because any single unresolvable construct makes the whole notebook's result
    # untrustworthy (see get_notebook_dependencies' docstring), the fixture as a whole -- despite
    # containing 14+ perfectly resolvable dependencies -- correctly falls back rather than
    # silently under-reporting the two it can't resolve.
    nb = _FIXTURE_IPYNB.read_text()
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


# --- plain SQL


def test_get_tables_extracts_fixture_dependencies():
    sql = _FIXTURE_SQL.read_text()

    tables = get_tables(sql, allowed_databases=["gold", "transf", "silver"])

    assert set(tables) == {"gold.dim_time", "transf.fact_memory", "silver.king_and_queen_scd1__current"}


def test_get_tables_excludes_ctes():
    sql = "with cte as (select 1 as x) select * from cte"

    assert get_tables(sql) == []


def test_get_tables_filters_by_allowed_databases():
    sql = "select * from gold.dim_time cross join not_a_fabricks_step.some_table"

    assert get_tables(sql, allowed_databases=["gold", "silver", "transf"]) == ["gold.dim_time"]
