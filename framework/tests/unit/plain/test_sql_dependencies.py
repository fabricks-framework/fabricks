from pathlib import Path

from fabricks.utils.sqlglot import get_tables

_FIXTURE = Path(__file__).parent / "fixtures" / "sql" / "dependency.sql"


def test_get_tables_extracts_fixture_dependencies():
    sql = _FIXTURE.read_text()

    tables = get_tables(sql, allowed_databases=["gold", "transf", "silver"])

    assert set(tables) == {"gold.dim_time", "transf.fact_memory", "silver.king_and_queen_scd1__current"}


def test_get_tables_excludes_ctes():
    sql = "with cte as (select 1 as x) select * from cte"

    assert get_tables(sql) == []


def test_get_tables_filters_by_allowed_databases():
    sql = "select * from gold.dim_time cross join not_a_fabricks_step.some_table"

    assert get_tables(sql, allowed_databases=["gold", "silver", "transf"]) == ["gold.dim_time"]
