import json
from pathlib import Path

from fabricks.utils.notebook import get_notebook_dependencies

_ALLOWED = ["bronze", "silver", "gold"]
_FIXTURE_IPYNB = Path(__file__).parent / "fixtures" / "notebooks" / "dependency.ipynb"


def _nb(*cell_sources: str) -> str:
    return json.dumps(
        {
            "cells": [{"cell_type": "code", "source": src} for src in cell_sources],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


def test_literal_spark_sql_call():
    nb = _nb('spark.sql("select * from gold.dim_time")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.dim_time"]


def test_variable_constant_spark_sql():
    nb = _nb('query = "select * from silver.customer_scd2__current"\nspark.sql(query)')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["silver.customer_scd2__current"]


def test_spark_table_literal():
    nb = _nb('spark.table("gold.fact_sales")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.fact_sales"]


def test_spark_read_table_and_format_chain():
    nb = _nb('spark.read.table("silver.orders")\nspark.read.format("delta").table("silver.orders_v2")')
    assert set(get_notebook_dependencies(nb, allowed_databases=_ALLOWED)) == {"silver.orders", "silver.orders_v2"}


def test_spark_readstream_table():
    nb = _nb('spark.readStream.format("delta").table("bronze.events")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["bronze.events"]


def test_delta_table_for_name():
    nb = _nb('from delta.tables import DeltaTable\nDeltaTable.forName(spark, "gold.dim_customer")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.dim_customer"]


def test_delta_table_is_delta_table_not_a_dependency():
    nb = _nb(
        "from delta import DeltaTable\n"
        "if DeltaTable.isDeltaTable(spark, table_uri):\n"
        "    pass\n"
        'spark.table("gold.fact_sales")'
    )
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.fact_sales"]


def test_percent_sql_magic_cell_parsed_as_sql():
    nb = _nb("%sql --- comment on the magic line\nselect * from gold.dim_region")
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.dim_region"]


def test_fully_commented_cell_excluded():
    nb = _nb('# df = spark.sql("select * from gold.should_not_appear")', 'spark.sql("select * from gold.real_table")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.real_table"]


def test_mixed_commented_and_active_line():
    nb = _nb('# old = spark.sql("select * from gold.old_table")\nnew = spark.sql("select * from gold.new_table")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.new_table"]


def test_pip_and_sh_magic_cells_skipped():
    nb = _nb("%pip install some-package", "%sh echo hello", 'spark.sql("select * from gold.after_magics")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.after_magics"]


def test_multi_statement_script_sql():
    nb = _nb(
        "spark.sql('''\n"
        "insert into gold.summary select * from silver.staging;\n"
        "merge into gold.summary_v2 using silver.staging_v2 s on true when matched then update set *;\n"
        "''')"
    )
    deps = get_notebook_dependencies(nb, allowed_databases=_ALLOWED)
    assert set(deps) == {"gold.summary", "silver.staging", "gold.summary_v2", "silver.staging_v2"}


def test_identifier_templating_with_literal_kwarg_resolves():
    nb = _nb('spark.sql("select * from identifier({tbl})", tbl="gold.dim_time")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.dim_time"]


def test_cte_excluded():
    nb = _nb('spark.sql("with cte as (select 1 as x) select * from cte")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


def test_allowed_databases_filtering():
    nb = _nb('spark.sql("select * from gold.dim_time cross join external_system.raw_feed")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) == ["gold.dim_time"]


def test_unresolvable_fstring_triggers_fallback():
    nb = _nb(
        'table_name = "gold.dynamic_target"\n'
        'spark.sql(f"select * from {table_name}")\n'
        'spark.sql("select * from gold.also_present")'
    )
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


def test_unresolvable_variable_table_arg_triggers_fallback():
    nb = _nb('df = spark.table(table)\nspark.sql("select * from gold.also_present")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


def test_identifier_with_non_literal_kwarg_triggers_fallback():
    nb = _nb(
        "def helper(x):\n"
        "    return x\n"
        'spark.sql("select * from identifier({tbl})", tbl=helper("gold.dim_time"))\n'
        'spark.sql("select * from gold.also_present")'
    )
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


def test_run_magic_triggers_fallback():
    nb = _nb("%run ./other_notebook", 'spark.sql("select * from gold.also_present")')
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


def test_zero_dependencies_triggers_fallback():
    nb = _nb("df = some_frame.filter(some_frame.x > 1).join(other_frame, 'id')")
    assert get_notebook_dependencies(nb, allowed_databases=_ALLOWED) is None


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
