"""fabricks.runchecks: offline validator for a runtime repo's job YAML
against its own SQL/notebook files (no Spark/Databricks connection needed)."""

from pathlib import Path

import pytest

from fabricks.runchecks import _find_conf_yaml, _passthrough_steps_from_conf, check_config, main


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_find_conf_yaml_via_pyproject_toml(tmp_path: Path) -> None:
    _write(tmp_path / "pyproject.toml", '[tool.fabricks]\nconfig = "fabricks/conf.yml"\n')
    runtime = tmp_path / "sub" / "dir"
    runtime.mkdir(parents=True)

    assert _find_conf_yaml(runtime) == tmp_path / "fabricks/conf.yml"


def test_find_conf_yaml_via_fabricksconfig_json(tmp_path: Path) -> None:
    _write(tmp_path / "fabricksconfig.json", '{"config": "fabricks/conf.uc.yml"}')

    assert _find_conf_yaml(tmp_path) == tmp_path / "fabricks/conf.uc.yml"


def test_find_conf_yaml_returns_none_when_undiscoverable(tmp_path: Path) -> None:
    assert _find_conf_yaml(tmp_path) is None


def test_main_warns_when_no_conf_discoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select 1 as x")

    monkeypatch.setattr("sys.argv", ["runchecks", str(tmp_path)])
    main()

    assert "no fabricksconfig.json/pyproject.toml found" in capsys.readouterr().out


def test_passthrough_steps_from_conf_collects_bronze_and_silver_only(tmp_path: Path) -> None:
    conf = tmp_path / "conf.yml"
    _write(
        conf,
        """
        - conf:
            bronze:
              - name: bronze
              - name: staging
            silver:
              - name: silver
            gold:
              - name: gold
        """,
    )

    assert _passthrough_steps_from_conf(conf) == frozenset({"bronze", "staging", "silver"})


def test_passthrough_steps_from_conf_returns_empty_on_bad_shape(tmp_path: Path) -> None:
    conf = tmp_path / "conf.yml"
    _write(conf, "not: a fabricks conf\n")

    assert _passthrough_steps_from_conf(conf) == frozenset()


def test_verbose_prints_per_step_stats(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(
        tmp_path / "gold" / "reference" / "_config.reference.yml",
        """
        - job:
            step: gold
            topic: reference
            item: currency
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select 1 as x")
    _write(tmp_path / "gold" / "reference" / "currency.sql", "select 1 as x")

    assert check_config(tmp_path, verbose=True) is False
    out = capsys.readouterr().out
    assert "gold" in out
    assert "2" in out  # 2 jobs, 2 topics, 2 sql files


def test_sql_reference_to_nonexistent_fabricks_managed_table_is_an_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(
        tmp_path / "silver" / "sales" / "_config.sales.yml",
        """
        - job:
            step: silver
            topic: sales
            item: orders_other
            options:
              mode: register
        """,
    )
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select * from silver.sales_orders")

    assert check_config(tmp_path) is True
    assert "silver.sales_orders" in capsys.readouterr().out


def test_sql_reference_to_unmanaged_database_is_a_warning_not_an_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select * from raw_source.external_orders")

    assert check_config(tmp_path) is False
    assert "raw_source.external_orders" in capsys.readouterr().out


def test_multi_statement_sql_is_an_error_without_script_true(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(
        tmp_path / "gold" / "sales" / "summary.sql",
        "create table gold.sales_summary (x int); insert into gold.sales_summary values (1)",
    )

    assert check_config(tmp_path) is True


def test_multi_statement_sql_is_fine_with_script_true(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
              script: true
        """,
    )
    _write(
        tmp_path / "gold" / "sales" / "summary.sql",
        "create table gold.sales_summary (x int); insert into gold.sales_summary values (1)",
    )

    assert check_config(tmp_path) is False


def test_clean_runtime_has_no_errors(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select 1 as x")

    assert check_config(tmp_path) is False


def test_missing_sql_file_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )

    assert check_config(tmp_path) is True


def test_passthrough_step_does_not_need_a_sql_file(tmp_path: Path) -> None:
    _write(
        tmp_path / "raw" / "_config.sales.yml",
        """
        - job:
            step: raw
            topic: sales
            item: orders
            options:
              mode: update
        """,
    )

    assert check_config(tmp_path) is True
    assert check_config(tmp_path, passthrough_steps=frozenset({"raw"})) is False


def test_dangling_parent_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
              parents: [silver.sales_orders]
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select 1 as x")

    assert check_config(tmp_path) is True


def test_unparseable_sql_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select this is not sql (((")

    assert check_config(tmp_path) is True


def test_duplicate_table_name_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "sales" / "summary.sql", "select 1 as x")

    assert check_config(tmp_path) is True


@pytest.mark.parametrize("mode", ["invoke", "register"])
def test_invoke_and_register_modes_never_need_a_sql_file(tmp_path: Path, mode: str) -> None:
    _write(
        tmp_path / "bronze" / "sales" / "_config.sales.yml",
        f"""
        - job:
            step: bronze
            topic: sales
            item: raw
            options:
              mode: {mode}
        """,
    )

    assert check_config(tmp_path) is False


def test_unset_parents_does_not_need_to_match_sql(tmp_path: Path) -> None:
    """parents: is an override, not a contract the SQL must satisfy -- a job that
    leaves it unset gets its dependency deducted from the SQL instead, and a job
    that sets it is free to narrow it (e.g. gating on a trigger job) without that
    being an error."""
    _write(
        tmp_path / "silver" / "sales" / "_config.sales.yml",
        """
        - job:
            step: silver
            topic: sales
            item: orders
            options:
              mode: register
        """,
    )
    _write(
        tmp_path / "gold" / "reference" / "_config.reference.yml",
        """
        - job:
            step: gold
            topic: reference
            item: currency
            options:
              mode: register
        """,
    )
    _write(
        tmp_path / "gold" / "sales" / "_config.sales.yml",
        """
        - job:
            step: gold
            topic: sales
            item: summary
            options:
              mode: complete
              parents: [silver.sales_orders]
        """,
    )
    _write(
        tmp_path / "gold" / "sales" / "summary.sql",
        "select * from silver.sales_orders join gold.reference_currency on true",
    )

    assert check_config(tmp_path) is False


def test_circular_dependency_via_declared_parents_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "a" / "_config.a.yml",
        """
        - job:
            step: gold
            topic: a
            item: one
            options:
              mode: complete
              parents: [gold.b_two]
        """,
    )
    _write(
        tmp_path / "gold" / "b" / "_config.b.yml",
        """
        - job:
            step: gold
            topic: b
            item: two
            options:
              mode: complete
              parents: [gold.a_one]
        """,
    )
    _write(tmp_path / "gold" / "a" / "one.sql", "select 1 as x")
    _write(tmp_path / "gold" / "b" / "two.sql", "select 1 as x")

    assert check_config(tmp_path) is True


def test_circular_dependency_via_sql_is_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "a" / "_config.a.yml",
        """
        - job:
            step: gold
            topic: a
            item: one
            options:
              mode: complete
        """,
    )
    _write(
        tmp_path / "gold" / "b" / "_config.b.yml",
        """
        - job:
            step: gold
            topic: b
            item: two
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "a" / "one.sql", "select * from gold.b_two")
    _write(tmp_path / "gold" / "b" / "two.sql", "select * from gold.a_one")

    assert check_config(tmp_path) is True


def test_no_cycle_is_not_an_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "gold" / "a" / "_config.a.yml",
        """
        - job:
            step: gold
            topic: a
            item: one
            options:
              mode: complete
        """,
    )
    _write(
        tmp_path / "gold" / "b" / "_config.b.yml",
        """
        - job:
            step: gold
            topic: b
            item: two
            options:
              mode: complete
        """,
    )
    _write(tmp_path / "gold" / "a" / "one.sql", "select 1 as x")
    _write(tmp_path / "gold" / "b" / "two.sql", "select * from gold.a_one")

    assert check_config(tmp_path) is False
