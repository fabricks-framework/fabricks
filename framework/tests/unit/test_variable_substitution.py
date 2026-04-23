from pathlib import Path

import pytest
import yaml

from fabricks.models.runtime.models import RuntimeConf


def test_variable_substitution_substitutes_all_fields() -> None:

    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_inline_variables.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime_conf = RuntimeConf.model_validate(conf_data)

    assert runtime_conf.options.workers == 8
    assert runtime_conf.options.catalog == "stg_dev_dwh"
    assert runtime_conf.options.retention_days == 14
    assert runtime_conf.options.secret_scope == "test_scope"
    assert runtime_conf.path_options.storage == "abfss://fabricks@account.dfs.core.windows.net/fabricks"


def test_variable_substitution_loads_from_path_options_variables(tmp_path: Path, runtime_config_factory) -> None:

    variables_file = tmp_path / "variables.dev.yml"
    variables_file.write_text(yaml.safe_dump({"$workers": "2", "$catalog": "stg_dev_dwh"}), encoding="utf-8")

    conf_data = runtime_config_factory(
        options={"workers": "$workers", "catalog": "$catalog"},
        path_options={"variables": str(variables_file)},  # Use absolute path
    )

    runtime_conf = RuntimeConf.model_validate(conf_data)

    assert runtime_conf.options.workers == 2
    assert runtime_conf.options.catalog == "stg_dev_dwh"
    assert runtime_conf.variables is not None
    assert "$workers" in runtime_conf.variables
    assert "$catalog" in runtime_conf.variables


def test_variable_substitution_path_options_takes_precedence_over_inline(
    tmp_path: Path,
    runtime_config_factory,
) -> None:

    variables_file = tmp_path / "variables.prd.yml"
    variables_file.write_text(yaml.safe_dump({"$workers": "16"}), encoding="utf-8")

    conf_data = runtime_config_factory(
        options={"workers": "$workers"},
        path_options={"variables": str(variables_file)},  # Use absolute path
        variables={"$workers": "4"},  # Inline variables should be ignored
    )

    runtime_conf = RuntimeConf.model_validate(conf_data)

    assert runtime_conf.options.workers == 16


def test_variable_substitution_supports_escaped_variable_keys(runtime_config_factory) -> None:

    conf_data = runtime_config_factory(
        options={"workers": "$workers"},
        variables={"\\$workers": "6"},
    )

    runtime_conf = RuntimeConf.model_validate(conf_data)

    assert runtime_conf.options.workers == 6


def test_variable_substitution_raises_for_missing_variables_file(tmp_path: Path, runtime_config_factory) -> None:
    import re

    missing_variables_path = tmp_path / "variables.dev.yml"
    conf_data = runtime_config_factory(
        options={"workers": "$workers"},
        path_options={"variables": str(missing_variables_path)},
    )

    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"variables file '{missing_variables_path}'"),
    ):
        RuntimeConf.model_validate(conf_data)


def test_variable_substitution_without_context_skips_substitution(runtime_config_factory) -> None:
    """Test that variables are not substituted when context is not provided."""

    # Use actual values (not variables) since validation would fail with unsubstituted variables
    conf_data = runtime_config_factory(
        options={"secret_scope": "test_scope", "workers": 8, "catalog": "stg_dev_dwh"},
        variables={"$unused": "value"},
    )

    # Without context, the model validates normally (no substitution needed)
    runtime_conf = RuntimeConf.model_validate(conf_data)

    assert runtime_conf.options.secret_scope == "test_scope"
    assert runtime_conf.options.workers == 8
    assert runtime_conf.variables == {"$unused": "value"}
