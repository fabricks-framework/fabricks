import re
from pathlib import Path

import pytest
import yaml

from fabricks.models.runtime.models import RuntimeConf


def test_variable_substitution_substitutes_all_fields() -> None:
    """Test inline variables substitution."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_inline_variables.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.options.retention_days == 14
    assert runtime.options.secret_scope == "test_scope"
    assert runtime.path_options.storage == "abfss://fabricks@account.dfs.core.windows.net/fabricks"


def test_variable_substitution_loads_from_path_options_variables(monkeypatch) -> None:
    """Test loading variables from path_options.variables file."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_path_variables.yml"

    # Mock config.path_to_config to framework directory so relative paths work
    framework_dir = Path(__file__).parent.parent.parent
    monkeypatch.setattr("fabricks.models.runtime.models.config.path_to_config", str(framework_dir / "pyproject.toml"))

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert "$workers" in runtime.variables
    assert "$catalog" in runtime.variables

    assert runtime.options.workers == 12
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.variables is not None


def test_variable_substitution_path_options_takes_precedence_over_inline() -> None:
    """Test that path_options.variables takes precedence over inline variables."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_inline_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    # Add path_options.variables pointing to prd file (absolute path)
    conf_data["path_options"]["variables"] = str(prd_variables_file)

    runtime = RuntimeConf.model_validate(conf_data)

    # Should use variables.prd.yml (32 workers), not inline variables (8 workers)
    assert runtime.options.workers == 32
    

def test_variable_substitution_raises_for_missing_variables_file() -> None:
    """Test that missing variables file raises FileNotFoundError."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_path_variables.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    # Point to non-existent file
    conf_data["path_options"]["variables"] = "tests/unit/fixtures/variables.missing.yml"

    with pytest.raises(
        FileNotFoundError,
        match=re.escape("variables file"),
    ):
        RuntimeConf.model_validate(conf_data)


def test_variable_substitution_without_context_skips_substitution() -> None:
    """Test that config without variables loads correctly."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_no_variables.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.secret_scope == "test_scope"
    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.variables is None


def test_variable_substitution_fabricks_variable_env_overrides_path_options(monkeypatch) -> None:
    """Test that FABRICKS_VARIABLE env var takes precedence over path_options.variables."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_path_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"

    # Mock config.variable to simulate FABRICKS_VARIABLE env var pointing to prd
    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(prd_variables_file))

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 32
    assert runtime.options.catalog == "stg_prd_dwh"


def test_variable_substitution_fabricks_variable_env_overrides_inline(monkeypatch) -> None:
    """Test that FABRICKS_VARIABLE env var takes precedence over inline variables."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    conf_file = fixtures_dir / "conf_inline_variables.yml"
    prd_variables_file = fixtures_dir / "variables.dev.yml"

    # Mock config.variable to simulate FABRICKS_VARIABLE env var
    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(prd_variables_file))

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 12
