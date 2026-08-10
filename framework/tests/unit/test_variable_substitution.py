import re
from pathlib import Path

import pytest
import yaml

from fabricks.models.runtime.models import RuntimeConf


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures/runtime"


def test_variable_substitution_substitutes_all_fields(fixtures_dir: Path) -> None:
    """Test inline variables substitution."""
    conf_file = fixtures_dir / "inline_variables.yml"
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    runtime = RuntimeConf.model_validate(conf_data)
    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.options.retention_days == 14
    assert runtime.options.secret_scope == "test_scope"
    assert runtime.path_options.storage == "abfss://fabricks@account.dfs.core.windows.net/fabricks"


def test_variable_substitution_loads_from_path_options_variables(fixtures_dir: Path, monkeypatch) -> None:
    """Test loading variables from path_options.variables file."""
    conf_file = fixtures_dir / "path_variables.yml"
    # Mock config.path_to_config to framework directory so relative paths work
    framework_dir = Path(__file__).parent.parent.parent
    monkeypatch.setattr("fabricks.models.runtime.models.config.path_to_config", str(framework_dir / "pyproject.toml"))
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    runtime = RuntimeConf.model_validate(conf_data)
    assert runtime.options.workers == 12
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.variables is not None


def test_variable_substitution_path_options_takes_precedence_over_inline(fixtures_dir: Path) -> None:
    """Test that path_options.variables takes precedence over inline variables."""
    conf_file = fixtures_dir / "inline_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    # Add path_options.variables pointing to prd file (absolute path)
    conf_data["path_options"]["variables"] = str(prd_variables_file)
    runtime = RuntimeConf.model_validate(conf_data)
    # Should use variables.prd.yml (32 workers), not inline variables (8 workers)
    assert runtime.options.workers == 32


def test_variable_substitution_raises_for_missing_variables_file(fixtures_dir: Path) -> None:
    """Test that missing variables file raises FileNotFoundError."""
    conf_file = fixtures_dir / "path_variables.yml"
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    # Point to non-existent file
    conf_data["path_options"]["variables"] = str(fixtures_dir / "variables.missing.yml")
    with pytest.raises(FileNotFoundError, match=re.escape("variables file")):
        RuntimeConf.model_validate(conf_data)


def test_variable_substitution_without_context_skips_substitution(fixtures_dir: Path) -> None:
    """Test that config without variables loads correctly."""
    conf_file = fixtures_dir / "no_variables.yml"
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    runtime = RuntimeConf.model_validate(conf_data)
    assert runtime.options.secret_scope == "test_scope"
    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.variables is None


def test_variable_substitution_fabricks_variable_env_overrides_path_options(fixtures_dir: Path, monkeypatch) -> None:
    """Test that FABRICKS_VARIABLE env var takes precedence over path_options.variables."""
    conf_file = fixtures_dir / "path_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"
    # Mock config.variable to simulate FABRICKS_VARIABLE env var pointing to prd
    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(prd_variables_file))
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    runtime = RuntimeConf.model_validate(conf_data)
    assert runtime.options.workers == 32
    assert runtime.options.catalog == "stg_prd_dwh"


def test_variable_substitution_fabricks_variable_env_overrides_inline(fixtures_dir: Path, monkeypatch) -> None:
    """Test that FABRICKS_VARIABLE env var takes precedence over inline variables."""
    conf_file = fixtures_dir / "inline_variables.yml"
    prd_variables_file = fixtures_dir / "variables.dev.yml"
    # Mock config.variable to simulate FABRICKS_VARIABLE env var
    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(prd_variables_file))
    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)
    runtime = RuntimeConf.model_validate(conf_data)
    assert runtime.options.workers == 12


def test_variable_substitution_with_dollar_escape(fixtures_dir: Path) -> None:
    """Test that $$ escape prevents variable substitution in paths."""
    conf_file = fixtures_dir / "escape_variables.yml"

    with open(conf_file, encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    # Verify regular variables were substituted
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.options.workers == 16
    assert runtime.options.secret_scope == "test_scope"

    # Main storage path: $storage_account substituted, $$Change escaped to $Change
    assert (
        runtime.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/$Change_Log_Data/fabricks"
    )

    # Parsers path: $$G_L escaped to $G_L
    assert runtime.path_options.parsers == "fabricks/parsers/$G_L_Parsers"

    # Verify bronze configurations exist
    assert runtime.bronze is not None
    assert len(runtime.bronze) == 3

    # Test first bronze: variable substituted, $$Change escaped to $Change
    bc_change_log = runtime.bronze[0]
    assert bc_change_log.name == "bc_change_log"
    assert (
        bc_change_log.path_options.storage
        == "abfss://raw@teststorageaccount.dfs.core.windows.net/bc/$Change Log Entry"
    )

    # Test second bronze: variable substituted, $$G_L escaped to $G_L
    bc_gl_entry = runtime.bronze[1]
    assert bc_gl_entry.name == "bc_gl_entry"
    assert bc_gl_entry.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/bc/$G_L Entry"

    # Test third bronze: variable substituted, multiple escapes ($$Item and $$Purchase)
    mixed_test = runtime.bronze[2]
    assert mixed_test.name == "mixed_escape_test"
    assert (
        mixed_test.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/$Item/$Purchase/data"
    )

    # Verify that variables dict still contains the collision names
    assert runtime.variables is not None
    for var_name in ["$Change", "$G_L", "$Item", "$Purchase"]:
        assert runtime.variables[var_name] == "SHOULD_NOT_BE_USED"
