from pathlib import Path
import re

import pytest
import yaml

from fabricks.models.runtime.models import RuntimeConf


@pytest.fixture(autouse=True)
def _pin_variable_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """`config` reads FABRICKS_VARIABLE and the config path at import; a developer's environment must not leak in."""
    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", None)
    monkeypatch.setattr(
        "fabricks.models.runtime.models.config.path_to_config", str(Path(__file__).parents[3] / "pyproject.toml")
    )


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures/runtime"


def test_variable_substitution_substitutes_all_fields(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "inline_variables.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.options.retention_days == 14
    assert runtime.options.secret_scope == "test_scope"
    assert runtime.path_options.storage == "abfss://fabricks@account.dfs.core.windows.net/fabricks"


def test_variable_substitution_loads_from_path_options_variables(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "path_variables.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 12
    assert runtime.options.catalog == "stg_dev_dwh"
    with (fixtures_dir / "variables.dev.yml").open(encoding="utf-8") as f:
        assert runtime.variables == yaml.safe_load(f)


def test_variable_substitution_path_options_takes_precedence_over_inline(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "inline_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    conf_data["path_options"]["variables"] = str(prd_variables_file)

    runtime = RuntimeConf.model_validate(conf_data)

    # Should use variables.prd.yml (32 workers), not inline variables (8 workers)
    assert runtime.options.workers == 32


def test_variable_substitution_raises_for_missing_variables_file(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "path_variables.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    conf_data["path_options"]["variables"] = str(fixtures_dir / "variables.missing.yml")

    with pytest.raises(FileNotFoundError, match=re.escape("variables file")):
        RuntimeConf.model_validate(conf_data)


def test_variable_substitution_without_context_skips_substitution(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "no_variables.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.secret_scope == "test_scope"
    assert runtime.options.workers == 8
    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.variables is None


def test_variable_substitution_fabricks_variable_env_overrides_path_options(fixtures_dir: Path, monkeypatch) -> None:
    conf_file = fixtures_dir / "path_variables.yml"
    prd_variables_file = fixtures_dir / "variables.prd.yml"

    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(prd_variables_file))

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 32
    assert runtime.options.catalog == "stg_prd_dwh"


def test_variable_substitution_fabricks_variable_env_overrides_inline(fixtures_dir: Path, monkeypatch) -> None:
    conf_file = fixtures_dir / "inline_variables.yml"
    dev_variables_file = fixtures_dir / "variables.dev.yml"

    monkeypatch.setattr("fabricks.models.runtime.models.config.variable", str(dev_variables_file))

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.workers == 12


def test_variable_substitution_with_dollar_escape(fixtures_dir: Path) -> None:
    conf_file = fixtures_dir / "escape_variables.yml"

    with conf_file.open(encoding="utf-8") as f:
        conf_data = yaml.safe_load(f)

    runtime = RuntimeConf.model_validate(conf_data)

    assert runtime.options.catalog == "stg_dev_dwh"
    assert runtime.options.workers == 16
    assert runtime.options.secret_scope == "test_scope"

    assert (
        runtime.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/$Change_Log_Data/fabricks"
    )

    assert runtime.path_options.parsers == "fabricks/parsers/$G_L_Parsers"

    assert runtime.bronze is not None
    assert len(runtime.bronze) == 3

    bc_change_log = runtime.bronze[0]
    assert bc_change_log.name == "bc_change_log"
    assert (
        bc_change_log.path_options.storage
        == "abfss://raw@teststorageaccount.dfs.core.windows.net/bc/$Change Log Entry"
    )

    bc_gl_entry = runtime.bronze[1]
    assert bc_gl_entry.name == "bc_gl_entry"
    assert bc_gl_entry.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/bc/$G_L Entry"

    mixed_test = runtime.bronze[2]
    assert mixed_test.name == "mixed_escape_test"
    assert (
        mixed_test.path_options.storage == "abfss://raw@teststorageaccount.dfs.core.windows.net/$Item/$Purchase/data"
    )

    assert runtime.variables is not None
    for var_name in ["$Change", "$G_L", "$Item", "$Purchase"]:
        assert runtime.variables[var_name] == "SHOULD_NOT_BE_USED"
