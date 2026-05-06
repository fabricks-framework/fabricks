"""Unit tests for read_yaml utility function."""

from pathlib import Path

import pytest

from fabricks.utils.path import GitPath
from fabricks.utils.read.read_yaml import read_yaml


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return (Path(__file__).parent / "fixtures/jobs").absolute()


@pytest.fixture
def test_variables() -> dict[str, str]:
    """Return test variables for substitution."""
    return {
        "$storage_account": "testaccount.dfs.core.windows.net",
        "$container": "testcontainer",
        "$catalog": "test_catalog",
        "$workers": "16",
        "$database": "test_db",
        "$table": "test_table",
        "$environment": "dev",
    }


def test_read_yaml_without_variables(fixtures_dir: Path) -> None:
    """Test reading YAML files without variable substitution."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job"))

    assert len(results) == 2

    assert results[0]["step"] == "bronze"
    assert results[0]["topic"] == "customers"

    assert "$storage_account" in results[0]["options"]["uri"]
    assert "$catalog" in results[0]["options"]["catalog"]


def test_read_yaml_with_variables(fixtures_dir: Path, test_variables: dict[str, str]) -> None:
    """Test reading YAML files with variable substitution."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job", variables=test_variables))

    assert len(results) == 2
    first = results[0]
    second = results[1]

    # First job - customers
    assert first["step"] == "bronze"
    assert first["topic"] == "customers"
    assert first["options"]["uri"] == "abfss://fabricks@testaccount.dfs.core.windows.net/testcontainer/raw/customers"
    assert first["options"]["catalog"] == "test_catalog"

    # Second job - orders
    assert second["topic"] == "orders"
    assert second["options"]["uri"] == "abfss://fabricks@testaccount.dfs.core.windows.net/testcontainer/raw/orders"
    assert second["options"]["workers"] == "16"


def test_read_yaml_with_partial_variables(fixtures_dir: Path) -> None:
    """Test reading YAML with only some variables provided (strict=False)."""
    path = GitPath(str(fixtures_dir / "variables.yml"))
    partial_vars = {
        "$storage_account": "testaccount.dfs.core.windows.net",
        "$container": "testcontainer",
    }

    # Must use strict=False to allow partial variable substitution
    results = list(read_yaml(path, root="job", variables=partial_vars, strict=False))

    assert len(results) == 2
    first = results[0]
    results[1]

    # Provided variables should be substituted
    assert "testaccount.dfs.core.windows.net" in first["options"]["uri"]
    assert "testcontainer" in first["options"]["uri"]

    # Non-provided variables should remain as-is
    assert "$catalog" in first["options"]["catalog"]


def test_read_yaml_with_empty_variables(fixtures_dir: Path) -> None:
    """Test reading YAML with empty variables dictionary (strict=False)."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    # Must use strict=False to allow missing variables
    results = list(read_yaml(path, root="job", variables={}, strict=False))

    assert len(results) == 2
    first = results[0]
    results[1]

    # All variables should remain as-is
    assert "$storage_account" in first["options"]["uri"]
    assert "$catalog" in first["options"]["catalog"]


def test_read_yaml_strict_mode_raises_on_missing_variable(fixtures_dir: Path) -> None:
    """Test that strict mode (default) raises an error for missing variables."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    # Only provide some variables, missing $catalog and $storage_account
    partial_vars = {"$container": "testcontainer"}

    # Should raise ValueError for missing variables
    with pytest.raises(ValueError, match="Variable\\(s\\) not found in lookup"):
        list(read_yaml(path, root="job", variables=partial_vars))


def test_read_yaml_strict_mode_succeeds_with_all_variables(fixtures_dir: Path, test_variables: dict[str, str]) -> None:
    """Test that strict mode (default) succeeds when all variables are provided."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    # Should not raise when all variables are provided
    results = list(read_yaml(path, root="job", variables=test_variables))

    assert len(results) == 2
    assert results[0]["options"]["catalog"] == "test_catalog"


def test_read_yaml_non_strict_mode_allows_missing_variables(fixtures_dir: Path) -> None:
    """Test that non-strict mode allows missing variables."""
    path = GitPath(str(fixtures_dir / "variables.yml"))

    # Only provide some variables
    partial_vars = {"$container": "testcontainer"}

    # With strict=False, should not raise, just leave missing variables as-is
    results = list(read_yaml(path, root="job", variables=partial_vars, strict=False))

    assert len(results) == 2
    first = results[0]

    # Missing variables should remain as placeholders
    assert "$catalog" in first["options"]["catalog"]
    assert "$storage_account" in first["options"]["uri"]

    # Provided variable should be substituted
    assert "testcontainer" in first["options"]["uri"]
