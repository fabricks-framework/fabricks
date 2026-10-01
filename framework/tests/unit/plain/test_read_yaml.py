from pathlib import Path

import pytest

from fabricks.utils.path import GitPath
from fabricks.utils.read.read_yaml import read_yaml


@pytest.fixture
def fixtures_dir() -> Path:
    return (Path(__file__).parent / "fixtures/jobs").absolute()


@pytest.fixture
def test_variables() -> dict[str, str]:
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
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job"))

    assert len(results) == 2

    assert results[0]["step"] == "bronze"
    assert results[0]["topic"] == "customers"

    assert "$storage_account" in results[0]["options"]["uri"]
    assert "$catalog" in results[0]["options"]["catalog"]


def test_read_yaml_with_variables(fixtures_dir: Path, test_variables: dict[str, str]) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job", variables=test_variables))

    assert len(results) == 2
    first = results[0]
    second = results[1]

    assert first["step"] == "bronze"
    assert first["topic"] == "customers"
    assert first["options"]["uri"] == "abfss://fabricks@testaccount.dfs.core.windows.net/testcontainer/raw/customers"
    assert first["options"]["catalog"] == "test_catalog"

    assert second["topic"] == "orders"
    assert second["options"]["uri"] == "abfss://fabricks@testaccount.dfs.core.windows.net/testcontainer/raw/orders"
    assert second["options"]["workers"] == "16"


def test_read_yaml_with_partial_variables(fixtures_dir: Path) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))
    partial_vars = {"$storage_account": "testaccount.dfs.core.windows.net", "$container": "testcontainer"}

    results = list(read_yaml(path, root="job", variables=partial_vars, strict=False))

    assert len(results) == 2
    first = results[0]

    assert "testaccount.dfs.core.windows.net" in first["options"]["uri"]
    assert "testcontainer" in first["options"]["uri"]

    assert "$catalog" in first["options"]["catalog"]


def test_read_yaml_with_empty_variables(fixtures_dir: Path) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job", variables={}, strict=False))

    assert len(results) == 2
    first = results[0]

    assert "$storage_account" in first["options"]["uri"]
    assert "$catalog" in first["options"]["catalog"]


def test_read_yaml_strict_mode_raises_on_missing_variable(fixtures_dir: Path) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))

    partial_vars = {"$container": "testcontainer"}

    with pytest.raises(ValueError, match="Variable\\(s\\) not found in lookup"):
        list(read_yaml(path, root="job", variables=partial_vars, strict=True))


def test_read_yaml_strict_mode_succeeds_with_all_variables(fixtures_dir: Path, test_variables: dict[str, str]) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))

    results = list(read_yaml(path, root="job", variables=test_variables, strict=True))

    assert len(results) == 2
    assert results[0]["options"]["catalog"] == "test_catalog"


def test_read_yaml_non_strict_mode_allows_missing_variables(fixtures_dir: Path) -> None:
    path = GitPath(str(fixtures_dir / "variables.yml"))

    partial_vars = {"$container": "testcontainer"}

    results = list(read_yaml(path, root="job", variables=partial_vars, strict=False))

    assert len(results) == 2
    first = results[0]

    assert "$catalog" in first["options"]["catalog"]
    assert "$storage_account" in first["options"]["uri"]

    assert "testcontainer" in first["options"]["uri"]
