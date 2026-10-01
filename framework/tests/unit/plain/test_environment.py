"""FABRICKS_ENVIRONMENT is parsed at import: unset defaults to databricks, case is ignored, anything else
fails fast. Each test executes a throwaway copy of the module so the real one is never rebound."""

import importlib.util

import pytest

from fabricks.utils import environment as real_environment


def _fresh_environment():
    spec = importlib.util.spec_from_file_location("_environment_under_test", real_environment.__file__)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_is_databricks(monkeypatch):
    monkeypatch.delenv("FABRICKS_ENVIRONMENT", raising=False)

    assert _fresh_environment().FABRICKS_ENVIRONMENT == "databricks"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("docker", "docker"), ("remote", "remote"), ("DOCKER", "docker")],
    ids=["docker", "remote", "case-insensitive"],
)
def test_value_is_normalised(monkeypatch, raw, expected):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", raw)

    assert expected == _fresh_environment().FABRICKS_ENVIRONMENT


def test_invalid_value_raises(monkeypatch):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "bogus")

    with pytest.raises(AssertionError, match="FABRICKS_ENVIRONMENT must be one of"):
        _fresh_environment()
