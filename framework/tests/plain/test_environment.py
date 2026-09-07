# framework/tests/plain/test_environment.py
import importlib

import pytest


def test_default_is_databricks(monkeypatch):
    monkeypatch.delenv("FABRICKS_ENVIRONMENT", raising=False)
    from fabricks.utils import environment

    importlib.reload(environment)
    assert environment.FABRICKS_ENVIRONMENT == "databricks"


def test_docker_value(monkeypatch):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "docker")
    from fabricks.utils import environment

    importlib.reload(environment)
    assert environment.FABRICKS_ENVIRONMENT == "docker"


def test_invalid_value_raises(monkeypatch):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "bogus")
    from fabricks.utils import environment

    with pytest.raises(AssertionError):
        importlib.reload(environment)
