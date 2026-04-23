"""Conftest for unit tests - automatically applies unit marker."""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest


def mock_spark():
    """Mock Spark and related dependencies for unit tests."""
    mock_spark = MagicMock()
    mock_dbutils = MagicMock()

    sys.modules["fabricks.utils.spark"] = MagicMock(
        spark=mock_spark,
        dbutils=mock_dbutils,
        get_spark=MagicMock(return_value=mock_spark),
        get_dbutils=MagicMock(return_value=mock_dbutils),
        DATABRICKS_LOCALMODE=False,
    )


# Mock must run at module level before any imports occur
mock_spark()


@pytest.fixture
def minimal_runtime_config() -> dict[str, Any]:
    """Minimal valid RuntimeConf configuration for testing."""
    return {
        "name": "test",
        "options": {
            "secret_scope": "test_scope",
            "timeouts": {"step": 3600, "job": 3600, "pre_run": 3600, "post_run": 3600},
        },
        "path_options": {
            "storage": "abfss://test",
            "udfs": "fabricks/udfs",
            "parsers": "fabricks/parsers",
            "schedules": "fabricks/schedules",
            "views": "fabricks/views",
            "requirements": "fabricks/requirements",
        },
    }


@pytest.fixture
def runtime_config_factory(minimal_runtime_config):
    """Factory to build RuntimeConf configs with overrides.

    Example:
        config = runtime_config_factory(
            options={"workers": "$workers"},
            variables={"$workers": "8"}  # Note: variables must be strings
        )
    """

    def _factory(**overrides: Any) -> dict[str, Any]:
        import copy

        config = copy.deepcopy(minimal_runtime_config)

        # Deep merge overrides
        for key, value in overrides.items():
            if key in config and isinstance(config[key], dict) and isinstance(value, dict):
                config[key].update(value)
            else:
                config[key] = value

        # Ensure variables are strings (RuntimeConf requirement)
        if "variables" in config and config["variables"]:
            config["variables"] = {k: str(v) for k, v in config["variables"].items()}

        return config

    return _factory


def pytest_collection_modifyitems(items):
    """Automatically add 'unit' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.unit)
        except (ValueError, AttributeError):
            # Fallback for older Python or edge cases
            if "unit" in str(item.fspath):
                item.add_marker(pytest.mark.unit)
