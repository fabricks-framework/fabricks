"""Conftest for tests/plain - pure-Python tests with no Spark/fabricks.context
dependency, automatically applies the 'plain' marker. Everything under this
directory gets fabricks.context replaced wholesale; for tests that need the
*real* fabricks.context with only Spark faked, see tests/spark/config/.
"""

from pathlib import Path
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest


def mock_spark():
    """Mock Spark and related dependencies for tests/plain."""
    mock_spark_session = MagicMock()
    mock_dbutils_obj = MagicMock()

    # Mock fabricks.utils.spark
    sys.modules["fabricks.utils.spark"] = MagicMock(
        spark=mock_spark_session,
        dbutils=mock_dbutils_obj,
        get_spark=MagicMock(return_value=mock_spark_session),
        get_dbutils=MagicMock(return_value=mock_dbutils_obj),
    )

    # Mock fabricks.context and related modules
    mock_context = MagicMock()
    mock_context.SPARK = mock_spark_session
    mock_context.DBUTILS = mock_dbutils_obj
    sys.modules["fabricks.context"] = mock_context
    sys.modules["fabricks.context.spark_session"] = MagicMock()


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


def pytest_collection_modifyitems(items):
    """Automatically add 'plain' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.plain)
        except (ValueError, AttributeError):
            # Fallback for older Python or edge cases
            if "plain" in str(item.fspath):
                item.add_marker(pytest.mark.plain)
