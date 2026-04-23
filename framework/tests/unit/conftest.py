"""Conftest for unit tests - automatically applies unit marker."""

import sys
from pathlib import Path
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
