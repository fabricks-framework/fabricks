"""Conftest for the Databricks-only integration tests - automatically applies databricks marker."""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Automatically add 'databricks' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.databricks)
        except (ValueError, AttributeError):
            # Fallback for older Python or edge cases
            if "databricks" in str(item.fspath):
                item.add_marker(pytest.mark.databricks)
