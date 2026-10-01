"""Plain tier: pure Python, with fabricks.context and fabricks.utils.spark replaced wholesale.

Tests that need the real fabricks.context with only Spark faked live in tests/unit/config/.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

from tests.tier_policy import activate_tier

_ORIGINAL_ACTIVE_TIER = os.environ.get("FABRICKS_ACTIVE_TEST_TIER")
activate_tier("plain")

_REPLACED_MODULES = ("fabricks.utils.spark", "fabricks.context", "fabricks.context.spark_session")
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _REPLACED_MODULES}


def mock_spark():
    """Mock Spark and related dependencies for tests/unit/plain."""
    mock_spark_session = MagicMock()
    mock_dbutils_obj = MagicMock()

    sys.modules["fabricks.utils.spark"] = MagicMock(
        spark=mock_spark_session,
        dbutils=mock_dbutils_obj,
        get_spark=MagicMock(return_value=mock_spark_session),
        get_dbutils=MagicMock(return_value=mock_dbutils_obj),
    )

    mock_context = MagicMock()
    mock_context.SPARK = mock_spark_session
    mock_context.DBUTILS = mock_dbutils_obj
    sys.modules["fabricks.context"] = mock_context
    sys.modules["fabricks.context.spark_session"] = MagicMock()


# Mock must run at module level before any imports occur
mock_spark()


@pytest.fixture(scope="session", autouse=True)
def _restore_process_state():
    """See tests/unit/config/conftest.py: only matters for a process that runs pytest more than once."""
    yield
    if _ORIGINAL_ACTIVE_TIER is None:
        os.environ.pop("FABRICKS_ACTIVE_TEST_TIER", None)
    else:
        os.environ["FABRICKS_ACTIVE_TEST_TIER"] = _ORIGINAL_ACTIVE_TIER
    for name, original in _ORIGINAL_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original
