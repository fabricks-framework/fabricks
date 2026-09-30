"""fabricks.core.dags.log must import for real (no fake) and resolve its table lazily."""

import pytest

from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


@pytest.fixture(autouse=True)
def _fake_dags_log_table():
    """Override the conftest fixture of the same name: this test inspects the untouched handler."""


def test_dags_log_is_the_real_module_and_resolves_nothing_at_import():
    assert type(LOGGER).__name__ == "Logger"
    assert type(TABLE_LOG_HANDLER).__name__ == "AzureTableLogHandler"
    assert TABLE_LOG_HANDLER._table is None
