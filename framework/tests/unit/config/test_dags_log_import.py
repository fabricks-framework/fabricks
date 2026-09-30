"""fabricks.core.dags.log must import for real (no fake) and resolve its table lazily."""

from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


def test_dags_log_is_the_real_module_and_resolves_nothing_at_import():
    assert type(LOGGER).__name__ == "Logger"
    assert type(TABLE_LOG_HANDLER).__name__ == "AzureTableLogHandler"
    assert TABLE_LOG_HANDLER._table is None
