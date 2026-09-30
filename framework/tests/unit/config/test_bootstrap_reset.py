"""The bootstrap Spark/dbutils mocks are shared by the whole process; they must be reset between tests."""

from fabricks.context import SPARK
from tests.unit.config import conftest as config_conftest


def test_reset_clears_configured_return_values_side_effects_and_assigned_children():
    SPARK.some_method.return_value = 42
    SPARK.other_method.side_effect = RuntimeError("leak")

    config_conftest._reset_bootstrap_mocks()

    assert SPARK.some_method.return_value != 42
    SPARK.other_method()  # would raise the leaked side effect


def test_reset_keeps_magic_method_defaults():
    assert bool(SPARK) is True  # first use creates the `__bool__` child

    config_conftest._reset_bootstrap_mocks()

    assert bool(SPARK) is True  # a reset `__bool__` would raise TypeError here (resolver does `if not SPARK`)


def test_reset_runs_automatically_around_every_test(request):
    assert "_reset_bootstrap_mocks_fixture" in request.fixturenames
