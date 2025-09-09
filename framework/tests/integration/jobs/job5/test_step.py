from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(501)
def test_step_get_dependencies():
    step = get_step("gold")

    deps, error = step.get_dependencies()
    assert deps.count() == 29, f"{deps.count()} dependencies <> 29"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies()
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"

    deps, error = step.get_dependencies(topic="scd1")
    assert deps.count() == 5, f"{deps.count()} dependencies <> 5"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies(topic="scd1")
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"

    step.update_dependencies(topic="itdoesnotexist")
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"
