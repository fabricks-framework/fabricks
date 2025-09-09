from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(501)
def test_step_gold():
    step = get_step("gold")

    deps, error = step.get_dependencies(loglevel=ERROR)
    assert deps.count() == 29, f"{deps.count()} dependencies <> 29"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies(loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"

    deps, error = step.get_dependencies(topic="scd1", loglevel=ERROR)
    assert deps.count() == 5, f"{deps.count()} dependencies <> 5"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies(topic="scd1", loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"

    step.update_dependencies(topic="it_does_not_exist", loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == 29, f"{df.count()} dependencies <> 29"


@pytest.mark.order(502)
def test_step_transf():
    step = get_step("transf")

    SPARK.sql("drop view if exists transf.fact_memory")
    step.update_views_list()

    df = SPARK.sql("select * from fabricks.transf_views")
    assert df.count() == 0, f"{df.count()} view(s) <> 0"

    step.create_db_objects()

    df = SPARK.sql("select * from fabricks.transf_views")
    assert df.count() == 1, f"{df.count()} view(s) <> 1"
