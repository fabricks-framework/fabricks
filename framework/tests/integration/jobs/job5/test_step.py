from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step

DEFAULT_LOGGER.setLevel(ERROR)

# Update these numbers if you add or remove jobs in gold
deps_in_gold = 32
jobs_in_gold = 55


@pytest.mark.order(501)
def test_update_dependencies():
    step = get_step("gold")

    deps, error = step.get_dependencies(loglevel=ERROR)
    assert deps.count() == deps_in_gold, f"{deps.count()} dependencies <> {deps_in_gold}"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies(loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == deps_in_gold, f"{df.count()} dependencies <> {deps_in_gold}"

    deps, error = step.get_dependencies(topic="scd1", loglevel=ERROR)
    assert deps.count() == 6, f"{deps.count()} dependencies <> 6"
    assert len(error) == 0, f"{error} error(s)"

    step.update_dependencies(topic="scd1", loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == deps_in_gold, f"{df.count()} dependencies <> {deps_in_gold}"

    step.update_dependencies(topic="it_does_not_exist", loglevel=ERROR)
    df = SPARK.sql("select * from fabricks.gold_dependencies")
    assert df.count() == deps_in_gold, f"{df.count()} dependencies <> {deps_in_gold}"


@pytest.mark.order(502)
def test_create_db_objects():
    step = get_step("transf")

    SPARK.sql("drop view if exists transf.fact_memory")
    step.update_views_list()

    df = SPARK.sql("select * from fabricks.transf_views")
    assert df.count() == 0, f"{df.count()} view(s) <> 0"

    step.create_db_objects()

    df = SPARK.sql("select * from fabricks.transf_views")
    assert df.count() == 1, f"{df.count()} view(s) <> 1"


@pytest.mark.order(503)
def test_update_configurations():
    step = get_step("gold")

    step.update_configurations()

    df = SPARK.sql("select * from fabricks.gold_jobs")
    assert df.count() == jobs_in_gold, f"{df.count()} job(s) <> {jobs_in_gold}"

    SPARK.sql("update fabricks.gold_jobs set options = null")

    df = SPARK.sql("select * from fabricks.gold_jobs where options is null")
    assert df.count() == jobs_in_gold, f"{df.count()} job(s) without options <> {jobs_in_gold}"

    SPARK.sql("delete from fabricks.gold_jobs where topic == 'scd1'")

    step.update_configurations()

    df = SPARK.sql("select * from fabricks.gold_jobs")
    assert df.count() == jobs_in_gold, f"{df.count()} job(s) <> {jobs_in_gold}"

    df = SPARK.sql("select * from fabricks.gold_jobs where options is null")
    assert df.count() == 0, f"{df.count()} job(s) without options <> 0"
