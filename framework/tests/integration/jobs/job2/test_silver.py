from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration.compare import compare_silver_to_expected

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(211)
def test_silver_monarch_scd2():
    job = get_job(step="silver", topic="monarch", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=2)


@pytest.mark.order(212)
def test_silver_monarch_scd1():
    job = get_job(step="silver", topic="monarch", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=2)


@pytest.mark.order(211)
def test_silver_regent_scd2():
    job = get_job(step="silver", topic="regent", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=2)


@pytest.mark.order(212)
def test_silver_regent_scd1():
    job = get_job(step="silver", topic="regent", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=2)


@pytest.mark.order(213)
def test_silver_memory_scd2():
    job = get_job(step="silver", topic="memory", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=2)


@pytest.mark.order(214)
def test_silver_memory_scd1():
    job = get_job(step="silver", topic="memory", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=2)


@pytest.mark.order(215)
def test_silver_king_and_queen_scd2():
    job = get_job(step="silver", topic="king_and_queen", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=2)


@pytest.mark.order(216)
def test_silver_king_and_queen_scd1():
    job = get_job(step="silver", topic="king_and_queen", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=2)


@pytest.mark.order(219)
def test_silver_princess_append():
    job = get_job(step="silver", topic="princess", item="append")
    assert job.table.dataframe.count() == 4, "count <> 4"


@pytest.mark.order(219)
def test_silver_princess_latest():
    job = get_job(step="silver", topic="princess", item="latest")
    assert job.table.dataframe.count() == 2, "count <> 2"


@pytest.mark.order(219)
def test_silver_prince_deletelog():
    job = get_job(step="silver", topic="prince", item="deletelog")
    assert job.table.dataframe.where("__is_deleted").count() == 1, "count <> 1"


@pytest.mark.order(219)
def test_silver_princess_schema_drift():
    job = get_job(step="silver", topic="princess", item="schema_drift")
    df = job.table.dataframe
    assert "newField" in df.columns, "newField not found in table"

    df = SPARK.sql("select * from silver.princess_schema_drift__current")
    assert "newField" in df.columns, "newField not found in current view"


@pytest.mark.order(219)
def test_silver_princess_check():
    job = get_job(step="silver", topic="princess", item="check")
    assert job.table.get_property("fabricks.last_batch") == "0", "last batch <> 0"
    assert job.paths.commits.join("0").exists(), "commit 0 not found"
    assert not job.paths.commits.join("1").exists(), "commit 1 found"
