from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs import get_job

DEFAULT_LOGGER.setLevel(ERROR)


def _get_last_error(job_id: str, status: str = "failed"):
    return (
        SPARK.sql(
            f"""
            select 
              l.exception.message as error, 
              l.timestamp 
            from 
              fabricks.logs l 
            where 
              true
              and l.job_id = '{job_id}' 
              and l.status = '{status}' 
            order by timestamp desc 
            limit 1
            """
        )
        .select("error")
        .collect()[0][0]
    )


@pytest.mark.order(161)
def test_gold_check_fail():
    j = get_job(step="gold", topic="check", item="fail")
    error = _get_last_error(j.job_id)
    assert error == "Please don't fail on me :("
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(162)
def test_gold_check_warning():
    j = get_job(step="gold", topic="check", item="warning")
    error = _get_last_error(j.job_id, status="warned")
    assert error == "I want you to warn me !"
    assert j.table.rows > 0, "table should not be empty"


@pytest.mark.order(163)
def test_gold_check_max_rows():
    j = get_job(step="gold", topic="check", item="max_rows")
    error = _get_last_error(j.job_id)
    assert error == "max rows check failed (3 > 2)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(164)
def test_gold_check_min_rows():
    j = get_job(step="gold", topic="check", item="min_rows")
    error = _get_last_error(j.job_id)
    assert error == "min rows check failed (1 < 2)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(165)
def test_gold_check_count_must_equal():
    j = get_job(step="gold", topic="check", item="count_must_equal")
    error = _get_last_error(j.job_id)
    assert error == "count must equal check failed (fabricks.dummy - 2 != 1)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(166)
def test_gold_check_skip():
    j = get_job(step="gold", topic="check", item="skip")
    error = _get_last_error(j.job_id, status="skipped")
    assert error == "I want you to skip this !"
    assert j.table.rows == 0, "table should be empty"
