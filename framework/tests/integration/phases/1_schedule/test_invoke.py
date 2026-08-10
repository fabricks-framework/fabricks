from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs import get_job


def _get_last_status(job_id: str):
    return (
        SPARK.sql(
            f"""
            select 
              l.status,
              l.timestamp 
            from 
              fabricks.logs l 
            where 
              true
              and l.job_id = '{job_id}' 
              and l.status in ('failed', 'done')
            order by timestamp desc 
            limit 1
            """
        )
        .select("status")
        .collect()[0][0]
    )


DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(191)
def test_gold_invoke_notebook():
    j = get_job(step="gold", topic="invoke", item="notebook")
    status = _get_last_status(j.job_id)
    assert status == "done"


@pytest.mark.order(192)
def test_gold_invoke_failed_pre_run():
    j = get_job(step="gold", topic="invoke", item="failed_pre_run")
    status = _get_last_status(j.job_id)
    assert status == "failed"


@pytest.mark.order(193)
def test_gold_invoke_post_run():
    j = get_job(step="gold", topic="invoke", item="post_run")
    status = _get_last_status(j.job_id)
    assert status == "done"
