from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration.compare import get_last_status

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(191)
def test_gold_invoke_notebook():
    j = get_job(step="gold", topic="invoke", item="notebook")
    status = get_last_status(j.job_id)
    assert status == "done"


@pytest.mark.order(192)
def test_gold_invoke_failed_pre_run():
    j = get_job(step="gold", topic="invoke", item="failed_pre_run")
    status = get_last_status(j.job_id)
    assert status == "failed"


@pytest.mark.order(193)
def test_gold_invoke_post_run():
    j = get_job(step="gold", topic="invoke", item="post_run")
    status = get_last_status(j.job_id)
    assert status == "done"
