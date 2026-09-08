"""Real dbutils.notebook.run invocation status propagation.

One notebook, invoked directly via get_job(...).run() -- not through a
schedule, unlike test_schedule.py -- asserting the resulting fabricks.last_status
row. See runtime_min/gold/gold/invoke/_config.invoke.yml for the three jobs.
"""

import pytest

from fabricks.context import SPARK
from fabricks.core import get_job


def _last_status(job_id: str):
    return SPARK.sql(f"select done, failed from fabricks.last_status where job_id = '{job_id}'").collect()[0]


def test_gold_invoke_notebook():
    j = get_job(step="gold", topic="invoke", item="notebook")
    j.run()
    assert _last_status(j.job_id).done


def test_gold_invoke_failed_pre_run():
    j = get_job(step="gold", topic="invoke", item="failed_pre_run")
    with pytest.raises(Exception):  # noqa: B017 - pre_run notebook deliberately raises
        j.run()
    assert _last_status(j.job_id).failed


def test_gold_invoke_post_run():
    j = get_job(step="gold", topic="invoke", item="post_run")
    j.run()
    assert _last_status(j.job_id).done
