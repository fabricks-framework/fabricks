from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.utils.helpers import run_in_parallel

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(401)
def test_run():
    jobs = ["gold.scd1_complete", "gold.scd1_update", "gold.scd1_identity", "gold.scd2_complete", "gold.scd2_update"]

    def _reload(j: str):
        job = get_job(job=j)
        job.run(reload=True)

    _ = run_in_parallel(func=_reload, iterable=jobs)
