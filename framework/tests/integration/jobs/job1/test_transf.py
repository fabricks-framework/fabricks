from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(121)
def test_transf_fact_wait_for():
    j = get_job(step="transf", topic="fact", item="wait_for")

    depedencies = [d.parent for d in j.get_dependencies()]
    expected_dependencies = {
        "silver.monarch_scd1",
        "transf.fact_memory",
    }

    assert len(depedencies) == 2, f"{len(depedencies)} <> 2"
    assert set(depedencies) == expected_dependencies, f"{set(depedencies)} <> {expected_dependencies}"
