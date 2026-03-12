from logging import ERROR

import pytest

from fabricks.context import SPARK
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

    df = SPARK.sql("""
        select
            max(if(job = 'transf.fact_memory', end_time, null)) as last_memory,
            max(if(job = 'transf.fact_wait_for', start_time, null)) as first_wait_for,
            first_wait_for > last_memory as check
        from
            fabricks.last_schedule
        where
            job in ('transf.fact_memory', 'transf.fact_wait_for')
    """)
    check = df.toPandas().to_dict(orient="records")[0]["check"]
    assert check, "transf.fact_wait_for started before transf.fact_memory ended"
