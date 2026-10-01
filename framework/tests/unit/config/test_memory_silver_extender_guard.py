"""Reproduces https://github.com/fabricks-framework/fabricks/issues/177:
a `mode: memory` silver job builds a plain `select *`
view/DataFrame with no Python step, so an extender configured on its bronze parent could never run.
Both call sites (Silver.create_or_replace_view() and Silver.get_data()) share
`_assert_bronze_parent_has_no_extender()` and must raise. Uses the real tests/spark/runtime fixtures:
the guard resolves the parent via `Bronze.from_job_id()`, which an in-memory conf cannot satisfy.
"""

import pytest

from fabricks.core import get_job
from fabricks.models import JobDependency


def test_memory_silver_rejects_a_bronze_parent_with_an_extender():
    job = get_job(step="silver", topic="extender_test", item="source")

    with pytest.raises(AssertionError, match="extender"):
        job.create_or_replace_view()


def test_memory_silver_get_data_rejects_a_bronze_parent_with_an_extender():
    job = get_job(step="silver", topic="extender_test", item="source")

    with pytest.raises(AssertionError, match="extender"):
        job.get_data()


def test_memory_silver_accepts_a_bronze_parent_without_an_extender():
    job = get_job(step="silver", topic="extender_test", item="source")
    dependency = JobDependency.from_parts(job.job_id, "bronze.king_scd1", "parent")

    job._assert_bronze_parent_has_no_extender(dependency)
