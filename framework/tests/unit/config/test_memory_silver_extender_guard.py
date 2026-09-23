"""Reproduces https://github.com/fabricks-framework/fabricks/issues/177:
a mode:memory silver job generates a plain `select * from {parent}` view
(Silver.create_or_replace_view()) or DataFrame (Silver.get_data()) with
no Python execution step -- so if its bronze parent has an extender
configured (job- or step-level), that extender can never actually run.
Silently ignoring it would leave the result missing whatever
transformation the extender was meant to apply, with no indication
anything is wrong. Both call sites share Silver._assert_bronze_parent_
has_no_extender() and should both raise instead.

Uses the real tests/spark/runtime fixtures (tests/spark/runtime/bronze/
_config.extender_test.yml + tests/spark/runtime/silver/_config.
extender_test.yml) rather than an in-memory conf dict, since the guard
resolves the bronze parent job for real via Bronze.from_job_id(), which
looks the job config up by job_id against the runtime -- an in-memory
conf isn't enough to satisfy that lookup.
"""

import pytest

from fabricks.core import get_job


def test_memory_silver_rejects_a_bronze_parent_with_an_extender():
    job = get_job(step="silver", topic="extender_test", item="source")

    with pytest.raises(AssertionError):
        job.create_or_replace_view()


def test_memory_silver_get_data_rejects_a_bronze_parent_with_an_extender():
    job = get_job(step="silver", topic="extender_test", item="source")

    with pytest.raises(AssertionError):
        job.get_data()
