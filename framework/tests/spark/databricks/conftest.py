"""Databricks test-tier activation and the one schedule run every test in
this directory reads its state from.
"""

import os
import re

import pytest
from tier_policy import activate_tier

from fabricks.core.schedules import standalone
from fabricks.deploy import Deploy

activate_tier("databricks")

# Both fixtures below are expensive (notebook deploy + full schedule run); runtest.py sets this to skip them
# for self-contained scratch tests.
_SKIP_SCHEDULE_FIXTURES = os.environ.get("FABRICKS_SKIP_FIXTURES") == "true"
# DagTerminator.terminate() raises this on any failed job; the deliberate ones are checked by test_no_unforced_*.
_EXPECTED_FAILURE = re.compile(r"\d+ job\(s\) failed")


@pytest.fixture(scope="session", autouse=True)
def _notebooks_deployed():
    # Import through the Workspace API so each orchestration notebook is a real notebook object,
    # not whatever `databricks bundle sync` last converted.
    if _SKIP_SCHEDULE_FIXTURES:
        return
    Deploy.notebooks(overwrite=True)


@pytest.fixture(scope="session", autouse=True)
def _schedule_run(_notebooks_deployed):
    # Here, not in test_schedule.py: pytest collects test_feature.py first, so a module-level fixture
    # wouldn't run the schedule before tests that read its results.
    if _SKIP_SCHEDULE_FIXTURES:
        return
    try:
        standalone(schedule="test")
    except ValueError as e:
        # only the terminator's "N job(s) failed" is expected; any other ValueError is a broken run, and the
        # tests below would then read the previous schedule's last_schedule rows
        if not _EXPECTED_FAILURE.fullmatch(str(e)):
            raise
