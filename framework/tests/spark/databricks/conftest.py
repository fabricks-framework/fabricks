"""Databricks test-tier activation and the one schedule run every test in
this directory reads its state from.
"""

import contextlib
import os

import pytest
from tier_policy import activate_tier

from fabricks.core.schedules import standalone
from fabricks.deploy import Deploy

activate_tier("databricks")

# Both fixtures below are expensive (notebook deploy + a full schedule run)
# and only needed by tests that read state the schedule produced. Ad-hoc
# scoped runs (tests/spark/databricks/runtest.py, for a self-contained
# scratch test that doesn't touch schedule state) can skip both by setting
# this env var before invoking pytest.
_SKIP_SCHEDULE_FIXTURES = os.environ.get("FABRICKS_SKIP_FIXTURES") == "true"


@pytest.fixture(scope="session", autouse=True)
def _notebooks_deployed():
    # fabricks' own orchestration notebooks (cluster/initialize/process/
    # standalone/run/terminate) get imported via the Workspace API's
    # explicit import_() call (fabricks/deploy/notebooks.py) into
    # FABRICKS_NOTEBOOKS -- a real Databricks notebook object each time,
    # not however `databricks bundle sync` happens to have converted the
    # matching .py file most recently.
    if _SKIP_SCHEDULE_FIXTURES:
        return
    Deploy.notebooks(overwrite=True)


@pytest.fixture(scope="session", autouse=True)
def _schedule_run(_notebooks_deployed):
    # Session-scoped + autouse here (not in test_schedule.py) so it fires
    # before the first test collected in *any* file in this directory --
    # pytest collects alphabetically (test_feature.py before
    # test_schedule.py), so a module-level autouse fixture wouldn't
    # guarantee the schedule runs before tests that read its results.
    # Depends on _notebooks_deployed so that always runs first.
    # DagTerminator.terminate() (fabricks/core/dags/terminator.py) raises
    # "N job(s) failed" whenever any job in the run doesn't reach done --
    # true by design here (check_fail/check_skip/check_max_rows/
    # check_duplicate_key/invoke_timeout/invoke_failed_pre_run are all
    # deliberate). test_no_unforced_failures/test_no_unforced_skips do the
    # real, precise validation against the expected job sets; this fixture
    # shouldn't also fail the whole session on the expected case.
    if _SKIP_SCHEDULE_FIXTURES:
        return
    with contextlib.suppress(ValueError):
        standalone(schedule="test")
