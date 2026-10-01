"""Guards BaseJob's facade surface: attributes and methods that fabricks.legacy and tests/spark/databricks/*.py
read directly on a job instance, with no in-repo caller, so they look dead and break callers outside this repo
when removed."""

from fabricks.core.jobs.base.job import BaseJob

_EXTERNALLY_CONSUMED_FACADES = [
    # state-read accessors (fabricks.legacy's compare.py, tests/spark/databricks/*.py)
    "spark",
    "table",
    "paths",
    "qualified_name",
    "cdc",
    "mode",
    "timeout",
    "get_udfs",
    # invocation/dependency facades (tests/spark/databricks/runjobs.py)
    "invoke_pre_run",
    "invoke_post_run",
    "update_dependencies",
    "check_pre_run",
    "check_post_run",
]


def test_base_job_keeps_the_externally_consumed_facade_surface():
    missing = [name for name in _EXTERNALLY_CONSUMED_FACADES if not hasattr(BaseJob, name)]
    assert not missing, f"BaseJob dropped facade(s) relied on outside this repo: {missing}"
