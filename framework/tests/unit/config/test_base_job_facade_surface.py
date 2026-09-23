"""Guards BaseJob's facade surface: the attributes/methods external
consumers this repo's own test suite never exercises -- the separate
fabricks.legacy test suite, and tests/spark/databricks/*.py -- read
directly on a job instance (job.paths, job.mode, ...).

During the job-composition refactor these were removed as apparently-dead
ceremony (no in-repo caller), then had to be restored one by one after
breaking real callers outside this repo. This test exists so that mistake
doesn't repeat silently: it fails fast here instead of after a real
Databricks deploy.
"""

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
