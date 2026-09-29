"""get_job_conf caches all rows for a step on first lookup (see
fabricks/core/jobs/get_job_conf.py's _get_step_rows) so N calls for
different jobs of the same step re-read/re-query the step once, not N
times.
"""

from fabricks.core.jobs.get_job_conf import _get_step_rows, clear_job_conf_cache, get_job_conf


def test_get_job_conf_reuses_cached_rows_across_jobs_of_the_same_step():
    clear_job_conf_cache()

    step_option = get_job_conf(step="gold", topic="fact", item="step_option")
    job_option = get_job_conf(step="gold", topic="fact", item="job_option")

    assert step_option.topic == "fact"
    assert step_option.item == "step_option"
    assert job_option.item == "job_option"

    info = _get_step_rows.cache_info()
    assert info.hits == 1
    assert info.misses == 1


def test_clear_job_conf_cache_resets_the_cache():
    get_job_conf(step="gold", topic="fact", item="step_option")
    assert _get_step_rows.cache_info().currsize > 0

    clear_job_conf_cache()

    assert _get_step_rows.cache_info().currsize == 0
