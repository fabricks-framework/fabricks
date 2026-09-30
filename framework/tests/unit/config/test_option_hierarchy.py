"""Option resolution: job-level vs step-level precedence, the timeout fallback chain, and the gold `table` option.

Job-level vs step-level option precedence: Generator._get_option_hierarchy
(framework/fabricks/core/jobs/base/generator.py:17) resolves job options ->
step options -> default, in that order. Mirrors
tests/spark/databricks/runtime/semantic/fact/_config.semantic.yml's
step_option/job_option fixtures (see tests/spark/databricks/jobs/job1/
test_semantic.py's test_semantic_fact_step_option/test_semantic_fact_job_option),
against the "gold" step declared locally in
tests/spark/runtime/fabricks/conf.fabricks.yml + tests/spark/runtime/gold/
_config.fact.yml.
"""

from unittest.mock import sentinel

from fabricks.core import get_job
from fabricks.models import StepTimeoutOptions
from fabricks.models.table import TableOptions


def test_option_hierarchy_falls_back_to_step_level_when_job_omits_it():
    job = get_job(step="gold", topic="fact", item="step_option")

    properties = job._generator._get_option_hierarchy("properties", into="table")

    assert properties == {"delta.minReaderVersion": 1, "delta.minWriterVersion": 7, "delta.columnMapping.mode": "none"}


def test_option_hierarchy_job_level_overrides_step_level():
    job = get_job(step="gold", topic="fact", item="job_option")

    properties = job._generator._get_option_hierarchy("properties", into="table")

    assert properties == {"delta.minReaderVersion": 2, "delta.minWriterVersion": 5, "delta.columnMapping.mode": "none"}


def test_option_hierarchy_returns_default_when_neither_level_sets_it():
    job = get_job(step="gold", topic="fact", item="step_option")

    result = job._generator._get_option_hierarchy("masks", into="table", default="fallback")

    assert result == "fallback"


def test_option_hierarchy_masks_job_level_wins_when_both_set():
    from fabricks.models.table import StepTableOptions

    job = get_job(step="gold", topic="fact", item="step_option")

    step_conf = job.step_conf
    step_table_options = (step_conf.table_options or StepTableOptions()).model_copy(
        update={"masks": {"dummy": "step_mask"}}
    )
    job.base_step_conf = step_conf.model_copy(update={"table_options": step_table_options})

    job.conf = job.conf.model_copy(update={"table_options": TableOptions(masks={"dummy": "job_mask"})})

    masks = job._generator._get_option_hierarchy("masks", into="table")

    assert masks == {"dummy": "job_mask"}


# --- timeout: job-level options.timeout -> step-level step_options.timeouts.job -> runtime_options.timeouts.job.
# --- The runtime fallback (3600) comes from tests/spark/runtime/fabricks/conf.fabricks.yml; the "gold" step there
# --- declares no `timeouts` of its own.


def _job():
    return get_job(step="gold", topic="fact", item="step_option")


def test_timeout_job_level_wins():
    job = _job()
    job.conf = job.conf.model_copy(update={"options": job.conf.options.model_copy(update={"timeout": 42})})

    assert job._resolver.timeout == 42


def test_timeout_falls_back_to_step_level_when_job_level_unset():
    job = _job()
    step_conf = job.step_conf
    new_options = step_conf.options.model_copy(update={"timeouts": StepTimeoutOptions(job=1800)})
    job.base_step_conf = step_conf.model_copy(update={"options": new_options})

    assert job._resolver.timeout == 1800


def test_timeout_falls_back_to_runtime_when_neither_job_nor_step_set():
    job = _job()

    assert job._resolver.timeout == 3600


# --- gold `table` option: get_data() reads the configured table.


def test_gold_table_option_reads_configured_table():
    job = get_job(step="gold", topic="fact", item="table_option")
    read_table = job.spark.read.table
    read_table.reset_mock()
    read_table.return_value = sentinel.dataframe

    assert job.get_data() is sentinel.dataframe
    read_table.assert_called_once_with("gold.table_option_source")
