"""Job-level vs step-level option precedence: Generator._get_option_hierarchy
(framework/fabricks/core/jobs/base/generator.py:17) resolves job options ->
step options -> default, in that order. Mirrors
tests/spark/databricks/runtime/semantic/fact/_config.semantic.yml's
step_option/job_option fixtures (see tests/spark/databricks/jobs/job1/
test_semantic.py's test_semantic_fact_step_option/test_semantic_fact_job_option),
against the "semantic" gold-step declared locally in
tests/spark/apache/runtime/fabricks/conf.fabricks.yml + tests/spark/apache/runtime/semantic/
_config.fact.yml.
"""

from fabricks.core import get_job


def test_option_hierarchy_falls_back_to_step_level_when_job_omits_it():
    job = get_job(step="semantic", topic="fact", item="step_option")

    properties = job._get_option_hierarchy("properties", into="table")

    assert properties == {
        "delta.minReaderVersion": 1,
        "delta.minWriterVersion": 7,
        "delta.columnMapping.mode": "none",
    }


def test_option_hierarchy_job_level_overrides_step_level():
    job = get_job(step="semantic", topic="fact", item="job_option")

    properties = job._get_option_hierarchy("properties", into="table")

    assert properties == {
        "delta.minReaderVersion": 2,
        "delta.minWriterVersion": 5,
        "delta.columnMapping.mode": "none",
    }


def test_option_hierarchy_returns_default_when_neither_level_sets_it():
    job = get_job(step="semantic", topic="fact", item="step_option")

    result = job._get_option_hierarchy("masks", into="table", default="fallback")

    assert result == "fallback"
