import pytest

from fabricks.core.jobs.silver import Silver
from fabricks.models import StepPathOptions, StepSilverConf, StepSilverOptions


def _silver_job(*, job_skip_if_stale=None, step_skip_if_stale=None):
    conf = {
        "step": "silver",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": "update", "change_data_capture": "scd1", "skip_if_stale": job_skip_if_stale},
    }
    job = Silver(step="silver", topic="fact", item="dummy", conf=conf)
    job.base_step_conf = StepSilverConf(
        name="silver",
        path_options=StepPathOptions(runtime="silver", storage="silver"),
        options=StepSilverOptions(order=1, parent="bronze", skip_if_stale=step_skip_if_stale),
    )
    return job


@pytest.mark.parametrize(
    ("job_value", "step_value", "expected"),
    [
        pytest.param(None, None, True, id="default-true"),
        pytest.param(None, True, True, id="step-true"),
        pytest.param(None, False, False, id="step-false"),
        pytest.param(True, None, True, id="job-true"),
        pytest.param(False, None, False, id="job-false"),
        pytest.param(True, False, True, id="job-true-beats-step-false"),
        # a falsy job value is still a value: only None falls through to the step
        pytest.param(False, True, False, id="job-false-beats-step-true"),
        pytest.param(True, True, True, id="both-true"),
        pytest.param(False, False, False, id="both-false"),
    ],
)
def test_skip_if_stale_job_value_wins_over_step_and_only_none_falls_through(job_value, step_value, expected):
    job = _silver_job(job_skip_if_stale=job_value, step_skip_if_stale=step_value)

    assert job.skip_if_stale is expected


def test_bronze_and_gold_never_skip_regardless_of_dependency_status():
    # BaseJob.skip_if_stale defaults to False -- only Silver overrides it.
    from fabricks.core.jobs.bronze import Bronze
    from fabricks.core.jobs.gold import Gold

    bronze = Bronze(
        step="bronze",
        topic="fact",
        item="dummy",
        conf={"step": "bronze", "topic": "fact", "item": "dummy", "options": {"mode": "append", "uri": "dummy"}},
    )
    gold = Gold(
        step="gold",
        topic="fact",
        item="dummy",
        conf={"step": "gold", "topic": "fact", "item": "dummy", "options": {"mode": "complete"}},
    )
    assert bronze.skip_if_stale is False
    assert gold.skip_if_stale is False
