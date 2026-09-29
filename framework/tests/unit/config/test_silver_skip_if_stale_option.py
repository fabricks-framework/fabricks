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


def test_skip_if_stale_defaults_to_true():
    job = _silver_job()
    assert job.skip_if_stale is True


def test_skip_if_stale_step_level_override():
    job = _silver_job(step_skip_if_stale=False)
    assert job.skip_if_stale is False


def test_skip_if_stale_job_level_wins_over_step():
    job = _silver_job(job_skip_if_stale=True, step_skip_if_stale=False)
    assert job.skip_if_stale is True


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
