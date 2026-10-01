"""Job-facade behavior that needs no Spark: step-row caching in get_job_conf, `get_job(orphan=True)`,
the `no_drop` guard, silver empty-batch handling, and the stream-batch worker."""

import importlib
from unittest.mock import MagicMock

import pytest

from fabricks.core import get_job
from fabricks.core.jobs import OrphanJob
from fabricks.core.jobs.base.exception import UnchangedWarning
from fabricks.core.jobs.base.job import _for_each_stream_batch
from fabricks.core.jobs.get_job_conf import _get_step_rows, clear_job_conf_cache, get_job_conf
from fabricks.core.jobs.silver import Silver
from tests.unit.config._helpers import _FakeDF


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


# get_job(orphan=True): https://github.com/fabricks-framework/fabricks/issues/198


def test_get_job_orphan_returns_an_orphan_job():
    job = get_job(step="silver", topic="foo", item="bar", orphan=True)
    assert isinstance(job, OrphanJob)
    assert (job.step, job.topic, job.item) == ("silver", "foo", "bar")


def test_get_job_orphan_defaults_to_false():
    job = get_job(step="silver", topic="append_test", item="test")

    assert type(job) is Silver


def test_get_job_orphan_rejects_job_id():
    with pytest.raises(AssertionError, match="job_id"):
        get_job(step="silver", topic="foo", item="bar", job_id="deadbeef", orphan=True)  # ty: ignore[no-matching-overload]


# Generator.drop() swallows errors from its spark.sql(...) calls, so only the no_drop guard is observable.


def _job(*, no_drop: bool | None = None):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(update={"options": job.conf.options.model_copy(update={"no_drop": no_drop})})
    return job


def test_drop_raises_when_no_drop_is_set():
    job = _job(no_drop=True)

    with pytest.raises(ValueError, match="no_drop"):
        job.drop()


def test_drop_runs_the_drop_statements_when_no_drop_is_unset():
    job = _job()
    job.spark.sql.reset_mock()

    job.drop()

    assert job.spark.sql.called, "the no_drop guard must not fire when the option is unset"


def _silver_job_with_empty_batch():
    conf = {
        "step": "silver",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": "update", "change_data_capture": "scd1", "stream": True},
    }
    job = Silver(step="silver", topic="fact", item="dummy", conf=conf)
    job._resolver._spark = MagicMock()
    job.spark.sql.return_value.isEmpty.return_value = True
    return job


def test_silver_for_each_batch_raises_unchanged_when_batch_has_no_data():
    job = _silver_job_with_empty_batch()

    with pytest.raises(UnchangedWarning):
        job.for_each_batch(_FakeDF(columns=["id", "__key", "__operation", "__timestamp"]))


def test_stream_batch_recreates_job_inside_worker(monkeypatch):
    job = MagicMock()
    get_job_module = importlib.import_module("fabricks.core.jobs.get_job")
    calls = []
    monkeypatch.setattr(get_job_module, "get_job_internal", lambda **kwargs: calls.append(kwargs) or job)
    df = MagicMock()
    conf = {"step": "bronze", "topic": "queen", "item": "scd1"}

    _for_each_stream_batch(df, 7, step="bronze", topic="queen", item="scd1", schedule="hourly", reload=True, conf=conf)

    assert calls == [{"step": "bronze", "topic": "queen", "item": "scd1", "conf": conf}]
    job._for_each_batch.assert_called_once_with(df, 7, schedule="hourly", reload=True)
