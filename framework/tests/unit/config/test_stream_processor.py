import importlib
from unittest.mock import MagicMock

from fabricks.core.jobs.base.processor import _for_each_stream_batch


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
