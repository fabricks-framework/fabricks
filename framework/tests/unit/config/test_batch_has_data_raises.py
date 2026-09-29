from unittest.mock import MagicMock

import pytest

from fabricks.core.jobs.base.exception import UnchangedWarning
from fabricks.core.jobs.silver import Silver
from tests.unit.config._helpers import _FakeDF


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
