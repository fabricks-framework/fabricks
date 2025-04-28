from logging import ERROR

import pytest

from fabricks.context import PATH_RUNTIME, SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.utils.helpers import run_notebook

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(102)
def test_schedule():
    table = Table("silver", "princess", "drop")
    table.drop()

    try:
        run_notebook(PATH_RUNTIME.parent().join("schedule"), schedule="test")
        assert False  # notebook should fail
    except Exception:
        assert True


@pytest.mark.order(103)
def test_no_unexpected_failure():
    df = SPARK.sql(
        """
        select
          *
        from
          fabricks.last_schedule l
        where
          true
          and l.failed
          and l.topic not in ("check")
          and l.job not in (
            "silver.princess_drop", 
            "gold.invoke_failed_pre_run", 
            "gold.invoke_timedout",
            "gold.invoke_timedout_pre_run"
          )
        """
    )
    assert df.count() == 0, "unforced failure <> 0"
