from logging import ERROR

import pytest
from databricks.sdk.runtime import spark

from framework.fabricks.context import PATH_RUNTIME
from framework.fabricks.context.log import Logger
from framework.fabricks.metastore.table import Table
from framework.fabricks.utils.helpers import run_notebook

Logger.setLevel(ERROR)


@pytest.mark.order(102)
def test_job1_schedule():
    table = Table("silver", "princess", "drop")
    table.drop()

    try:
        run_notebook(PATH_RUNTIME.parent().join("schedule"), schedule="test")
        assert False  # notebook should fail
    except Exception:
        assert True


@pytest.mark.order(103)
def test_job1_no_unexpected_failure():
    df = spark.sql(
        """
        select
          *
        from
          fabricks.last_schedule l
        where
          true
          and l.failed
          and l.topic not in ("check")
          and l.job not in ("silver.princess_drop", "gold.invoke_failed_pre_run")
        """
    )
    assert df.count() == 0, "unforced failure <> 0"
