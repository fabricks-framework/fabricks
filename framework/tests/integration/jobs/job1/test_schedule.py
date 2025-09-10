from logging import ERROR

import pytest

from fabricks.context import PATH_RUNTIME, SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.utils.helpers import run_notebook

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(102)
def test_schedule():
    t = Table("silver", "princess", "drop")
    t.drop()

    try:
        run_notebook(PATH_RUNTIME.parent().joinpath("schedule"), schedule="test")
        assert False  # notebook should fail
    except Exception:
        assert True


@pytest.mark.order(103)
def test_no_unforced_failure():
    df = SPARK.sql(
        """
        select
          *
        from
          fabricks.last_schedule l
        where
          true
          and l.failed
          and l.job not in (
            "silver.princess_drop", 
            "gold.invoke_failed_pre_run", 
            "gold.invoke_notebooks_failed_pre_run", 
            "gold.invoke_timedout",
            "gold.invoke_timedout_pre_run",
            "gold.check_fail",
            "gold.check_max_rows",
            "gold.check_min_rows",
            "gold.check_count_must_equal",
            "gold.check_duplicate_key",
            "gold.check_duplicate_identity"
          )
        """
    )
    assert df.count() == 0, "unforced failure <> 0"


@pytest.mark.order(103)
def test_forced_failures():
    df = SPARK.sql(
        """
        select
          *
        from
          fabricks.last_schedule l
        where
          true
          and l.failed
          and l.job in (
            "silver.princess_drop", 
            "gold.invoke_failed_pre_run", 
            "gold.invoke_notebooks_failed_pre_run", 
            "gold.invoke_timedout",
            "gold.invoke_timedout_pre_run",
            "gold.check_fail",
            "gold.check_max_rows",
            "gold.check_min_rows",
            "gold.check_count_must_equal",
            "gold.check_duplicate_key",
            "gold.check_duplicate_identity"
          )
        """
    )
    assert df.count() == 11, "forced failures <> 11"
