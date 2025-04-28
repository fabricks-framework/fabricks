import os
import sys
from logging import ERROR
from pathlib import Path

import pytest

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

root = p.absolute()

if str(root) not in sys.path:
    sys.path.append(str(root))


from fabricks.context import PATH_RUNTIME, SPARK  # noqa: E402
from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402
from fabricks.metastore.table import Table  # noqa: E402
from fabricks.utils.helpers import run_notebook  # noqa: E402

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
