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

from fabricks.cdc import NoCDC  # noqa: E402
from fabricks.context import SPARK  # noqa: E402
from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402

DEFAULT_LOGGER.setLevel(ERROR)


# TODO: add check for columns
@pytest.mark.order(171)
def test_gold_nocdc_overwrite():
    df = SPARK.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "overwrite")

    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(172)
def test_gold_nocdc_append():
    df = SPARK.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "append")

    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 2
