from logging import ERROR

import pytest

from fabricks.cdc import NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER

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
