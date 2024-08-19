from logging import ERROR

import pytest
from databricks.sdk.runtime import spark

from fabricks.cdc import NoCDC
from fabricks.context.log import Logger

Logger.setLevel(ERROR)


# TODO: add check for columns
@pytest.mark.order(171)
def test_job1_gold_nocdc_overwrite():
    df = spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "overwrite")

    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(172)
def test_job1_gold_nocdc_append():
    df = spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "append")

    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 2
