import pytest

from fabricks.cdc import NoCDC


@pytest.mark.order(1)
def test_gold_nocdc_overwrite(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "overwrite", spark=local_spark)

    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(2)
def test_gold_nocdc_append(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "append", spark=local_spark)

    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 2
