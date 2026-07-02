from typing import Literal

import pytest

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.models.cdc import CdcContext
from tests.integration.compare import assert_dfs_equal

CDC = {"scd1": SCD1, "scd2": SCD2}


@pytest.mark.integration
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
@pytest.mark.parametrize("iter", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_cdc_isolated(cdc: Literal["scd1", "scd2", "latest"], iter: int | list[int] = 1):
    if isinstance(iter, int):
        iter = [iter]

    iter_str = [str(i) for i in iter]
    last_iter = iter[-1]
    topic = "monarch"
    tgt: SCD1 | SCD2 = CDC[cdc]("test", f"{topic}_{cdc}_{'_'.join(iter_str)}")
    expected = f"expected.silver_{cdc}_job{last_iter}"
    DEFAULT_LOGGER.info(f"comparing to {expected}")
    x = 0

    for i in iter_str:
        view = f"input.monarch_job{i}"
        query = f"select * from {view}"
        context = CdcContext(keys=["id"], schema_drift=True)

        if cdc == "scd2":
            context.correct_valid_from = True

        if x == 0:
            tgt.drop()
            tgt.create_table(query, context=context)

        tgt.update(query, context=context)  # mirrors silver.py mode="update"
        x += 1

    expected_df = SPARK.table(expected).drop("__source")
    assert_dfs_equal(tgt.table.dataframe, expected_df, soft_delete=False)


# TODO: add check for columns
@pytest.mark.order(171)
def test_gold_nocdc_overwrite():
    df = SPARK.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "overwrite")
    nocdc.overwrite(df, context=CdcContext())
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df, context=CdcContext())
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(172)
def test_gold_nocdc_append():
    df = SPARK.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "append")
    nocdc.append(df, context=CdcContext())
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df, context=CdcContext())
    assert nocdc.table.dataframe.count() == 2
