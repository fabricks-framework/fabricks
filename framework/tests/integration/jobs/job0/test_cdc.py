from typing import Literal

import pytest

from fabricks.cdc import SCD1, SCD2
from fabricks.context import SPARK
from fabricks.models.cdc import CdcContext
from tests.integration.compare import assert_dfs_equal

CDC = {"scd1": SCD1, "scd2": SCD2}


@pytest.mark.integration
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
@pytest.mark.parametrize("iter", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_cdc_isolated(cdc: Literal["scd1", "scd2", "latest"], iter: int):
    topic = "monarch"
    tgt: SCD1 | SCD2 = CDC[cdc]("test", f"{topic}_{cdc}_{iter}")
    # tgt.drop()
    view = f"input.{topic}_job{iter}"

    if not SPARK.catalog.tableExists(view):
        raise Exception(f"table {view} does not exist")

    query = f"select * from {view}"
    context = CdcContext(keys=["id"])
    tgt.create_table(query, context=context)
    tgt.update(query, context=context)  # mirrors silver.py mode="update"
    expected = SPARK.table(f"expected.silver_{cdc}_job{iter}").drop("__source")
    assert_dfs_equal(tgt.table.dataframe, expected, soft_delete=False)
