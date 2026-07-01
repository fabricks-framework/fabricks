from typing import Literal

import pytest

from fabricks.cdc import SCD1, SCD2
from fabricks.context import SPARK
from fabricks.models.cdc import CdcContext
from tests.integration.compare import assert_dfs_equal

CDC = {"scd1": SCD1, "scd2": SCD2}


@pytest.mark.integration
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
def test_cdc_isolated(cdc: Literal["scd1", "scd2", "latest"]):
    topic = "monarch"

    for i in range(1, 12):
        tgt: SCD1 | SCD2 = CDC[cdc]("test", f"{topic}_{cdc}")
        tgt.drop()

        view = f"input.{topic}_job{i}"

        if not SPARK.catalog.tableExists(view):
            continue

        batch = f"select * from {view}"

        if i == 1:
            tgt.create_table(batch, CdcContext())

        tgt.update(batch, CdcContext())  # mirrors silver.py mode="update"
        expected = SPARK.table(f"expected.silver_{cdc}_job{i}").drop("__source")
        assert_dfs_equal(tgt.table.dataframe, expected)
