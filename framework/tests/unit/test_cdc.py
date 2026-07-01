import pytest

from fabricks.cdc import SCD1, SCD2
from fabricks.context import SPARK
from fabricks.metastore.database import Database
from fabricks.models.cdc import CdcContext
from tests.integration.compare import assert_dfs_equal
from tests.integration.utils import create_input_views

CDC = {"scd1": SCD1, "scd2": SCD2}


@pytest.mark.integration
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
def test_cdc_isolated(cdc):
    topic = "monarch"
    create_input_views([topic])  # input.monarch_jobN (cumulative, __job tags each row)
    Database("test").create()
    tgt = CDC[cdc]("test", f"{topic}_{cdc}")
    tgt.drop()

    for i in range(1, 12):
        view = f"input.{topic}_job{i}"

        if not SPARK.catalog.tableExists(view):
            continue

        # ponytail: input views are cumulative -> take only this job's rows, mirroring the
        # per-batch increment the real Silver job feeds. Drop the filter if you want snapshot/complete.
        batch = f"select * from {view} where __job = 'job{i}'"

        if i == 1:
            tgt.create_table(batch, CdcContext())

        tgt.update(batch, CdcContext())  # mirrors silver.py mode="update"
        expected = SPARK.table(f"expected.silver_{cdc}_job{i}").drop("__source")
        assert_dfs_equal(tgt.table.dataframe, expected)
