from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(185)
def test_gold_dim_overwrite():
    j = get_job(step="gold", topic="dim", item="overwrite")
    j.overwrite()

    assert j.table.rows == 2, f"rows 2 <> {j.table.rows}"
    assert "__identity" in j.table.dataframe.columns, "__identity not found"

    table_features = j.table.describe_detail().select("tableFeatures").collect()[0][0]
    assert "identityColumns" in table_features


@pytest.mark.order(185)
def test_gold_fact_overwrite():
    j = get_job(step="gold", topic="fact", item="overwrite")
    j.overwrite()

    assert j.table.rows == 2, f"rows 2 <> {j.table.rows}"
