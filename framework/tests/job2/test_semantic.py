from logging import ERROR

import pytest

from framework.fabricks.context.log import Logger
from framework.fabricks.metastore.table import Table

Logger.setLevel(ERROR)


@pytest.mark.order(231)
def test_job2_semantic_fact_schema_drift():
    df = Table("semantic", "fact", "schema_drift").dataframe
    assert "newField" in df.columns, "newField not found"
