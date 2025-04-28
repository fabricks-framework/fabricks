from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(231)
def test_semantic_fact_schema_drift():
    df = Table("semantic", "fact", "schema_drift").dataframe
    assert "newField" in df.columns, "newField not found"
