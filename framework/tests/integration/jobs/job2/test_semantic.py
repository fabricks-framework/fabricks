import os
import sys
from logging import ERROR
from pathlib import Path

import pytest

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

root = p.absolute()

if str(root) not in sys.path:
    sys.path.append(str(root))


from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402
from fabricks.metastore.table import Table  # noqa: E402

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(231)
def test_semantic_fact_schema_drift():
    df = Table("semantic", "fact", "schema_drift").dataframe
    assert "newField" in df.columns, "newField not found"
