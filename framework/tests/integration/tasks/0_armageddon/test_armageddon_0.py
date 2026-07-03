import pytest

from fabricks.context import SPARK
from fabricks.utils.helpers import run_notebook
from tests.integration._types import PATHS


@pytest.mark.order(1)
def test_armageddon():
    run_notebook(PATHS.root.joinpath("armageddon"))


@pytest.mark.order(2)
def test_armageddon_output():
    df = SPARK.sql("select * from fabricks.dbojects where exists")
    assert df.count() == 96, "armageddon should have created 96 tables and views"
