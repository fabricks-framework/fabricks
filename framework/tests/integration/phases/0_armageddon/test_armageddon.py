import pytest

from fabricks.context import SPARK
from fabricks.utils.helpers import run_notebook
from tests.integration.helpers.const import ROOT


@pytest.mark.order(1)
def test_armageddon():
    with pytest.raises(Exception):
        run_notebook(ROOT.joinpath("armageddon"))


@pytest.mark.order(2)
def test_armageddon_output():
    df = SPARK.sql("select * from fabricks.dbojects where exists")
    assert df.count() == 96, "armageddon should have created 96 tables and views"
