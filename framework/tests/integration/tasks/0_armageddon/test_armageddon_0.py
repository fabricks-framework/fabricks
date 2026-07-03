import pytest

from fabricks.context import SPARK
from fabricks.deploy import Deploy
from tests.integration._types import steps


@pytest.mark.order(1)
def test_armageddon():
    with pytest.raises(Exception):
        Deploy.armageddon(steps=steps, nowait=True, mode="parallel", deploy_notebooks=False)  # why wait ?


@pytest.mark.order(2)
def test_armageddon_output():
    df = SPARK.sql("select * from fabricks.dbojects where exists")
    assert df.count() == 96, "armageddon should have created 96 tables and views"
