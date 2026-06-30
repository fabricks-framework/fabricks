import pytest

from fabricks.context import SPARK


@pytest.mark.order(101)
def test_armageddon():
    df = SPARK.sql("select * from fabricks.dbojects where exists")
    assert df.count() == 128, "armageddon should have created 128 tables and views"
