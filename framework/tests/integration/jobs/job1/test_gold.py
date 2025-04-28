from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.metastore.table import Table
from tests.integration.compare import compare_gold_to_expected

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(121)
def test_gold_scd1_complete():
    j = get_job(step="gold", topic="scd1", item="complete")
    compare_gold_to_expected(j, "scd1", 1, where="__is_current")


@pytest.mark.order(122)
def test_gold_scd1_update():
    j = get_job(step="gold", topic="scd1", item="update")
    compare_gold_to_expected(j, "scd1", 1, where="__is_current")


@pytest.mark.order(123)
def test_gold_scd1_identity():
    j = get_job(step="gold", topic="scd1", item="identity")
    compare_gold_to_expected(j, "scd1", 1, where="__is_current")


@pytest.mark.order(123)
def test_gold_scd2_complete():
    j = get_job(step="gold", topic="scd2", item="complete")
    compare_gold_to_expected(j, "scd2", 1)


@pytest.mark.order(124)
def test_gold_scd2_update():
    j = get_job(step="gold", topic="scd2", item="update")
    compare_gold_to_expected(j, "scd2", 1)


@pytest.mark.order(125)
def test_gold_scd2_correct_valid_from():
    df = SPARK.sql("select min(`__valid_from`) <> '1900-01-01' from gold.scd2_correct_valid_from")
    check = df.collect()[0][0]
    assert check, "min __valid_from is 1900-01-01"


@pytest.mark.order(129)
def test_gold_fact_udf():
    addition = SPARK.sql("select addition from gold.fact_udf").collect()[0][0]
    assert addition == "3", f"{addition} <> 3"

    # phone_number = spark.sql("select phone_number from gold.fact_udf").collect()[0][0]
    # assert phone_number.clean_phone_nr == "+32478478478", f"{phone_number} <> +32478478478"


@pytest.mark.order(129)
def test_gold_fact_memory():
    columns = SPARK.sql("select * from gold.fact_memory").columns
    assert "__it_should_not_be_found" not in columns, "__it_should_not_be_found found"


@pytest.mark.order(129)
def test_gold_fact_order_duplicate():
    df = Table("gold", "fact", "order_duplicate").dataframe
    assert df.count() == 1, f"rows {df.count()} <> 1"
    dummy = df.select("dummy").collect()[0][0]
    assert dummy == 2, f"dummy {dummy} <> 2"


@pytest.mark.order(129)
def test_gold_fact_deduplicate():
    df = Table("gold", "fact", "deduplicate").dataframe
    assert df.count() == 1, f"rows {df.count()} <> 1"


@pytest.mark.order(129)
def test_gold_fact_manual():
    j = get_job(step="gold", topic="fact", item="manual")
    assert j.table.rows == 0, "table not empty"


@pytest.mark.order(129)
def test_gold_dim_identity():
    j = get_job(step="gold", topic="dim", item="identity")
    assert "__identity" in j.table.dataframe.columns, "__identity not found"
    table_features = j.table.describe_detail().select("tableFeatures").collect()[0][0]
    assert "identityColumns" in table_features


@pytest.mark.order(129)
def test_gold_dim_date():
    j = get_job(step="gold", topic="dim", item="date")
    assert "__identity" in j.table.dataframe.columns, "__identity not found"
    table_features = j.table.describe_detail().select("tableFeatures").collect()[0][0]
    assert "identityColumns" not in table_features


@pytest.mark.order(129)
def test_gold_fact_option():
    j = get_job(step="gold", topic="fact", item="option")

    assert "__identity" in j.table.columns, "__identity not found"

    country = j.table.get_property("country")
    assert country, "country not found"
    assert country == "Belgium", "country is not Belgium"

    clusters = j.table.describe_detail().collect()[0].clusteringColumns
    assert "monarch" in clusters, "cluster not found"

    comment = j.table.describe_detail().collect()[0].description
    assert comment == "Strength lies in unity", "comment not found"

    # spark options
    optimize_write = j.table.get_property("delta.autoOptimize.optimizeWrite")
    assert optimize_write, "optimizeWrite not found"
    assert optimize_write.lower() == "false", "optimizeWrite enabled"

    change_data_feed = j.table.get_property("delta.enableChangeDataFeed")
    assert change_data_feed, "enableChangeDataFeed not found"
    assert change_data_feed.lower() == "true", "enableChangeDataFeed not enabled"

    assert j.timeout == 1800, f"timeout {j.timeout} <> 1800"


@pytest.mark.order(129)
def test_gold_scd1_last_timestamp():
    last_timestamp = SPARK.sql("select * from gold.scd1_last_timestamp__last_timestamp").collect()[0][0]
    assert last_timestamp is None
