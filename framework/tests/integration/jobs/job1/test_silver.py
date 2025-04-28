from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.metastore.table import Table
from tests.integration.compare import compare_silver_to_expected

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(111)
def test_silver_monarch_scd2():
    job = get_job(step="silver", topic="monarch", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=1)


@pytest.mark.order(112)
def test_silver_monarch_scd1():
    job = get_job(step="silver", topic="monarch", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=1)


@pytest.mark.order(111)
def test_silver_regent_scd2():
    job = get_job(step="silver", topic="regent", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=1)


@pytest.mark.order(112)
def test_silver_regent_scd1():
    job = get_job(step="silver", topic="regent", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=1)


@pytest.mark.order(113)
def test_silver_memory_scd2():
    job = get_job(step="silver", topic="memory", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=1)


@pytest.mark.order(114)
def test_silver_memory_scd1():
    job = get_job(step="silver", topic="memory", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=1)


@pytest.mark.order(115)
def test_silver_king_and_queen_scd2():
    job = get_job(step="silver", topic="king_and_queen", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=1)


@pytest.mark.order(116)
def test_silver_king_and_queen_scd1():
    job = get_job(step="silver", topic="king_and_queen", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=1)


@pytest.mark.order(119)
def test_silver_monarch_delta():
    job = get_job(step="silver", topic="monarch", item="delta")
    compare_silver_to_expected(job=job, cdc="scd2", iter=1)

    data_type = (
        SPARK.sql("describe extended silver.monarch_delta")
        .where("col_name == 'decimalField'")
        .select("data_type")
        .collect()[0][0]
    )
    assert data_type == "double", "decimalField is not double"

    cols = Table("silver", "monarch", "delta").columns
    assert "country" in cols, "country not found"


@pytest.mark.order(119)
def test_silver_princess_append():
    df = Table("silver", "princess", "append").dataframe
    assert df.count() == 2


@pytest.mark.order(119)
def test_silver_princess_latest():
    df = Table("silver", "princess", "latest").dataframe
    assert df.count() == 1


@pytest.mark.order(119)
def test_silver_prince_special_char():
    cols = Table("silver", "prince", "special_char").columns
    assert "@Id" in cols, "@Id not found"
    assert "Näàme" in cols, "Näàme not found"
    assert "double Field!" in cols, "double Field! not found"


@pytest.mark.order(119)
def test_silver_princess_extend():
    cols = Table("silver", "princess", "extend").columns
    assert "country" in cols, "country not found"


@pytest.mark.order(119)
def test_silver_princess_order_duplicate():
    order_by = Table("silver", "princess", "order_duplicate").dataframe.select("order_by").collect()[0][0]
    assert order_by == 2, f"order by {order_by} <> 2"


@pytest.mark.order(119)
def test_silver_princess_calculated_column():
    order_by = Table("silver", "princess", "calculated_column").dataframe.select("order_by").collect()[0][0]
    assert order_by == 2, f"order by {order_by} <> 2"


@pytest.mark.order(119)
def test_silver_timeout():
    job = get_job(step="silver", topic="princess", item="calculated_column")
    assert job.timeout == 3600, f"timeout {job.timeout} <> 3600"
