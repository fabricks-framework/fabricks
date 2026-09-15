from unittest.mock import sentinel

from fabricks.core import get_job


def test_gold_table_option_reads_configured_table():
    job = get_job(step="gold", topic="fact", item="table_option")
    read_table = job.spark.read.table
    read_table.reset_mock()
    read_table.return_value = sentinel.dataframe

    assert job.get_data() is sentinel.dataframe
    read_table.assert_called_once_with("gold.table_option_source")
