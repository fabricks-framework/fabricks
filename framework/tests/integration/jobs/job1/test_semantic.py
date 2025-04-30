from logging import ERROR

import pytest
from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(139)
def test_semantic_fact_table():
    j = get_job(step="semantic", topic="fact", item="table")
    assert j.table.exists(), f"{j} not found"
    cols = [c for c in j.table.columns if c.startswith("__")]
    assert "__metadata" in cols, "__metadata not found"
    assert len(cols) == 1, "__ col found"


@pytest.mark.order(139)
def test_semantic_fact_step_option():
    j = get_job(step="semantic", topic="fact", item="step_option")
    props_df = j.table.show_properties()
    assert props_df.where("key == 'delta.minReaderVersion'").select("value").collect()[0][0] == "1", (
        "minReaderVersion <> 1"
    )
    assert props_df.where("key == 'delta.minWriterVersion'").select("value").collect()[0][0] == "7", (
        "minWriterVersion <> 7"
    )
    assert props_df.where("key == 'delta.targetFileSize'").select("value").collect()[0][0] == "1073741824", (
        "delta.targetFileSize <> 1073741824"
    )
    assert props_df.where("key == 'delta.tuneFileSizesForRewrites'").select("value").collect()[0][0] == "False", (
        "delta.tuneFileSizesForRewrites <> False"
    )
    assert props_df.where("key == 'delta.columnMapping.mode'").select("value").collect()[0][0] == "none", (
        "delta.columnMapping.mode <> none"
    )


@pytest.mark.order(139)
def test_semantic_fact_job_option():
    j = get_job(step="semantic", topic="fact", item="job_option")
    props_df = j.table.show_properties()
    assert props_df.where("key == 'delta.minReaderVersion'").select("value").collect()[0][0] == "2", (
        "minReaderVersion <> 2"
    )
    assert props_df.where("key == 'delta.minWriterVersion'").select("value").collect()[0][0] == "5", (
        "minWriterVersion <> 5"
    )
    assert props_df.where("key == 'delta.columnMapping.mode'").select("value").collect()[0][0] == "none", (
        "delta.columnMapping.mode <> none"
    )


@pytest.mark.order(139)
def test_semantic_fact_zstd():
    j = get_job(step="semantic", topic="fact", item="zstd")
    parquet = [f for f in dbutils.fs.ls(j.table.delta_path.string) if f.size > 0][0]
    file_name = parquet.name
    assert "zstd" in file_name, "codec <> zstd"


@pytest.mark.order(139)
def test_semantic_fact_powerbi():
    j = get_job(step="semantic", topic="fact", item="powerbi")
    props_df = j.table.show_properties()
    assert props_df.where("key == 'delta.minReaderVersion'").select("value").collect()[0][0] == "2", (
        "minReaderVersion <> 2"
    )
    assert props_df.where("key == 'delta.minWriterVersion'").select("value").collect()[0][0] == "5", (
        "minWriterVersion <> 5"
    )
