from pathlib import Path

import pyarrow.parquet as parquet

from fabricks.core import get_job


def _run(local_spark, item: str):
    job = get_job(step="semantic", topic="fact", item=item)
    source = job.get_data(stream=False)
    assert source is not None
    job.create()
    job.for_each_batch(source)
    return job


def _properties(job) -> dict[str, str]:
    return {row.key: row.value for row in job.table.get_properties().collect()}


def test_semantic_table_materializes_rows_and_metadata(local_spark):
    job = _run(local_spark, "table")

    rows = job.table.dataframe.select("Monarch_ID", "Monarch").orderBy("Monarch_ID").collect()
    assert rows == [(1, "king"), (2, "queen")]
    assert "__metadata" in job.table.columns


def test_semantic_step_properties_are_materialized(local_spark):
    properties = _properties(_run(local_spark, "step_option"))

    assert properties["delta.checkpointInterval"] == "11"


def test_semantic_job_properties_override_step(local_spark):
    properties = _properties(_run(local_spark, "job_option"))

    assert properties["delta.checkpointInterval"] == "7"


def test_semantic_partitioning_is_physical(local_spark):
    job = _run(local_spark, "partitioning")
    partitions = {row[0] for row in local_spark.sql(f"show partitions {job.table.qualified_name}").collect()}

    assert partitions == {"king", "queen"}


def test_semantic_zstd_is_physical(local_spark):
    previous = local_spark.conf.get("spark.sql.parquet.compression.codec")
    local_spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    try:
        job = _run(local_spark, "zstd")
    finally:
        local_spark.conf.set("spark.sql.parquet.compression.codec", previous)

    data_file = next(Path(str(job.table.delta_path)).rglob("*.parquet"))
    metadata = parquet.ParquetFile(data_file).metadata
    codecs = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(group).num_columns)
    }
    assert codecs == {"ZSTD"}


def test_semantic_powerbi_properties_are_materialized(local_spark):
    job = _run(local_spark, "powerbi")
    properties = _properties(job)

    assert properties["delta.columnMapping.mode"] == "name"
    assert properties["delta.minReaderVersion"] == "2"
    assert properties["delta.minWriterVersion"] == "5"
    assert {row[0] for row in local_spark.sql(f"show partitions {job.table.qualified_name}").collect()} == {
        "king",
        "queen",
    }
