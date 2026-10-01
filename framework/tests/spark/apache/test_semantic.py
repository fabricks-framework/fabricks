from pathlib import Path

import pyarrow.parquet as parquet


def _run(fresh_job, item: str):
    job = fresh_job("semantic", "fact", item)
    source = job.get_data(stream=False)
    assert source is not None
    job.create()
    job.for_each_batch(source)
    return job


def _properties(job) -> dict[str, str]:
    return {row.key: row.value for row in job.table.get_properties().collect()}


def test_semantic_table_materializes_rows_and_metadata(local_spark, fresh_job):
    job = _run(fresh_job, "table")

    rows = job.table.dataframe.select("Monarch_ID", "Monarch").orderBy("Monarch_ID").collect()
    assert rows == [(1, "king"), (2, "queen")]
    metadata = job.table.dataframe.select("__metadata").orderBy("Monarch_ID").collect()
    assert all(set(row["__metadata"].asDict()) == {"inserted"} for row in metadata)
    assert all(row["__metadata"].inserted is not None for row in metadata)


def test_semantic_schema_drift_updates_the_physical_table(local_spark, fresh_job):
    job = fresh_job("semantic", "fact", "table")
    initial = local_spark.createDataFrame([(1, "king")], ["Monarch_ID", "Monarch"])
    drifted = local_spark.createDataFrame([(1, "king", "Belgium")], ["Monarch_ID", "Monarch", "Realm"])

    job.create()
    job._for_each_batch(initial)
    job._for_each_batch(drifted)

    assert "Realm" in job.table.columns
    assert [(row.Monarch, row.Realm) for row in job.table.dataframe.select("Monarch", "Realm").collect()] == [
        ("king", "Belgium")
    ]


def test_semantic_step_properties_are_materialized(local_spark, fresh_job):
    properties = _properties(_run(fresh_job, "step_option"))

    assert properties["delta.checkpointInterval"] == "11"


def test_semantic_job_properties_override_step(local_spark, fresh_job):
    properties = _properties(_run(fresh_job, "job_option"))

    assert properties["delta.checkpointInterval"] == "7"


def test_semantic_partitioning_is_physical(local_spark, fresh_job):
    job = _run(fresh_job, "partitioning")
    partitions = {row[0] for row in local_spark.sql(f"show partitions {job.table.qualified_name}").collect()}

    assert partitions == {"king", "queen"}


def test_semantic_zstd_is_physical(local_spark, fresh_job):
    job = _run(fresh_job, "zstd")

    data_file = next(Path(str(job.table.delta_path)).rglob("*.parquet"))
    metadata = parquet.ParquetFile(data_file).metadata
    codecs = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(group).num_columns)
    }
    assert codecs == {"ZSTD"}


def test_semantic_powerbi_properties_are_materialized(local_spark, fresh_job):
    job = _run(fresh_job, "powerbi")
    properties = _properties(job)

    assert properties["delta.columnMapping.mode"] == "name"
    assert properties["delta.minReaderVersion"] == "2"
    assert properties["delta.minWriterVersion"] == "5"
    assert {row[0] for row in local_spark.sql(f"show partitions {job.table.qualified_name}").collect()} == {
        "king",
        "queen",
    }
