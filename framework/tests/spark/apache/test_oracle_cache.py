"""The oracle cache must follow the oracle data: an edited source file may never be served from an old copy."""

from tests.spark.expected.compare import _cached_oracle


def _oracle(spark, value: str):
    return spark.sql(f"select 1 as id, '{value}' as name")


def test_cached_oracle_reuses_the_copy_for_unchanged_data(local_spark, tmp_path):
    source = tmp_path / "iter1.jsonl"
    source.write_text('{"id": 1}')

    first = _cached_oracle(_oracle(local_spark, "a"), source, tmp_path / "cache", "scd2_iter1")
    second = _cached_oracle(_oracle(local_spark, "a"), source, tmp_path / "cache", "scd2_iter1")

    assert first == second
    assert local_spark.read.format("delta").load(str(first)).collect()[0].name == "a"


def test_cached_oracle_misses_when_the_source_file_changes(local_spark, tmp_path):
    source = tmp_path / "iter1.jsonl"
    source.write_text('{"id": 1}')
    old = _cached_oracle(_oracle(local_spark, "a"), source, tmp_path / "cache", "scd2_iter1")

    source.write_text('{"id": 1, "name": "edited"}')
    new = _cached_oracle(_oracle(local_spark, "edited"), source, tmp_path / "cache", "scd2_iter1")

    assert new != old
    assert local_spark.read.format("delta").load(str(new)).collect()[0].name == "edited"


def test_cached_oracle_leaves_no_staging_directories_behind(local_spark, tmp_path):
    source = tmp_path / "iter1.jsonl"
    source.write_text('{"id": 1}')

    final = _cached_oracle(_oracle(local_spark, "a"), source, tmp_path / "cache", "scd2_iter1")

    assert [p.name for p in (tmp_path / "cache").iterdir()] == [final.name]
