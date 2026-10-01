"""Small builders and readers shared by the focused SCD1/SCD2 apache tests."""

from pyspark.sql import DataFrame, SparkSession

_TYPES = {"id": "int", "region": "string", "name": "string", "__operation": "string", "__timestamp": "string"}
COLUMNS = ["id", "name", "__operation", "__timestamp"]
FAR_FUTURE = "9999-12-31 00:00:00"


def batch(spark: SparkSession, *rows: tuple, columns: list[str] = COLUMNS) -> DataFrame:
    """Typed so that None cells and empty batches keep a schema."""
    return spark.createDataFrame(list(rows), ", ".join(f"{c} {_TYPES[c]}" for c in columns))


def empty_like(spark: SparkSession, frame: DataFrame) -> DataFrame:
    return spark.createDataFrame([], frame.schema)


def rows(table, *columns: str) -> list[tuple]:
    """The table's rows as sorted tuples of the requested columns (None stays None)."""
    found = [tuple(r[c] for c in columns) for r in table.dataframe.collect()]
    return sorted(found, key=lambda row: tuple("" if v is None else str(v) for v in row))


def history(table, key: int) -> list[tuple]:
    """(name, valid_from, valid_to, is_current) of one key, oldest version first, timestamps as strings."""
    found = [
        (r["name"], str(r["__valid_from"]), str(r["__valid_to"]), r["__is_current"])
        for r in table.dataframe.where(f"id = {key}").collect()
    ]
    return sorted(found, key=lambda row: row[1])
