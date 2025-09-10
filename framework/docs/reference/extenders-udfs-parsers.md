# Extenders, UDFs, and Parsers

This reference explains how to extend Fabricks with custom Python code and reusable SQL assets:
- Extenders: Python functions that transform a Spark DataFrame before it is written.
- UDFs: User-defined functions you register on the Spark session and use in SQL.
- Parsers: Source-specific readers/cleaners that return a DataFrame (optional, advanced).

Use these to encapsulate business logic, reuse patterns, and keep SQL jobs focused and readable.

---

## Extenders

Extenders are Python functions that take a DataFrame and return a transformed DataFrame. They are applied during job execution (typically in Silver/Gold) right before write.

- Location: Put Python modules under your runtime, for example `fabricks/extenders`.
- Referencing: In job YAML use `extender: name` and optionally `extender_options: { ... }` to pass arguments.

Example (adds a `country` column):

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from fabricks.core.extenders import extender

@extender(name="add_country")
def add_country(df: DataFrame, **kwargs) -> DataFrame:
    return df.withColumn("country", lit(kwargs.get("country", "Unknown")))
```

In a job:

```yaml
- job:
    step: silver
    topic: sales_analytics
    item: daily_summary
    options:
      mode: update
    extender: add_country
    extender_options:
      country: CH
```

Notes:
- Extenders should be idempotent and fast.
- Avoid heavy I/O; handle source reading in Bronze or via a Parser.
- Prefer small, composable transformations.

---

## UDFs

UDFs are registered on the Spark session and then callable from SQL. Place the UDF registration code in your runtime (e.g., `fabricks/udfs`), and ensure it runs before jobs that need it (for example during initialization or via a notebook hook).

Example (simple addition UDF):

```python
from pyspark.sql import SparkSession
from fabricks.core.udfs import udf

@udf(name="addition")
def addition(spark: SparkSession):
    def _add(a: int, b: int) -> int:
        return a + b
    spark.udf.register("udf_addition", _add)
```

Using built-in helpers and a custom UDF in SQL:

```sql
select
  udf_identity(s.id, id) as __identity,
  udf_key(array(s.id)) as __key,
  s.id as id,
  s.name as monarch,
  udf_addition(1, 2) as addition
from silver.monarch_scd1__current s
```

Notes:
- UDFs should validate inputs and avoid side-effects.
- Prefer Spark SQL built-ins and DataFrame functions where possible for performance.

---

## Parsers

Parsers read and lightly clean raw data. They return a DataFrame and should not write output or mutate state.

What a parser should do:
- Read raw data from `data_path` (batch or stream based on `stream` flag)
- Optionally apply/validate a schema from `schema_path`
- Perform light, source-specific cleanup (drops/renames/type fixes)
- Return a Spark DataFrame (no writes, no side effects)

Inputs:
- `data_path`: source location (directory or file)
- `schema_path`: optional schema location (e.g., JSON/DDL)
- `spark`: active `SparkSession`
- `stream`: when true, prefer `readStream` where supported

Reference in a job:

```yaml
- job:
    step: bronze
    topic: demo
    item: source
    options:
      mode: append
      uri: /mnt/demo/raw/demo
      parser: monarch
    parser_options:
      file_format: parquet
      read_options:
        mergeSchema: true
```

Example implementation:

```python
from pyspark.sql import DataFrame, SparkSession
from fabricks.core.parsers import parser
from fabricks.utils.path import Path

@parser(name="monarch")
class MonarchParser:
    def parse(self, data_path: Path, schema_path: Path, spark: SparkSession, stream: bool) -> DataFrame:
        df = spark.read.parquet(data_path.string)
        cols_to_drop = [c for c in df.columns if c.startswith("BEL_")]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df
```

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from fabricks.core.extenders import extender

@extender(name="add_country")
def add_country(df: DataFrame, **kwargs) -> DataFrame:
    return df.withColumn("country", lit(kwargs.get("country")))
```

---

## Related

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Reference: [Checks & Data Quality](checks-data-quality.md) • [Table Options](table-options.md)
- Runtime: [Runtime Configuration](../helpers/runtime.md)
