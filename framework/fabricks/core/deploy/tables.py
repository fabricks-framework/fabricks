from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from fabricks.cdc import NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table


def deploy_tables(drop: bool = False):
    DEFAULT_LOGGER.info("🌟 (create or replace tables)")

    create_table_log(drop)
    create_table_dummy(drop)
    create_table_step(drop)


def create_table_step(drop: bool = False):
    table = Table("fabricks", "steps")
    if drop:
        table.drop()

    if not table.exists():
        schema = StructType(
            [
                StructField("step", StringType(), True),
                StructField("expand", StringType(), True),
                StructField("order", LongType(), True),
            ]
        )
        table.create(schema=schema, partitioning=True, partition_by=["expand"])


def create_table_log(drop: bool = False):
    table = Table("fabricks", "logs")
    if drop:
        table.drop()

    if not table.exists():
        schema = StructType(
            [
                StructField("schedule_id", StringType(), True),
                StructField("schedule", StringType(), True),
                StructField("step", StringType(), True),
                StructField("job_id", StringType(), True),
                StructField("job", StringType(), True),
                StructField("notebook_id", StringType(), True),
                StructField("level", StringType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField(
                    "exception",
                    StructType(
                        [
                            StructField("type", StringType(), True),
                            StructField("message", StringType(), True),
                            StructField("traceback", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        table.create(schema=schema, partitioning=True, partition_by=["schedule_id", "step"])


def create_table_dummy(drop: bool = False):
    table = NoCDC("fabricks", "dummy")
    df = SPARK.sql(
        """
          select
          1 as __key,
          md5('1') as __hash,
          cast('1900-01-01' as timestamp) as __valid_from,
          cast('9999-12-31' as timestamp) as __valid_to
        """
    )
    if drop:
        table.drop()
    table.overwrite(df)
