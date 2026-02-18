from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType, VariantType

from fabricks.cdc import NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table


def deploy_tables(drop: bool = False, update: bool = False):
    DEFAULT_LOGGER.info("create or replace fabricks (default) tables", extra={"label": "fabricks"})

    create_table_log(drop=drop, update=update)
    create_table_dummy(drop=drop, update=update)
    create_table_step(drop=drop, update=update)

# TODO: switch to view qnd use fabricks.runtime
def create_table_step(drop: bool = False, update: bool = False):
    table = Table("fabricks", "steps")
    schema = StructType(
        [
            StructField("step", StringType(), True),
            StructField("expand", StringType(), True),
            StructField("order", LongType(), True),
        ]
    )

    if drop:
        table.drop()

    if not table.exists():
        table.create(
            schema=schema,
            partitioning=True,
            partition_by=["expand"],
        )
    elif update:
        table.overwrite_schema(schema=schema)


def create_table_log(drop: bool = False, update: bool = False):
    table = Table("fabricks", "logs")
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
            StructField("json", VariantType(), True),
        ]
    )

    if drop:
        table.drop()

    if not table.exists():
        table.create(
            schema=schema,
            partitioning=True,
            partition_by=["schedule_id", "step"],
        )
    elif update:
        table.overwrite_schema(schema=schema)


def create_table_dummy(drop: bool = False, update: bool = False):
    cdc = NoCDC("fabricks", "dummy")
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
        cdc.drop()

    if not cdc.table.exists():
        cdc.overwrite(df)
    elif update:
        cdc.overwrite_schema(df)
