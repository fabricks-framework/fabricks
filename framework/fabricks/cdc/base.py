from typing import Optional

from pyspark.sql import SparkSession

from fabricks.cdc.cdc_abc import CDCAbstract
from fabricks.cdc.mixins.configurator import ConfiguratorMixin
from fabricks.cdc.mixins.generator import GeneratorMixin
from fabricks.cdc.mixins.merger import MergerMixin
from fabricks.cdc.mixins.processor import ProcessorMixin
from fabricks.context import SPARK
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table


class BaseCDC(MergerMixin, ProcessorMixin, GeneratorMixin, ConfiguratorMixin, CDCAbstract):
    def __init__(
        self,
        database: str,
        *levels: str,
        change_data_capture: str,
        spark: Optional[SparkSession] = None,
    ):
        if spark is None:
            spark = SPARK

        assert spark is not None
        self.spark: SparkSession = spark
        self.database = Database(database)
        self.levels = levels
        self.change_data_capture = change_data_capture
        self.table = Table(self.database.name, *self.levels, spark=self.spark)
