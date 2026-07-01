from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union, cast

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from fabricks.cdc.mixins._protocol import CdcProtocol
from fabricks.cdc.mixins._types import AllowedSources
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.models.cdc import CdcContext
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks
from fabricks.utils.sqlglot import fix as fix_sql


class GeneratorMixin(CdcProtocol):
    def drop(self):
        self.table.drop()

    def create_table(
        self,
        src: AllowedSources,
        context: CdcContext,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str | bool | int]] = None,
        masks: Optional[dict[str, str]] = None,
        primary_key: Optional[dict[str, Any]] = None,
        foreign_keys: Optional[dict[str, Any]] = None,
        generated_columns: Optional[dict[str, str]] = None,
        comments: Optional[dict[str, Any]] = None,
    ):
        context = context.model_copy(
            update={"mode": "complete", "slice": None, "rectify": False, "deduplicate": False}
        )
        df = self.get_data(src, context=context)

        if partitioning is True:
            assert partition_by, "partitioning column(s) not found"

        df = self.reorder_dataframe(df)
        identity = False if identity is None else identity
        liquid_clustering = False if liquid_clustering is None else liquid_clustering
        self.table.create(
            df=df,
            partitioning=partitioning,
            partition_by=partition_by,
            identity=identity,
            liquid_clustering=liquid_clustering,
            cluster_by=cluster_by,
            properties=properties,
            masks=masks,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            generated_columns=generated_columns,
            comments=comments,
        )

    def create_or_replace_view(
        self,
        src: Union[Table, str],
        context: CdcContext,
        schema_evolution: bool = True,
    ):
        assert not isinstance(src, DataFrameLike), "dataframe not allowed"
        assert context.mode == "complete", f"{context.mode} not allowed"
        sql = self.get_query(src, context=context)
        df = self.spark.sql(sql)
        df = self.reorder_dataframe(df)
        columns = backticks(df.columns)
        sql = f"""
        create or replace view {self}
        {"with schema evolution" if schema_evolution else "-- no schema evolution"}
        as
        with __view as (
          {sql}
        )
        select
          {",".join(columns)}
        from __view
        """
        sql = fix_sql(sql)
        DEFAULT_LOGGER.debug("create or replace view", extra={"label": self, "sql": sql})

        try:
            self.spark.sql(sql)
        except Py4JJavaError as e:
            DEFAULT_LOGGER.exception("fail to execute sql query", extra={"label": self, "sql": sql}, exc_info=e)

    def optimize_table(self):
        columns = None

        if self.change_data_capture == "scd1":
            columns = ["__key"]
        elif self.change_data_capture == "scd2":
            columns = ["__key", "__valid_from"]

        self.table.optimize(columns=columns)

    def get_differences_with_deltatable(self, src: AllowedSources, context: CdcContext) -> DataFrame:
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("column", StringType(), False),
                StructField("data_type", StringType(), True),
                StructField("new_column", StringType(), True),
                StructField("new_data_type", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

        if self.is_view:
            return self.spark.createDataFrame([], schema=schema)
        else:
            context = context.model_copy(update={"mode": "complete", "slice": None})
            df = self.get_data(src, context=context)
            df = self.reorder_dataframe(df)
            diffs = self.table.get_schema_differences(df)

            return self.spark.createDataFrame([cast(Any, d.model_dump()) for d in diffs], schema=schema)

    def get_schema_differences(self, src: AllowedSources, context: CdcContext) -> Optional[Sequence[SchemaDiff]]:
        if self.is_view:
            return None
        else:
            context = context.model_copy(update={"mode": "complete", "slice": None})
            df = self.get_data(src, context=context)
            df = self.reorder_dataframe(df)

            return self.table.get_schema_differences(df)

    def schema_drifted(self, src: AllowedSources, context: CdcContext) -> Optional[bool]:
        d = self.get_schema_differences(src, context=context)

        if d is None:
            return None

        return len(d) > 0

    def _update_schema(
        self,
        src: AllowedSources,
        context: CdcContext,
        overwrite: bool = False,
        widen_types: Optional[bool] = False,
    ):
        if self.is_view:
            assert not isinstance(src, DataFrameLike) and not isinstance(src, StructType), (
                "dataframe and structtype not allowed"
            )
            self.create_or_replace_view(src=src, context=CdcContext())
        else:
            context = context.model_copy(update={"mode": "complete", "slice": None})
            df = self.get_data(src, context=context)
            df = self.reorder_dataframe(df)

            if overwrite:
                self.table.overwrite_schema(df)
            else:
                self.table.update_schema(df, widen_types=widen_types)

    def update_schema(self, src: AllowedSources, context: CdcContext, widen_types: Optional[bool] = False):
        if self.schema_drifted(src=src, context=context):
            self._update_schema(src=src, widen_types=widen_types, context=context)

    def overwrite_schema(self, src: AllowedSources, context: CdcContext):
        if self.schema_drifted(src=src, context=context):
            self._update_schema(src=src, overwrite=True, context=context)
