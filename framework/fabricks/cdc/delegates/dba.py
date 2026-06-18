from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Union, cast

from py4j.protocol import Py4JJavaError
from pyspark.sql.types import StringType, StructField, StructType

from fabricks.cdc.config import AllowedSources
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import SchemaDiff
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks

if TYPE_CHECKING:
    from fabricks.cdc.base import BaseCDC


class CDCDba:
    """Owns all DDL and schema management for a CDC instance.

    Covers the full table lifecycle (create / drop / view), schema
    evolution (update / overwrite / drift detection), and optimization.
    Mirrors the role of JobDBA in the job layer.
    """

    def __init__(self, cdc: BaseCDC):
        self._cdc = cdc

    def drop(self):
        self._cdc.table.drop()

    def create_table(
        self,
        src: AllowedSources,
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
        **kwargs,
    ):
        cdc = self._cdc
        kwargs["mode"] = "complete"
        kwargs["slice"] = False
        kwargs["rectify"] = False
        kwargs["deduplicate"] = False

        df = cdc.get_data(src, **kwargs)

        if partitioning is True:
            assert partition_by, "partitioning column(s) not found"

        df = cdc.reorder_dataframe(df)

        identity = False if identity is None else identity
        liquid_clustering = False if liquid_clustering is None else liquid_clustering

        cdc.table.create(
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

    def create_or_replace_view(self, src: Union[Any, str], schema_evolution: bool = True, **kwargs):
        assert not isinstance(src, DataFrameLike), "dataframe not allowed"
        assert kwargs["mode"] == "complete", f"{kwargs['mode']} not allowed"

        cdc = self._cdc
        sql = cdc.get_query(src, **kwargs)

        df = cdc.spark.sql(sql)
        df = cdc.reorder_dataframe(df)
        columns = backticks(df.columns)

        sql = f"""
        create or replace view {cdc}
        {"with schema evolution" if schema_evolution else "-- no schema evolution"}
        as
        with __view as (
          {sql}
        )
        select
          {",".join(columns)}
        from __view
        """
        sql = cdc.fix_sql(sql)
        DEFAULT_LOGGER.debug("create or replace view", extra={"label": cdc, "sql": sql})

        try:
            cdc.spark.sql(sql)
        except Py4JJavaError as e:
            DEFAULT_LOGGER.exception("fail to execute sql query", extra={"label": cdc, "sql": sql}, exc_info=e)

    def optimize_table(self):
        cdc = self._cdc
        columns = None

        if cdc.change_data_capture == "scd1":
            columns = ["__key"]
        elif cdc.change_data_capture == "scd2":
            columns = ["__key", "__valid_from"]

        cdc.table.optimize(columns=columns)

    def _prepare_complete_df(self, src: AllowedSources, **kwargs):
        kwargs["mode"] = "complete"
        kwargs.pop("slice", None)
        cdc = self._cdc
        df = cdc.get_data(src, **kwargs)
        return cdc.reorder_dataframe(df)

    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs):
        cdc = self._cdc
        schema = StructType(
            [
                StructField("column", StringType(), False),
                StructField("data_type", StringType(), True),
                StructField("new_column", StringType(), True),
                StructField("new_data_type", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

        if cdc.is_view:
            return cdc.spark.createDataFrame([], schema=schema)

        df = self._prepare_complete_df(src, **kwargs)
        diffs = cdc.table.get_schema_differences(df)
        return cdc.spark.createDataFrame([cast(Any, d.model_dump()) for d in diffs], schema=schema)

    def get_schema_differences(self, src: AllowedSources, **kwargs) -> Optional[Sequence[SchemaDiff]]:
        cdc = self._cdc
        if cdc.is_view:
            return None

        df = self._prepare_complete_df(src, **kwargs)
        return cdc.table.get_schema_differences(df)

    def schema_drifted(self, src: AllowedSources, **kwargs) -> Optional[bool]:
        d = self.get_schema_differences(src, **kwargs)
        if d is None:
            return None
        return len(d) > 0

    def _update_schema(
        self,
        src: AllowedSources,
        overwrite: bool = False,
        widen_types: bool = False,
        **kwargs,
    ):
        cdc = self._cdc
        if cdc.is_view:
            assert not isinstance(src, DataFrameLike) and not isinstance(src, StructType), (
                "dataframe and structtype not allowed"
            )
            cdc.create_or_replace_view(src=src)
        else:
            df = self._prepare_complete_df(src, **kwargs)
            if overwrite:
                cdc.table.overwrite_schema(df)
            else:
                cdc.table.update_schema(df, widen_types=widen_types)

    def update_schema(self, src: AllowedSources, **kwargs):
        self._update_schema(src=src, **kwargs)

    def overwrite_schema(self, src: AllowedSources, **kwargs):
        self._update_schema(src=src, overwrite=True, **kwargs)
