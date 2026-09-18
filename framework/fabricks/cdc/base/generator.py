from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from fabricks.cdc.base._types import AllowedSources
from fabricks.cdc.base.configurator import Configurator
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks
from fabricks.utils.sqlglot import fix as fix_sql


class Generator(Configurator):
    def drop(self) -> None:
        self.table.drop()

    def create_table(
        self,
        src: AllowedSources,
        partitioning: bool | None = False,
        partition_by: list[str] | str | None = None,
        identity: bool | None = False,
        liquid_clustering: bool | None = False,
        cluster_by: list[str] | str | None = None,
        properties: dict[str, str | bool | int] | None = None,
        masks: dict[str, str] | None = None,
        primary_key: dict[str, Any] | None = None,
        foreign_keys: dict[str, Any] | None = None,
        generated_columns: dict[str, str] | None = None,
        comments: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
    ) -> None:
        kwargs["mode"] = "complete"
        kwargs["slice"] = False
        kwargs["rectify"] = False
        kwargs["deduplicate"] = False

        df = self.get_data(src, **kwargs)

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
        src: Table | str,
        schema_evolution: bool = True,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
    ) -> None:
        assert not isinstance(src, DataFrameLike), "dataframe not allowed"

        assert kwargs["mode"] == "complete", f"{kwargs['mode']} not allowed"
        sql = self.get_query(src, **kwargs)

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

    def optimize_table(self) -> None:
        columns = None

        if self.change_data_capture == "scd1":
            columns = ["__key"]
        elif self.change_data_capture == "scd2":
            columns = ["__key", "__valid_from"]

        self.table.optimize(columns=columns)

    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs: Any) -> DataFrame:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
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

        kwargs["mode"] = "complete"
        kwargs.pop("slice", None)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        diffs = self.table.get_schema_differences(df)
        return self.spark.createDataFrame([cast(Any, d.model_dump()) for d in diffs], schema=schema)

    def get_schema_differences(self, src: AllowedSources, **kwargs: Any) -> Sequence[SchemaDiff] | None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        if self.is_view:
            return None

        kwargs["mode"] = "complete"
        kwargs.pop("slice", None)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        return self.table.get_schema_differences(df)

    def schema_drifted(self, src: AllowedSources, **kwargs: Any) -> bool | None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        d = self.get_schema_differences(src, **kwargs)
        if d is None:
            return None

        return len(d) > 0

    def _update_schema(
        self,
        src: AllowedSources,
        overwrite: bool = False,
        widen_types: bool = False,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
    ) -> None:
        if self.is_view:
            assert not isinstance(src, DataFrameLike), "dataframe and structtype not allowed"
            assert not isinstance(src, StructType), "dataframe and structtype not allowed"
            self.create_or_replace_view(src=src)

        else:
            kwargs["mode"] = "complete"
            kwargs.pop("slice", None)

            df = self.get_data(src, **kwargs)
            df = self.reorder_dataframe(df)
            if overwrite:
                self.table.overwrite_schema(df)
            else:
                self.table.update_schema(df, widen_types=widen_types)

    def update_schema(self, src: AllowedSources, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        self._update_schema(src=src, **kwargs)

    def overwrite_schema(self, src: AllowedSources, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        self._update_schema(src=src, overwrite=True, **kwargs)
