from __future__ import annotations

from typing import List, Optional, Union

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame

from fabricks.cdc.base.configurator import Configurator
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.utils.sqlglot import fix as fix_sql


class Generator(Configurator):
    def drop(self):
        self.table.drop()

    def create_table(
        self,
        src: Union[DataFrame, Table, str],
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
        **kwargs,
    ):
        kwargs["mode"] = "complete"
        kwargs["slice"] = False
        kwargs["rectify"] = False
        kwargs["deduplicate"] = False
        df = self.get_data(src, **kwargs)

        if liquid_clustering:
            assert cluster_by, "clustering column not found"
        elif partitioning:
            assert partition_by, "partitioning column not found"

        fields = [c for c in df.columns if not c.startswith("__")]
        __leading = [c for c in self.allowed_leading_columns if c in df.columns]
        __trailing = [c for c in self.allowed_trailing_columns if c in df.columns]
        columns = __leading + fields + __trailing

        df = df.select([f"`{c}`" for c in columns])

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
        )

    def create_or_replace_view(self, src: Union[Table, str], schema_evolution: bool = True, **kwargs):
        assert not isinstance(src, (DataFrame, CDataFrame)), "dataframe not allowed"

        assert kwargs["mode"] == "complete", f"{kwargs['mode']} not allowed"
        sql = self.get_query(src, **kwargs)

        df = self.spark.sql(sql)
        df = self.reorder_columns(df)
        columns = [f"`{c}`" for c in df.columns]

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
        DEFAULT_LOGGER.debug("create or replace view", extra={"job": self, "sql": sql})

        try:
            self.spark.sql(sql)
        except Py4JJavaError:
            DEFAULT_LOGGER.exception("🙈", extra={"job": self})

    def optimize_table(self):
        liquid_clustering = self.table.get_property("delta.feature.liquid") == "supported"
        if liquid_clustering:
            self.table.optimize()
        else:
            columns = None
            if self.change_data_capture == "scd1":
                columns = ["__key"]
            elif self.change_data_capture == "scd2":
                columns = ["__key", "__valid_from"]
            vorder = self.table.get_property("delta.parquet.vorder.enabled") or "false"
            vorder = vorder.lower() == "true"
            self.table.optimize(columns=columns, vorder=vorder)

    def update_schema(self, src: Union[DataFrame, Table, str], **kwargs):
        overwrite = kwargs.get("overwrite", False)

        if self.is_view():
            assert not isinstance(src, (DataFrame, CDataFrame)), "dataframe not allowed"
            self.create_or_replace_view(src=src, **kwargs)

        else:
            kwargs["mode"] = "complete"
            df = self.get_data(src, **kwargs)
            if overwrite:
                self.table.overwrite_schema(df)
            else:
                self.table.update_schema(df)

    def overwrite_schema(self, src: Union[DataFrame, Table, str]):
        self.update_schema(src=src, overwrite=True)
