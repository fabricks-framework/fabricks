import re
from functools import lru_cache
from typing import List, Optional, Union, overload

from delta import DeltaTable
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max
from pyspark.sql.types import StructType
from typing_extensions import deprecated

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.relational import Relational
from fabricks.utils.path import Path
from fabricks.utils.sqlglot import fix


class Table(Relational):
    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str, spark: Optional[SparkSession] = SPARK):
        return cls(step, topic, item, spark=spark)

    @property
    @deprecated("use delta_path instead")
    def deltapath(self) -> Path:
        return self.database.delta_path.join("/".join(self.levels))

    @property
    def delta_path(self) -> Path:
        return self.database.delta_path.join("/".join(self.levels))

    @property
    def deltatable(self) -> DeltaTable:
        return DeltaTable.forPath(self.spark, self.delta_path.string)

    @property
    def delta_table(self) -> DeltaTable:
        return DeltaTable.forPath(self.spark, self.delta_path.string)

    @property
    def dataframe(self) -> DataFrame:
        return self.spark.sql(f"select * from {self}")

    @property
    def columns(self) -> List[str]:
        return self.dataframe.columns

    @property
    def rows(self) -> int:
        return self.spark.sql(f"select count(*) from {self}").collect()[0][0]

    @property
    def last_version(self) -> int:
        df = self.describe_history()
        version = df.select(max("version")).collect()[0][0]
        return version

    @property
    def identity_enabled(self) -> bool:
        return self.get_property("delta.feature.identityColumns") == "supported"

    def drop(self):
        super().drop()
        if self.delta_path.exists():
            DEFAULT_LOGGER.debug("delete delta folder", extra={"job": self})
            self.delta_path.rm()

    @overload
    def create(
        self,
        df: DataFrame,
        *,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
    ): ...

    @overload
    def create(
        self,
        *,
        schema: StructType,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
    ): ...

    def create(
        self,
        df: Optional[DataFrame] = None,
        schema: Optional[StructType] = None,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
    ):
        self._create(
            df=df,
            schema=schema,
            partitioning=partitioning,
            partition_by=partition_by,
            identity=identity,
            liquid_clustering=liquid_clustering,
            cluster_by=cluster_by,
            properties=properties,
        )

    def _create(
        self,
        df: Optional[DataFrame] = None,
        schema: Optional[StructType] = None,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
    ):
        DEFAULT_LOGGER.info("create table", extra={"job": self})
        if not df:
            assert schema is not None
            df = self.spark.createDataFrame([], schema)

        def _backtick(name: str, dtype: str) -> str:
            j = df.schema[name].jsonValue()
            r = re.compile(r"(?<='name': ')[^']+(?=',)")
            names = re.findall(r, str(j))
            for n in names:
                escaped = re.escape(n)
                dtype = re.sub(f"(?<=,){escaped}(?=:)|(?<=<){escaped}(?=:)", f"`{n}`", dtype)
            return dtype

        ddl_columns = ",\n\t".join([f"`{name}` {_backtick(name, dtype)}" for name, dtype in df.dtypes])
        ddl_identity = "-- no identity" if "__identity" not in df.columns else ""
        ddl_cluster_by = "-- no cluster by"
        ddl_partition_by = "-- no partitioned by"
        ddl_tblproperties = "-- not tblproperties"

        if liquid_clustering:
            assert cluster_by
            if isinstance(cluster_by, str):
                cluster_by = [cluster_by]
            cluster_by = [f"`{c}`" for c in cluster_by]
            ddl_cluster_by = "cluster by (" + ", ".join(cluster_by) + ")"

        if partitioning:
            assert partition_by
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            partition_by = [f"`{p}`" for p in partition_by]
            ddl_partition_by = "partitioned by (" + ", ".join(partition_by) + ")"

        if identity:
            ddl_identity = "__identity bigint generated by default as identity (start with 1 increment by 1), "

        if not properties:
            special_char = False

            for c in df.columns:
                match = re.search(r"[^a-zA-Z0-9_]", c)
                if match:
                    special_char = True
                    break

            if special_char:
                properties = {
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                }

        if properties:
            ddl_tblproperties = (
                "tblproperties (" + ",".join(f"'{key}' = '{value}'" for key, value in properties.items()) + ")"
            )

        sql = f""" 
        create table if not exists {self.qualified_name} 
        (
        {ddl_identity}
        {ddl_columns}
        )
        {ddl_tblproperties}
        {ddl_partition_by}
        {ddl_cluster_by}
        location '{self.delta_path}'
        """
        try:
            sql = fix(sql)
        except Exception:
            pass

        DEFAULT_LOGGER.debug("ddl", extra={"job": self, "sql": sql})
        self.spark.sql(sql)

    def is_deltatable(self) -> bool:
        return DeltaTable.isDeltaTable(self.spark, str(self.delta_path))

    @property
    def column_mapping_enabled(self) -> bool:
        return self.get_property("delta.columnMapping.mode") == "name"

    def exists(self) -> bool:
        return self.is_deltatable() and self.registered()

    def register(self):
        DEFAULT_LOGGER.debug("register table", extra={"job": self})
        self.spark.sql(f"create table if not exists {self.qualified_name} using delta location '{self.delta_path}'")

    def restore_to_version(self, version: int):
        DEFAULT_LOGGER.info(f"restore table to version {version}", extra={"job": self})
        self.spark.sql(f"restore table {self.qualified_name} to version as of {version}")

    def truncate(self):
        DEFAULT_LOGGER.warning("truncate table", extra={"job": self})
        self.create_restore_point()
        self.spark.sql(f"truncate table {self.qualified_name}")

    def schema_drifted(self, df: DataFrame, exclude_columns_with_prefix: Optional[str] = None) -> bool:
        df = self.get_differences_with_dataframe(df)
        return not df.isEmpty()

    @lru_cache(maxsize=None)
    def get_differences_with_dataframe(self, df: DataFrame) -> DataFrame:
        df1 = self.dataframe
        if self.identity_enabled:
            if "__identity" in df1.columns:
                df1 = df1.drop("__identity")

        df1 = self.spark.createDataFrame(df1.dtypes, ["column", "data_type"])  # type: ignore
        df2 = self.spark.createDataFrame(df.dtypes, ["column", "data_type"])  # type: ignore

        df = self.spark.sql(
            """
            with base as (
            select
              d1.column,
              d1.data_type,
              d2.column as new_column,
              d2.data_type as new_data_type
            from
              {df1} d1 full
              outer join {df2} d2 on d1.column = d2.column
            )
            select
              coalesce(`column`, `new_column`) as `column`,
              * except(`column`, `new_column`),
              if(`new_column` is null, 'dropped', if(`column` is null, 'added', 'changed')) as status
            from
              base
            where
              false
              or column is null
              or new_column is null
              or not data_type <=> new_data_type
            """,
            df1=df1,
            df2=df2,
        )

        if not df.isEmpty():
            DEFAULT_LOGGER.debug("difference(s) with delta table", extra={"job": self, "df": df})
        return df

    def update_schema(self, df: DataFrame):
        diff_df = self.get_differences_with_dataframe(df)
        diff_df = diff_df.where("status in ('added', 'changed')")

        if not diff_df.isEmpty():
            DEFAULT_LOGGER.info("update table", extra={"job": self, "df": diff_df})
            for row in diff_df.collect():
                DEFAULT_LOGGER.debug(
                    f"{row.status.replace('ed', 'ing')} ({row.new_data_type})",
                    extra={"job": self, "column": row.column},
                )
                try:
                    update_df = df.select(row.column).where("1 == 2")
                    (
                        self.deltatable.alias("dt")
                        .merge(update_df.alias("df"), "1 == 2")
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                except Exception:
                    pass

    def overwrite_schema(self, df: DataFrame):
        diff_df = self.get_differences_with_dataframe(df)

        if not diff_df.isEmpty():
            DEFAULT_LOGGER.warning("overwrite table (update)", extra={"job": self, "df": diff_df})
            self.update_schema(df)

        diff_df = self.get_differences_with_dataframe(df)
        if not diff_df.isEmpty():
            diff_df.show()
            DEFAULT_LOGGER.warning("overwrite table (overwrite)", extra={"job": self, "df": diff_df})
            for row in diff_df.collect():
                if row.status == "added":
                    self.add_column(row.column, row.new_data_type)
                elif row.status == "dropped":
                    self.drop_column(row.column)
                elif row.status == "changed":
                    try:
                        self.change_column(row.column, row.new_data_type)
                    except AnalysisException:
                        self.drop_column(row.column)
                        self.add_column(row.column, row.new_data_type)

    def vacuum(self, retention_days: int = 7):
        DEFAULT_LOGGER.debug(f"vacuum table (removing files older than {retention_days} days)", extra={"job": self})
        self.spark.sql("SET self.spark.databricks.delta.retentionDurationCheck.enabled = False")
        try:
            self.create_restore_point()
            retention_hours = retention_days * 24
            self.deltatable.vacuum(retention_hours)
        finally:
            # finally
            pass
        self.spark.sql("SET self.spark.databricks.delta.retentionDurationCheck.enabled = True")

    def optimize(
        self,
        columns: Optional[Union[str, List[str]]] = None,
        vorder: Optional[bool] = False,
    ):
        DEFAULT_LOGGER.info("optimize", extra={"job": self})

        zorder_by = columns is not None
        if zorder_by:
            if isinstance(columns, str):
                columns = [columns]
            columns = [f"`{c}`" for c in columns]
            cols = ", ".join(columns)

            if vorder:
                DEFAULT_LOGGER.debug(f"zorder by {cols} vorder", extra={"job": self})
                self.spark.sql(f"optimize {self.qualified_name} zorder by ({cols}) vorder")
            else:
                DEFAULT_LOGGER.debug(f"zorder by {cols}", extra={"job": self})
                self.spark.sql(f"optimize {self.qualified_name} zorder by ({cols})")

        elif vorder:
            DEFAULT_LOGGER.debug("vorder", extra={"job": self})
            self.spark.sql(f"optimize {self.qualified_name} vorder")

        else:
            DEFAULT_LOGGER.debug("optimize", extra={"job": self})
            self.spark.sql(f"optimize {self.qualified_name}")

    def analyze(self):
        DEFAULT_LOGGER.debug("analyze", extra={"job": self})
        self.compute_statistics()
        self.compute_delta_statistics()

    def compute_statistics(self):
        DEFAULT_LOGGER.debug("compute statistics", extra={"job": self})
        cols = [
            f"`{name}`"
            for name, dtype in self.dataframe.dtypes
            if not dtype.startswith("struct") and not dtype.startswith("array") and name not in ["__metadata"]
        ]
        cols = ", ".join(sorted(cols))
        self.spark.sql(f"analyze table delta.`{self.delta_path}` compute statistics for columns {cols}")

    def compute_delta_statistics(self):
        DEFAULT_LOGGER.debug("compute delta statistics", extra={"job": self})
        self.spark.sql(f"analyze table delta.`{self.delta_path}` compute delta statistics")

    def drop_column(self, name: str):
        assert self.column_mapping_enabled, "column mapping not enabled"

        DEFAULT_LOGGER.warning(f"drop column {name}", extra={"job": self})
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            drop column `{name}`
            """
        )

    def change_column(self, name: str, type: str):
        assert self.column_mapping_enabled, "column mapping not enabled"

        DEFAULT_LOGGER.info(f"change column {name} ({type})", extra={"job": self})
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            change column `{name}` `{name}` {type}
            """
        )

    def rename_column(self, old: str, new: str):
        assert self.column_mapping_enabled, "column mapping not enabled"

        DEFAULT_LOGGER.info(f"rename column {old} -> {new}", extra={"job": self})
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            rename column `{old}` to `{new}`
            """
        )

    def get_details(self) -> DataFrame:
        return self.spark.sql(f"describe detail {self.qualified_name}")

    def get_properties(self) -> DataFrame:
        return self.spark.sql(f"show tblproperties {self.qualified_name}")

    def get_description(self) -> DataFrame:
        return self.spark.sql(f"describe extended {self.qualified_name}")

    def get_history(self) -> DataFrame:
        df = self.spark.sql(f"describe history {self.qualified_name}")
        return df

    def get_last_version(self) -> int:
        df = self.get_history()
        version = df.select(max("version")).collect()[0][0]
        return version

    def get_property(self, key: str) -> Optional[str]:
        try:
            value = self.get_properties().where(f"key == '{key}'").select("value").collect()[0][0]
            return value

        except IndexError:
            return None

    def enable_change_data_feed(self):
        DEFAULT_LOGGER.debug("enable change data feed", extra={"job": self})
        self.set_property("delta.enableChangeDataFeed", "true")

    def enable_column_mapping(self):
        DEFAULT_LOGGER.debug("enable column mapping", extra={"job": self})

        try:
            self.spark.sql(
                f"""
                alter table {self.qualified_name} 
                set tblproperties ('delta.columnMapping.mode' = 'name')
                """
            )

        except Exception:
            DEFAULT_LOGGER.debug("update reader and writer version", extra={"job": self})
            self.spark.sql(
                f"""
                alter table {self.qualified_name} 
                set tblproperties (
                    'delta.columnMapping.mode' = 'name',
                    'delta.minReaderVersion' = '2',
                    'delta.minWriterVersion' = '5'
                )
                """
            )

    def set_property(self, key: Union[str, int], value: Union[str, int]):
        DEFAULT_LOGGER.debug(f"set property {key} = {value}", extra={"job": self})
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            set tblproperties ({key} = '{value}')
            """
        )

    def add_constraint(self, name: str, expr: str):
        DEFAULT_LOGGER.debug(f"add constraint ({name} check ({expr}))", extra={"job": self})
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            add constraint {name} check ({expr});
            """
        )

    def add_comment(self, comment: str):
        DEFAULT_LOGGER.debug(f"add comment '{comment}'", extra={"job": self})
        self.spark.sql(
            f"""
            comment on table {self.qualified_name}
            is '{comment}';
            """
        )

    def add_materialized_column(self, name: str, expr: str, type: str):
        assert self.column_mapping_enabled, "column mapping not enabled"

        DEFAULT_LOGGER.info(f"add materialized column ({name} {type})", extra={"job": self})
        self.spark.sql(
            f""""
            alter table {self.qualified_name}
            add columns (`{name}` {type} materialized {expr})
            """
        )

    def add_column(self, name: str, type: str, after: Optional[str] = None):
        DEFAULT_LOGGER.info(f"add column {name} ({type})", extra={"job": self})
        ddl_after = "" if not after else f"after {after}"
        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            add columns (`{name}` {type} {ddl_after})
            """
        )

    def create_bloomfilter_index(self, columns: Union[str, List[str]]):
        if isinstance(columns, str):
            columns = [columns]
        columns = [f"`{c}`" for c in columns]
        cols = ", ".join(columns)

        DEFAULT_LOGGER.info(f"bloomfilter by {cols}", extra={"job": self})
        self.spark.sql(
            f"""
            create bloomfilter index on table {self.qualified_name}
            for columns ({cols})
            """
        )

    def create_restore_point(self):
        last_version = self.get_last_version() + 1
        self.set_property("fabricks.last_version", last_version)

    def show_properties(self) -> DataFrame:
        return self.spark.sql(f"show tblproperties {self.qualified_name}")

    def describe_detail(self) -> DataFrame:
        return self.spark.sql(f"describe detail {self.qualified_name}")

    def describe_extended(self) -> DataFrame:
        return self.spark.sql(f"describe extended {self.qualified_name}")

    def describe_history(self) -> DataFrame:
        df = self.spark.sql(f"describe history {self.qualified_name}")
        return df

    def enable_liquid_clustering(self, columns: Union[str, List[str]]):
        if isinstance(columns, str):
            columns = [columns]
        columns = [f"`{c}`" for c in columns]
        cols = ", ".join(columns)
        DEFAULT_LOGGER.info(f"cluster by {cols}", extra={"job": self})

        self.spark.sql(
            f"""
            alter table {self.qualified_name}
            cluster by ({cols})
            """
        )
