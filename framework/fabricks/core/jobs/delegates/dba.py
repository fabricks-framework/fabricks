from __future__ import annotations

from typing import List, Literal, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing_extensions import deprecated

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.protocols import StorableJob
from fabricks.metastore.table import SchemaDiff
from fabricks.metastore.view import create_or_replace_global_temp_view


class SchemaDriftException(Exception):
    @staticmethod
    def from_diffs(table: str, diffs: Sequence[SchemaDiff]):
        out = []
        type_widening_compatible = True

        added = [d.new_column or d.column for d in diffs if d.status == "added"]
        if added:
            type_widening_compatible = False
            out.append("added columns:\n" + "\n".join(f"\t- {col}" for col in added))

        removed = [d.column for d in diffs if d.status == "dropped"]
        if removed:
            type_widening_compatible = False
            out.append("removed columns:\n" + "\n".join(f"\t- {col}" for col in removed))

        changed = [f"{d.column} ({d.data_type} -> {d.new_data_type})" for d in diffs if d.status == "changed"]
        if changed:
            if False in [d.type_widening_compatible for d in diffs if d.status == "changed"]:
                type_widening_compatible = False

            out.append("changed columns:\n" + "\n".join(f"\t- {col}" for col in changed))

        out_str = "\n".join(out)

        if type_widening_compatible:
            return SchemaDriftException(f"type widening detected:\n {out_str}", diffs, type_widening_compatible)
        else:
            return SchemaDriftException(f"schema drift detected:\n {out_str}", diffs, type_widening_compatible)

    def __init__(self, message: str, diffs: Sequence[SchemaDiff], type_widening_compatible: bool = False):
        super().__init__(message)
        self.diffs = diffs
        self.type_widening_compatible = type_widening_compatible


class JobDBA:
    """Owns all physical DB-object maintenance for a self._job.

    Covers the full lifecycle (create / register / drop / truncate), schema
    management (update / overwrite / comments / drift detection), storage
    artifact cleanup (rm / checkpoints / commits), routine maintenance
    (vacuum / optimize), external-table registration, and dependency tracking.

    Like the other delegates it takes the job and reads what it needs — but
    never fetches data on its own: the job passes DataFrames in when required.
    """

    def __init__(self, job: StorableJob):
        self._job = job

    # --- table DDL build (absorbed from JobTable) ---

    def _partitioning_columns(self, df: DataFrame) -> Optional[List[str]]:
        columns = (
            self._job.table_options.partition_by
            if self._job.table_options and self._job.table_options.partition_by
            else []
        )
        if columns:
            return columns

        columns = [c for c in df.columns if c.startswith("__partition")]
        if columns:
            DEFAULT_LOGGER.debug(
                f"found {len(columns)} partitioning column(s) ({', '.join(columns)})",
                extra={"label": self._job},
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any partitioning column", extra={"label": self._job})
        return None

    def _clustering_columns(self, df: DataFrame) -> Optional[List[str]]:
        columns = (
            self._job.table_options.cluster_by
            if self._job.table_options and self._job.table_options.cluster_by
            else []
        )
        if columns:
            return columns

        columns = []
        df_types = dict(df.dtypes)

        def _add_if_allowed(column: str):
            c_type = df_types[column]
            if c_type not in ["boolean"]:
                columns.append(column)
            else:
                DEFAULT_LOGGER.warning(
                    f"{column} found but {c_type} not allowed for clustering column",
                    extra={"label": self._job},
                )

        if "__source" in df_types:
            _add_if_allowed("__source")
        if "__is_current" in df_types:
            _add_if_allowed("__is_current")
        if "__key" in df_types:
            _add_if_allowed("__key")
        elif "__hash" in df_types:
            _add_if_allowed("__hash")
        for column in df.columns:
            if column.startswith("__cluster"):
                _add_if_allowed(column)

        if columns:
            DEFAULT_LOGGER.debug(
                f"found {len(columns)} clustering column(s) ({', '.join(columns)})",
                extra={"label": self._job},
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any clustering column", extra={"label": self._job})
        return None

    @staticmethod
    def _default_table_properties(maximum_compatibility: bool, powerbi: bool) -> dict[str, str | bool | int]:
        if maximum_compatibility:
            properties: dict[str, str | bool | int] = {
                "delta.minReaderVersion": "1",
                "delta.minWriterVersion": "7",
                "delta.columnMapping.mode": "none",
            }
        elif powerbi:
            properties: dict[str, str | bool | int] = {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
            }
        else:
            properties: dict[str, str | bool | int] = {
                "delta.enableTypeWidening": "true",
                "delta.enableDeletionVectors": "true",
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
                "delta.feature.timestampNtz": "supported",
            }
        properties["fabricks.last_version"] = "0"
        return properties

    def _get_option_hierarchy(self, attribute: str, into: Literal["table", "spark"] = "table", default=None):
        if into == "table":
            job_value = getattr(self._job.table_options, attribute, None) if self._job.table_options else None
            if job_value is not None:
                return job_value
            step_value = (
                getattr(self._job.step_conf.table_options, attribute, None)
                if self._job.step_conf.table_options
                else None
            )
            if step_value is not None:
                return step_value
        elif into == "spark":
            job_value = getattr(self._job.spark_options, attribute, None) if self._job.spark_options else None
            if job_value is not None:
                return job_value
            step_value = (
                getattr(self._job.step_conf.spark_options, attribute, None)
                if self._job.step_conf.spark_options
                else None
            )
            if step_value is not None:
                return step_value
        return default

    def _build_table(self, df: DataFrame):
        """DDL build: resolves layout options and issues CREATE TABLE via the CDC layer."""

        def _create_table(batch_df: DataFrame, batch: Optional[int] = 0):
            batch_df = self._job.base_transform(batch_df)
            cdc_options = self._job.get_cdc_context(batch_df)

            liquid_clustering = False
            cluster_by = []
            partitioning = False
            partition_by = []

            powerbi = self._get_option_hierarchy("powerbi", into="table", default=False)
            masks = self._get_option_hierarchy("masks", into="table", default=None)
            identity = False

            maximum_compatibility = self._job.table_options.maximum_compatibility if self._job.table_options else False
            default_properties = self._default_table_properties(bool(maximum_compatibility), bool(powerbi))

            if "__identity" in batch_df.columns:
                identity = False
            else:
                identity = self._job.table_options.identity if self._job.table_options else False

            partition_by = self._partitioning_columns(batch_df)
            if partition_by:
                cluster_by = None
                partitioning = True

            if not partitioning:
                liquid_clustering = self._get_option_hierarchy("liquid_clustering", into="table", default=None)

                if liquid_clustering == "auto":
                    liquid_clustering = True
                    cluster_by = []
                elif liquid_clustering is not False:
                    cluster_by = self._clustering_columns(batch_df)
                    if cluster_by:
                        liquid_clustering = True
                    else:
                        liquid_clustering = None
                        cluster_by = None

            if not powerbi:
                properties = self._get_option_hierarchy("properties", into="table", default=None)
            else:
                properties = None

            if properties is None:
                properties = default_properties

            primary_key = self._job.table_options.primary_key or {} if self._job.table_options else {}
            foreign_keys = self._job.table_options.foreign_keys or {} if self._job.table_options else {}
            comments = self._job.table_options.comments or {} if self._job.table_options else {}
            generated_columns = self._job.table_options.generated_columns or {} if self._job.table_options else {}

            if generated_columns:
                for key in generated_columns.keys():
                    assert key.startswith("__generated_"), (
                        "generated column name must start with '__generated_' to avoid potential issue(s) with the CDC logic"
                    )

            # if dataframe, reference is passed (BUG)
            name = f"{self._job.step}_{self._job.topic}_{self._job.item}__init"
            global_temp_view = create_or_replace_global_temp_view(name=name, df=batch_df.limit(0), job=self._job)
            sql = f"select * from {global_temp_view}"

            self._job.cdc.create_table(
                sql,
                identity=identity,
                liquid_clustering=liquid_clustering,
                cluster_by=cluster_by,
                partitioning=partitioning,
                partition_by=partition_by,
                properties=properties,
                masks=masks,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
                generated_columns=generated_columns,
                comments=comments,
                **cdc_options,
            )

        if self._job.stream:
            spark = df.sparkSession
            dummy_df = spark.readStream.table("fabricks.dummy")
            # __metadata is always present
            dummy_df = dummy_df.withColumn("__metadata", lit(None))
            dummy_df = dummy_df.select("__metadata")

            df = df.unionByName(dummy_df, allowMissingColumns=True)
            path = self._job.paths.to_checkpoints.append("__init")
            if path.exists():
                path.rm()

            query = (
                df.writeStream.foreachBatch(_create_table)
                .option("checkpointLocation", path.string)
                .trigger(once=True)
                .start()
            )
            query.awaitTermination()
            path.rm()
        else:
            _create_table(df)

        constraints = self._job.table_options.constraints or {} if self._job.table_options else {}
        if constraints:
            for key, value in constraints.items():
                self._job.table.add_constraint(name=key, expr=str(value))

        comment = self._job.table_options.comment if self._job.table_options else None
        if comment:
            self._job.table.add_table_comment(comment=comment)

    # --- lifecycle ---

    def create_table(self):
        if self._job.table.exists():
            DEFAULT_LOGGER.debug("table already exists, skipped creation", extra={"label": self._job})
            return

        DEFAULT_LOGGER.debug("create table", extra={"label": self._job})

        df = self._job.get_data(stream=self._job.stream, schema_only=True)
        if df:
            self._build_table(df)

    def create_or_replace_view(self, sql: str):
        job = self._job
        df = job.spark.sql(sql)
        cdc_options = job.get_cdc_context(df)
        job.cdc.create_or_replace_view(sql, **cdc_options)

    def create(self):
        if self._job.persist:
            self.create_table()
        elif self._job.virtual:
            self._job.create_or_replace_view()
        else:
            raise ValueError(f"{self._job.mode} not allowed")

    def register(self):
        if self._job.persist:
            self._job.table.register()
        elif self._job.virtual:
            self._job.create_or_replace_view()
        else:
            raise ValueError(f"{self._job.mode} not allowed")

    def drop(self):
        if self._job.options.no_drop:
            raise ValueError("no_drop is set, cannot drop the job")

        try:
            row = self._job.spark.sql(
                f"""
                select
                    count(*) as count,
                    array_join(sort_array(collect_set(j.job)), ', \n') as children
                from
                    fabricks.dependencies d
                    inner join fabricks.jobs j on d.job_id = j.job_id
                where
                    parent like '{self._job}'
                """
            ).collect()[0]
            from typing import cast

            if cast(int, row.count) > 0:
                DEFAULT_LOGGER.warning(
                    f"{row.count} children found", extra={"label": self._job, "content": row.children}
                )
        except Exception:
            pass

        self._job.cdc.drop()
        self.rm()

    def truncate(self):
        DEFAULT_LOGGER.warning("truncate", extra={"label": self._job})
        self.rm()
        if self._job.persist:
            self._job.table.truncate()

    # --- storage artifacts ---

    def rm(self):
        if self._job.paths.to_schema.exists():
            DEFAULT_LOGGER.info("delete schema folder", extra={"label": self._job})
            self._job.paths.to_schema.rm()
        self.rm_checkpoints()

    def rm_checkpoints(self):
        if self._job.paths.to_checkpoints.exists():
            DEFAULT_LOGGER.info("delete checkpoints folder", extra={"label": self._job})
            self._job.paths.to_checkpoints.rm()

    def rm_commit(self, id):
        path = self._job.paths.to_commits.joinpath(str(id))
        if path.exists():
            DEFAULT_LOGGER.warning(f"delete commit {id}", extra={"label": self._job})
            path.rm()

    # --- schema management ---

    def _update_schema(
        self,
        df: Optional[DataFrame] = None,
        overwrite: Optional[bool] = False,
        widen_types: Optional[bool] = False,
    ):

        def _do_update(df: DataFrame, batch: Optional[int] = None):
            context = self._job.get_cdc_context(df, reload=True)
            if overwrite:
                self._job.cdc.overwrite_schema(df, **context)
            else:
                self._job.cdc.update_schema(df, widen_types=widen_types, **context)

        if self._job.persist:
            if df is not None:
                _do_update(df)
            else:
                df = self._job.get_data(stream=self._job.stream, schema_only=True)
                assert df is not None
                df = self._job.base_transform(df)

                if self._job.stream:
                    path = self._job.paths.to_checkpoints.append("__schema")
                    query = (
                        df.writeStream.foreachBatch(_do_update)
                        .option("checkpointLocation", path.string)
                        .trigger(once=True)
                        .start()
                    )
                    query.awaitTermination()
                    path.rm()
                else:
                    _do_update(df)

        elif self._job.virtual:
            self._job.create_or_replace_view()
        else:
            raise ValueError(f"{self._job.mode} not allowed")

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False):
        self._update_schema(df=df, overwrite=False, widen_types=widen_types)

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        self._update_schema(df=df, overwrite=True)

    def update_comments(self, table: Optional[bool] = True, columns: Optional[bool] = True):
        if self._job.virtual:
            return

        if self._job.persist:
            self._job.table.drop_comments()

            if table:
                comment = self._job.table_options.comment if self._job.table_options else None
                if comment:
                    self._job.table.add_table_comment(comment=comment)

            if columns:
                comments = self._job.table_options.comments or {} if self._job.table_options else {}
                if comments:
                    for col, comment in comments.items():
                        self._job.table.add_column_comment(column=col, comment=str(comment))

    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]:
        if df is None:
            df = self._job.get_data(stream=self._job.stream)
            assert df is not None
            df = self._job.base_transform(df)

        context = self._job.get_cdc_context(df, reload=True)
        return self._job.cdc.get_schema_differences(df, **context)

    def schema_drifted(self, df: Optional[DataFrame] = None) -> Optional[bool]:
        d = self.get_schema_differences(df)
        if d is None:
            return None
        return len(d) > 0

    def get_differences_with_deltatable(self, df: Optional[DataFrame] = None):
        if df is None:
            df = self._job.get_data(stream=self._job.stream)
            assert df is not None
            df = self._job.base_transform(df)

        context = self._job.get_cdc_context(df, reload=True)
        return self._job.cdc.get_differences_with_deltatable(df, **context)

    # --- maintenance ---

    @deprecated("use maintain instead")
    def optimize(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        return self.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=analyze)

    def maintain(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        compute_statistics: Optional[bool] = True,
    ):
        if self._job.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": self._job})
        else:
            if vacuum:
                self.vacuum()
            if optimize:
                self._job.cdc.optimize_table()
            if compute_statistics:
                self._job.table.compute_statistics()

    def vacuum(self):
        if self._job.mode == "memory":
            DEFAULT_LOGGER.debug("could not vacuum (memory)", extra={"label": self._job})
        else:
            job_days = self._job.table_options.retention_days if self._job.table_options else None
            step_days = self._job.step_table_options.retention_days if self._job.step_table_options else None
            runtime_days = self._job.runtime_options.retention_days

            if job_days is not None:
                retention_days = job_days
            elif step_days:
                retention_days = step_days
            else:
                assert runtime_days
                retention_days = runtime_days

            self._job.table.vacuum(retention_days=retention_days)

    # --- restore ---

    def restore(self, last_version: str | None = None, last_batch: str | None = None):
        if self._job.persist:
            if last_version is not None:
                _last_version = int(last_version)
                if self._job.table.get_last_version() > _last_version:
                    self._job.table.restore_to_version(_last_version)

            if self._job.stream:
                if last_batch is not None:
                    current_batch = int(last_batch) + 1
                    self.rm_commit(current_batch)

                    assert last_batch == self._job.table.get_property("fabricks.last_batch")
                    assert self._job.paths.to_commits.joinpath(last_batch).exists()

    # --- external tables ---

    def register_external_table(self, file_format: str, uri: str):
        try:
            self._job.spark.sql(
                f"create table if not exists {self._job.qualified_name} using {file_format} location '{uri}'"
            )
        except Exception as e:
            DEFAULT_LOGGER.exception("could not register external table", extra={"label": self._job})
            raise e

    def drop_external_table(self):
        DEFAULT_LOGGER.warning("remove external table from metastore", extra={"label": self._job})
        self._job.spark.sql(f"drop table if exists {self._job.qualified_name}")
