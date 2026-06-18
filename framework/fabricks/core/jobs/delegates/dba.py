from __future__ import annotations

from typing import List, Literal, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing_extensions import deprecated

from fabricks.cdc import NoCDC
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.protocols import JobProtocol
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
    """Owns all physical DB-object maintenance for a job.

    Covers the full lifecycle (create / register / drop / truncate), schema
    management (update / overwrite / comments / drift detection), storage
    artifact cleanup (rm / checkpoints / commits), routine maintenance
    (vacuum / optimize), external-table registration, and dependency tracking.

    Like the other delegates it takes the job and reads what it needs — but
    never fetches data on its own: the job passes DataFrames in when required.
    """

    def __init__(self, job: JobProtocol):
        self._job = job

    # --- table DDL build (absorbed from JobTable) ---

    def _partitioning_columns(self, df: DataFrame) -> Optional[List[str]]:
        job = self._job
        columns = job.table_options.partition_by if job.table_options and job.table_options.partition_by else []
        if columns:
            return columns

        columns = [c for c in df.columns if c.startswith("__partition")]
        if columns:
            DEFAULT_LOGGER.debug(
                f"found {len(columns)} partitioning column(s) ({', '.join(columns)})",
                extra={"label": job},
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any partitioning column", extra={"label": job})
        return None

    def _clustering_columns(self, df: DataFrame) -> Optional[List[str]]:
        job = self._job
        columns = job.table_options.cluster_by if job.table_options and job.table_options.cluster_by else []
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
                    extra={"label": job},
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
                extra={"label": job},
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any clustering column", extra={"label": job})
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
        job = self._job
        if into == "table":
            job_value = getattr(job.table_options, attribute, None) if job.table_options else None
            if job_value is not None:
                return job_value
            step_value = getattr(job.step_conf.table_options, attribute, None) if job.step_conf.table_options else None
            if step_value is not None:
                return step_value
        elif into == "spark":
            job_value = getattr(job.spark_options, attribute, None) if job.spark_options else None
            if job_value is not None:
                return job_value
            step_value = getattr(job.step_conf.spark_options, attribute, None) if job.step_conf.spark_options else None
            if step_value is not None:
                return step_value
        return default

    def _build_table(self, df: DataFrame):
        """DDL build: resolves layout options and issues CREATE TABLE via the CDC layer."""
        job = self._job

        def _create_table(batch_df: DataFrame, batch: Optional[int] = 0):
            batch_df = job.base_transform(batch_df)
            cdc_options = job.get_cdc_context(batch_df)

            liquid_clustering = False
            cluster_by = []
            partitioning = False
            partition_by = []

            powerbi = self._get_option_hierarchy("powerbi", into="table", default=False)
            masks = self._get_option_hierarchy("masks", into="table", default=None)
            identity = False

            maximum_compatibility = job.table_options.maximum_compatibility if job.table_options else False
            default_properties = self._default_table_properties(bool(maximum_compatibility), bool(powerbi))

            if "__identity" in batch_df.columns:
                identity = False
            else:
                identity = job.table_options.identity if job.table_options else False

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

            primary_key = job.table_options.primary_key or {} if job.table_options else {}
            foreign_keys = job.table_options.foreign_keys or {} if job.table_options else {}
            comments = job.table_options.comments or {} if job.table_options else {}
            generated_columns = job.table_options.generated_columns or {} if job.table_options else {}

            if generated_columns:
                for key in generated_columns.keys():
                    assert key.startswith("__generated_"), (
                        "generated column name must start with '__generated_' to avoid potential issue(s) with the CDC logic"
                    )

            # if dataframe, reference is passed (BUG)
            name = f"{job.step}_{job.topic}_{job.item}__init"
            global_temp_view = create_or_replace_global_temp_view(name=name, df=batch_df.limit(0), job=job)
            sql = f"select * from {global_temp_view}"

            job.cdc.create_table(
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

        if job.stream:
            spark = df.sparkSession
            dummy_df = spark.readStream.table("fabricks.dummy")
            # __metadata is always present
            dummy_df = dummy_df.withColumn("__metadata", lit(None))
            dummy_df = dummy_df.select("__metadata")

            df = df.unionByName(dummy_df, allowMissingColumns=True)
            path = job.paths.to_checkpoints.append("__init")
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

        constraints = job.table_options.constraints or {} if job.table_options else {}
        if constraints:
            for key, value in constraints.items():
                job.table.add_constraint(name=key, expr=str(value))

        comment = job.table_options.comment if job.table_options else None
        if comment:
            job.table.add_table_comment(comment=comment)

    # --- lifecycle ---

    def create_table(self):
        job = self._job
        if job.table.exists():
            DEFAULT_LOGGER.debug("table already exists, skipped creation", extra={"label": job})
            return

        DEFAULT_LOGGER.debug("create table", extra={"label": job})
        job.register_udfs()

        df = job.get_data(stream=job.stream, schema_only=True)
        if df:
            self._build_table(df)

    def create(self):
        job = self._job
        if job.persist:
            self.create_table()
        elif job.virtual:
            job.create_or_replace_view()
        else:
            raise ValueError(f"{job.mode} not allowed")

    def register(self):
        job = self._job
        if job.persist:
            job.table.register()
        elif job.virtual:
            job.create_or_replace_view()
        else:
            raise ValueError(f"{job.mode} not allowed")

    def drop(self):
        job = self._job
        if job.options.no_drop:
            raise ValueError("no_drop is set, cannot drop the job")

        try:
            row = job.spark.sql(
                f"""
                select
                    count(*) as count,
                    array_join(sort_array(collect_set(j.job)), ', \n') as children
                from
                    fabricks.dependencies d
                    inner join fabricks.jobs j on d.job_id = j.job_id
                where
                    parent like '{job}'
                """
            ).collect()[0]
            from typing import cast

            if cast(int, row.count) > 0:
                DEFAULT_LOGGER.warning(f"{row.count} children found", extra={"label": job, "content": row.children})
        except Exception:
            pass

        job.cdc.drop()
        self.rm()

    def truncate(self):
        job = self._job
        DEFAULT_LOGGER.warning("truncate", extra={"label": job})
        self.rm()
        if job.persist:
            job.table.truncate()

    # --- storage artifacts ---

    def rm(self):
        job = self._job
        if job.paths.to_schema.exists():
            DEFAULT_LOGGER.info("delete schema folder", extra={"label": job})
            job.paths.to_schema.rm()
        self.rm_checkpoints()

    def rm_checkpoints(self):
        job = self._job
        if job.paths.to_checkpoints.exists():
            DEFAULT_LOGGER.info("delete checkpoints folder", extra={"label": job})
            job.paths.to_checkpoints.rm()

    def rm_commit(self, id):
        job = self._job
        path = job.paths.to_commits.joinpath(str(id))
        if path.exists():
            DEFAULT_LOGGER.warning(f"delete commit {id}", extra={"label": job})
            path.rm()

    # --- schema management ---

    def _update_schema(
        self,
        df: Optional[DataFrame] = None,
        overwrite: Optional[bool] = False,
        widen_types: Optional[bool] = False,
    ):
        job = self._job

        def _do_update(df: DataFrame, batch: Optional[int] = None):
            context = job.get_cdc_context(df, reload=True)
            if overwrite:
                job.cdc.overwrite_schema(df, **context)
            else:
                job.cdc.update_schema(df, widen_types=widen_types, **context)

        if job.persist:
            if df is not None:
                _do_update(df)
            else:
                df = job.get_data(stream=job.stream, schema_only=True)
                assert df is not None
                df = job.base_transform(df)

                if job.stream:
                    path = job.paths.to_checkpoints.append("__schema")
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

        elif job.virtual:
            job.create_or_replace_view()
        else:
            raise ValueError(f"{job.mode} not allowed")

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False):
        self._update_schema(df=df, overwrite=False, widen_types=widen_types)

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        self._update_schema(df=df, overwrite=True)

    def update_comments(self, table: Optional[bool] = True, columns: Optional[bool] = True):
        job = self._job
        if job.virtual:
            return

        if job.persist:
            job.table.drop_comments()

            if table:
                comment = job.table_options.comment if job.table_options else None
                if comment:
                    job.table.add_table_comment(comment=comment)

            if columns:
                comments = job.table_options.comments or {} if job.table_options else {}
                if comments:
                    for col, comment in comments.items():
                        job.table.add_column_comment(column=col, comment=str(comment))

    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]:
        job = self._job
        if df is None:
            df = job.get_data(stream=job.stream)
            assert df is not None
            df = job.base_transform(df)

        context = job.get_cdc_context(df, reload=True)
        return job.cdc.get_schema_differences(df, **context)

    def schema_drifted(self, df: Optional[DataFrame] = None) -> Optional[bool]:
        d = self.get_schema_differences(df)
        if d is None:
            return None
        return len(d) > 0

    def get_differences_with_deltatable(self, df: Optional[DataFrame] = None):
        job = self._job
        if df is None:
            df = job.get_data(stream=job.stream)
            assert df is not None
            df = job.base_transform(df)

        context = job.get_cdc_context(df, reload=True)
        return job.cdc.get_differences_with_deltatable(df, **context)

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
        job = self._job
        if job.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": job})
        else:
            if vacuum:
                self.vacuum()
            if optimize:
                job.cdc.optimize_table()
            if compute_statistics:
                job.table.compute_statistics()

    def vacuum(self):
        job = self._job
        if job.mode == "memory":
            DEFAULT_LOGGER.debug("could not vacuum (memory)", extra={"label": job})
        else:
            job_days = job.table_options.retention_days if job.table_options else None
            step_days = job.step_table_options.retention_days if job.step_table_options else None
            runtime_days = job.runtime_options.retention_days

            if job_days is not None:
                retention_days = job_days
            elif step_days:
                retention_days = step_days
            else:
                assert runtime_days
                retention_days = runtime_days

            job.table.vacuum(retention_days=retention_days)

    # --- restore ---

    def restore(self, last_version: str | None = None, last_batch: str | None = None):
        job = self._job
        if job.persist:
            if last_version is not None:
                _last_version = int(last_version)
                if job.table.get_last_version() > _last_version:
                    job.table.restore_to_version(_last_version)

            if job.stream:
                if last_batch is not None:
                    current_batch = int(last_batch) + 1
                    self.rm_commit(current_batch)

                    assert last_batch == job.table.get_property("fabricks.last_batch")
                    assert job.paths.to_commits.joinpath(last_batch).exists()

    # --- external tables ---

    def register_external_table(self, file_format: str, uri: str):
        job = self._job
        try:
            job.spark.sql(f"create table if not exists {job.qualified_name} using {file_format} location '{uri}'")
        except Exception as e:
            DEFAULT_LOGGER.exception("could not register external table", extra={"label": job})
            raise e

    def drop_external_table(self):
        job = self._job
        DEFAULT_LOGGER.warning("remove external table from metastore", extra={"label": job})
        job.spark.sql(f"drop table if exists {job.qualified_name}")

    # --- dependencies ---

    def update_dependencies(self):
        job = self._job
        DEFAULT_LOGGER.info("update dependencies", extra={"label": job})
        deps = job.get_dependencies()
        if deps:
            df = job.spark.createDataFrame([d.model_dump() for d in deps])
            cdc = NoCDC("fabricks", job.step, "dependencies")
            cdc.delete_missing(df, keys=["dependency_id"], update_where=f"job_id = '{job.job_id}'", uuid=True)
