from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.cdc import NoCDC
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.resolver import resolve_option
from fabricks.metastore.table import SchemaDiff
from fabricks.metastore.view import create_or_replace_global_temp_view

if TYPE_CHECKING:
    from fabricks.core.jobs.base.job import BaseJob


class JobGenerator:
    """DDL mechanics: table/view creation, schema updates, drop/truncate/rm, external tables."""

    def __init__(self, job: BaseJob) -> None:
        self.job = job

    def _get_option_hierarchy(
        self,
        attribute: str,
        into: Literal["table", "spark"] = "table",
        default: Any = None,  # noqa: ANN401 - dynamic attribute lookup, return type depends on `attribute`
    ) -> Any:  # noqa: ANN401 - dynamic attribute lookup, return type depends on `attribute`
        """
        Get a table option value with fallback priority: job options → step options → default.

        Args:
            attribute: The attribute name to retrieve from the options.
            into: The type of options to look into ("table" or "spark").
            default: Default value if attribute is not found in either location

        Returns:
            The first non-None value found, or default if none found
        """
        if into == "table":
            table_options = self.job._resolver.table_options
            step_table_options = self.job.step_conf.table_options
            return resolve_option(
                getattr(table_options, attribute, None) if table_options else None,
                getattr(step_table_options, attribute, None) if step_table_options else None,
                default=default,
            )

        if into == "spark":
            spark_options = self.job._resolver.spark_options
            step_spark_options = self.job.step_conf.spark_options
            return resolve_option(
                getattr(spark_options, attribute, None) if spark_options else None,
                getattr(step_spark_options, attribute, None) if step_spark_options else None,
                default=default,
            )

        return default

    def update_dependencies(self) -> None:
        DEFAULT_LOGGER.info("update dependencies", extra={"label": self.job})

        deps = self.job.get_dependencies()
        if deps:
            df = self.job.spark.createDataFrame([d.model_dump() for d in deps])
            cdc = NoCDC("fabricks", self.job.step, "dependencies")
            cdc.delete_missing(df, keys=["dependency_id"], update_where=f"job_id = '{self.job.job_id}'", uuid=True)

    def rm(self) -> None:
        """
        Removes the schema folder and checkpoints associated with the generator.

        If the schema folder exists, it will be deleted. The method also calls the `rm_checkpoints`
        method to remove any checkpoints associated with the generator.
        """
        if self.job._resolver.paths.to_schema.exists():
            DEFAULT_LOGGER.info("delete schema folder", extra={"label": self.job})
            self.job._resolver.paths.to_schema.rm()
        self.rm_checkpoints()

    def rm_checkpoints(self) -> None:
        """
        Removes the checkpoints folder if it exists.

        This method checks if the checkpoints folder exists and deletes it if it does.
        """
        if self.job._resolver.paths.to_checkpoints.exists():
            DEFAULT_LOGGER.info("delete checkpoints folder", extra={"label": self.job})
            self.job._resolver.paths.to_checkpoints.rm()

    def rm_commit(self, id: str | int) -> None:
        """
        Remove a commit with the given ID.

        Args:
            id (Union[str, int]): The ID of the commit to remove.

        Returns:
            None
        """
        path = self.job._resolver.paths.to_commits.joinpath(str(id))
        if path.exists():
            DEFAULT_LOGGER.warning(f"delete commit {id}", extra={"label": self.job})
            path.rm()

    def truncate(self) -> None:
        """
        Truncates the job by removing all data associated with it.

        This method removes the job from the system and, if the `is_table` flag is set to True,
        it also truncates the associated table.

        Returns:
            None
        """
        DEFAULT_LOGGER.warning("truncate", extra={"label": self.job})
        self.rm()
        if self.job.is_table:
            self.job.table.truncate()

    def maintain(
        self, vacuum: bool | None = True, optimize: bool | None = True, compute_statistics: bool | None = True
    ) -> None:
        if self.job._resolver.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": self.job})

        else:
            if vacuum:
                self.job.vacuum()
            if optimize:
                self.job.cdc.optimize_table()
            if compute_statistics:
                self.job.table.compute_statistics()

    def vacuum(self) -> None:
        if self.job._resolver.mode == "memory":
            DEFAULT_LOGGER.debug("could not vacuum (memory)", extra={"label": self.job})

        else:
            table_options = self.job._resolver.table_options
            step_table_options = self.job._resolver.step_table_options
            retention_days = resolve_option(
                table_options.retention_days if table_options else None,
                step_table_options.retention_days if step_table_options else None,
                self.job._resolver.runtime_options.retention_days,
            )
            assert retention_days is not None

            self.job.table.vacuum(retention_days=retention_days)

    def drop(self) -> None:
        """
        Drops the current job and its dependencies.

        This method drops the current job and its dependencies by performing the following steps:
        1. Queries the database to check if there are any child jobs associated with the current job.
        2. If child jobs are found, logs a warning message and prints the list of child jobs.
        3. Drops the current job's change data capture (cdc).
        4. Removes the current job.

        Note: This method handles any exceptions that occur during the process.

        Returns:
                None
        """
        if self.job.options.no_drop:
            raise ValueError("no_drop is set, cannot drop the job")

        try:
            row = self.job.spark.sql(
                f"""
                select
                    count(*) as count,
                    array_join(sort_array(collect_set(j.job)), ', \n') as children
                from
                    fabricks.dependencies d
                    inner join fabricks.jobs j on d.job_id = j.job_id
                where
                    parent like '{self.job}'
                """
            ).collect()[0]
            if cast(int, row.count) > 0:
                DEFAULT_LOGGER.warning(
                    f"{row.count} children found", extra={"label": self.job, "content": row.children}
                )

        except Exception:
            pass

        self.job.cdc.drop()
        self.rm()

    def create(self) -> None:
        """
        Creates a table or view based on the specified mode.

        If `is_table` is True, it creates a table by calling the `create_table` method.
        If `is_view` is True, it creates or replaces a view by calling the `create_or_replace_view` method.
        If neither `is_table` nor `is_view` is True, it raises a ValueError.

        Raises:
            ValueError: If neither `is_table` nor `is_view` is True.

        """
        if self.job.is_table:
            self.create_table()
        elif self.job.is_view:
            self.job.create_or_replace_view()
        else:
            raise ValueError(f"{self.job._resolver.mode} not allowed")

    def register(self) -> None:
        """
        Register the job.

        If `is_table` is True, the job's table is registered.
        If `is_view` is True, a view is created or replaced.
        Otherwise, a ValueError is raised.

        Raises:
            ValueError: If `is_table` and `is_view` are both False.

        """
        if self.job.is_table:
            self.job.table.register()
        elif self.job.is_view:
            self.job.create_or_replace_view()
        else:
            raise ValueError(f"{self.job._resolver.mode} not allowed")

    def create_or_replace_view(self) -> None:
        """
        Creates or replaces a view.

        This method is responsible for creating or replacing a view in the database.
        It should be implemented by subclasses to define the specific logic for creating or replacing the view.

        Raises:
            NotImplementedError: This method is meant to be overridden by subclasses.
        """

    def _build_partitioning_columns(self, df: DataFrame) -> list[str] | None:
        table_options = self.job._resolver.table_options
        columns = table_options.partition_by if table_options and table_options.partition_by else []
        if columns:
            return columns

        columns = [c for c in df.columns if c.startswith("__partition")]
        if columns:
            DEFAULT_LOGGER.debug(
                f"found {len(columns)} partitioning column(s) ({', '.join(columns)})", extra={"label": self.job}
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any partitioning column", extra={"label": self.job})
        return None

    def _build_clustering_columns(self, df: DataFrame) -> list[str] | None:
        table_options = self.job._resolver.table_options
        columns = table_options.cluster_by if table_options and table_options.cluster_by else []
        if columns:
            return columns

        columns = []
        df_types = dict(df.dtypes)

        def _add_if_allowed(column: str) -> None:
            c_type = df_types[column]
            if c_type not in ["boolean"]:
                columns.append(column)
            else:
                DEFAULT_LOGGER.warning(
                    f"{column} found but {c_type} not allowed for clustering column", extra={"label": self.job}
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
                f"found {len(columns)} clustering column(s) ({', '.join(columns)})", extra={"label": self.job}
            )
            return columns

        DEFAULT_LOGGER.debug("could not determine any clustering column", extra={"label": self.job})
        return None

    def create_table(self) -> None:
        def _create_table(df: DataFrame, _batch: int | None = 0) -> None:
            df = self.job.base_transform(df)
            cdc_options = self.job.build_cdc_context(df)
            table_options = self.job._resolver.table_options

            liquid_clustering = False
            cluster_by = []

            partitioning = False
            partition_by = []

            powerbi = self._get_option_hierarchy("powerbi", into="table", default=False)
            masks = self._get_option_hierarchy("masks", into="table", default=None)
            identity = False

            maximum_compatibility = table_options.maximum_compatibility if table_options else False

            default_properties: dict[str, str | bool | int] = {}

            if maximum_compatibility:
                default_properties = {
                    "delta.minReaderVersion": "1",
                    "delta.minWriterVersion": "7",
                    "delta.columnMapping.mode": "none",
                }
            elif powerbi:
                default_properties = {
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                }
            else:
                default_properties = {
                    "delta.enableTypeWidening": "true",
                    "delta.enableDeletionVectors": "true",
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                    "delta.feature.timestampNtz": "supported",
                }

            default_properties["fabricks.last_version"] = "0"

            identity = False if "__identity" in df.columns else table_options.identity if table_options else False

            # first, check for partitioning columns
            partition_by = self._build_partitioning_columns(df)
            if partition_by:
                cluster_by = None
                partitioning = True

            # second, check for clustering columns if partitioning is not enabled
            if not partitioning:
                liquid_clustering = self._get_option_hierarchy("liquid_clustering", into="table", default=None)

                if liquid_clustering == "auto":
                    liquid_clustering = True
                    cluster_by = []

                elif liquid_clustering is not False:
                    cluster_by = self._build_clustering_columns(df)

                    if cluster_by:
                        liquid_clustering = True
                    else:
                        liquid_clustering = None
                        cluster_by = None

            properties = self._get_option_hierarchy("properties", into="table", default=None) if not powerbi else None

            if properties is None:
                properties = default_properties

            primary_key = table_options.primary_key or {} if table_options else {}
            foreign_keys = table_options.foreign_keys or {} if table_options else {}
            comments = table_options.comments or {} if table_options else {}

            generated_columns = table_options.generated_columns or {} if table_options else {}
            if generated_columns:
                for key in generated_columns:
                    assert key.startswith("__generated_"), (
                        "generated column name must start with '__generated_' "
                        "to avoid potential issue(s) with the CDC logic"
                    )

            # if dataframe, reference is passed (BUG)
            name = f"{self.job.step}_{self.job.topic}_{self.job.item}__init"
            global_temp_view = create_or_replace_global_temp_view(name=name, df=df.limit(0), job=self.job)
            sql = f"select * from {global_temp_view}"

            self.job.cdc.create_table(
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

        if not self.job.table.exists():
            DEFAULT_LOGGER.debug("create table", extra={"label": self.job})

            df = self.job.get_data(stream=self.job.is_stream, schema_only=True)
            if df:
                if self.job.is_stream:
                    # add dummy stream to be sure that the writeStream will start
                    spark = df.sparkSession

                    dummy_df = spark.readStream.table("fabricks.dummy")
                    # __metadata is always present
                    dummy_df = dummy_df.withColumn("__metadata", lit(None))
                    dummy_df = dummy_df.select("__metadata")

                    df = df.unionByName(dummy_df, allowMissingColumns=True)
                    path = self.job._resolver.paths.to_checkpoints.append("__init")
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

                table_options = self.job._resolver.table_options
                constraints = table_options.constraints or {} if table_options else {}
                if constraints:
                    for key, value in constraints.items():
                        self.job.table.add_constraint(name=key, expr=str(value))

                comment = table_options.comment if table_options else None
                if comment:
                    self.job.table.add_table_comment(comment=comment)

        else:
            DEFAULT_LOGGER.debug("table already exists, skipped creation", extra={"label": self.job})

    def _update_schema(
        self, df: DataFrame | None = None, overwrite: bool | None = False, widen_types: bool | None = False
    ) -> None:
        def _update_schema(df: DataFrame, _batch: int | None = None) -> None:
            context = self.job.build_cdc_context(df, reload=True)
            if overwrite:
                self.job.cdc.overwrite_schema(df, **context)
            else:
                self.job.cdc.update_schema(df, widen_types=widen_types, **context)

        if self.job.is_table:
            if df is not None:
                _update_schema(df)

            else:
                df = self.job.get_data(stream=self.job.is_stream, schema_only=True)
                assert df is not None
                df = self.job.base_transform(df)

                if self.job.is_stream:
                    path = self.job._resolver.paths.to_checkpoints.append("__schema")
                    query = (
                        df.writeStream.foreachBatch(_update_schema)
                        .option("checkpointLocation", path.string)
                        .trigger(once=True)
                        .start()
                    )
                    query.awaitTermination()
                    path.rm()

                else:
                    _update_schema(df)

        elif self.job.is_view:
            self.job.create_or_replace_view()

        else:
            raise ValueError(f"{self.job._resolver.mode} not allowed")

    def update_schema(self, df: DataFrame | None = None, widen_types: bool | None = False) -> None:
        self._update_schema(df=df, overwrite=False, widen_types=widen_types)

    def overwrite_schema(self, df: DataFrame | None = None) -> None:
        self._update_schema(df=df, overwrite=True)

    def update_comments(self, table: bool | None = True, columns: bool | None = True) -> None:
        if self.job.is_view:
            return

        if self.job.is_table:
            self.job.table.drop_comments()
            table_options = self.job._resolver.table_options

            if table:
                comment = table_options.comment if table_options else None
                if comment:
                    self.job.table.add_table_comment(comment=comment)

            if columns:
                comments = table_options.comments or {} if table_options else {}
                if comments:
                    for col, comment in comments.items():
                        self.job.table.add_column_comment(column=col, comment=str(comment))

    def get_differences_with_deltatable(self, df: DataFrame | None = None) -> DataFrame:
        if df is None:
            df = self.job.get_data(stream=self.job.is_stream)
            assert df is not None
            df = self.job.base_transform(df)

        context = self.job.build_cdc_context(df, reload=True)

        return self.job.cdc.get_differences_with_deltatable(df, **context)

    def get_schema_differences(self, df: DataFrame | None = None) -> Sequence[SchemaDiff] | None:
        if df is None:
            df = self.job.get_data(stream=self.job.is_stream)
            assert df is not None
            df = self.job.base_transform(df)

        context = self.job.build_cdc_context(df, reload=True)

        return self.job.cdc.get_schema_differences(df, **context)

    def schema_drifted(self, df: DataFrame | None = None) -> bool | None:
        d = self.get_schema_differences(df)
        if d is None:
            return None
        return len(d) > 0

    def register_external_table(self, file_format: str, uri: str) -> None:
        try:
            self.job.spark.sql(
                f"create table if not exists {self.job.qualified_name} using {file_format} location '{uri}'"
            )

        except Exception as e:
            DEFAULT_LOGGER.exception("could not register external table", extra={"label": self.job})
            raise e

    def drop_external_table(self) -> None:
        DEFAULT_LOGGER.warning("remove external table from metastore", extra={"label": self.job})
        self.job.spark.sql(f"drop table if exists {self.job.qualified_name}")
