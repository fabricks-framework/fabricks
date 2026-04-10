from abc import abstractmethod
from typing import List, Literal, Optional, Sequence, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.cdc import NoCDC
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.configurator import Configurator
from fabricks.metastore.table import SchemaDiff
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.models import JobDependency


class Generator(Configurator):
    def _get_option_hierarchy(self, attribute: str, into: Literal["table", "spark"] = "table", default=None):
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
            job_value = getattr(self.table_options, attribute, None) if self.table_options else None
            if job_value is not None:
                return job_value

            step_value = (
                getattr(self.step_conf.table_options, attribute, None) if self.step_conf.table_options else None
            )
            if step_value is not None:
                return step_value

        elif into == "spark":
            job_value = getattr(self.spark_options, attribute, None) if self.spark_options else None
            if job_value is not None:
                return job_value

            step_value = (
                getattr(self.step_conf.spark_options, attribute, None) if self.step_conf.spark_options else None
            )
            if step_value is not None:
                return step_value

        return default

    def update_dependencies(self):
        DEFAULT_LOGGER.info("update dependencies", extra={"label": self})

        deps = self.get_dependencies()
        if deps:
            df = self.spark.createDataFrame([d.model_dump() for d in deps])
            cdc = NoCDC("fabricks", self.step, "dependencies")
            cdc.delete_missing(df, keys=["dependency_id"], update_where=f"job_id = '{self.job_id}'", uuid=True)

    @abstractmethod
    def get_dependencies(self) -> Sequence[JobDependency]: ...

    def rm(self):
        """
        Removes the schema folder and checkpoints associated with the generator.

        If the schema folder exists, it will be deleted. The method also calls the `rm_checkpoints` method to remove any checkpoints associated with the generator.
        """
        if self.paths.to_schema.exists():
            DEFAULT_LOGGER.info("delete schema folder", extra={"label": self})
            self.paths.to_schema.rm()
        self.rm_checkpoints()

    def rm_checkpoints(self):
        """
        Removes the checkpoints folder if it exists.

        This method checks if the checkpoints folder exists and deletes it if it does.
        """
        if self.paths.to_checkpoints.exists():
            DEFAULT_LOGGER.info("delete checkpoints folder", extra={"label": self})
            self.paths.to_checkpoints.rm()

    def rm_commit(self, id: Union[str, int]):
        """
        Remove a commit with the given ID.

        Args:
            id (Union[str, int]): The ID of the commit to remove.

        Returns:
            None
        """
        path = self.paths.to_commits.joinpath(str(id))
        if path.exists():
            DEFAULT_LOGGER.warning(f"delete commit {id}", extra={"label": self})
            path.rm()

    def truncate(self):
        """
        Truncates the job by removing all data associated with it.

        This method removes the job from the system and, if the `persist` flag is set to True,
        it also truncates the associated table.

        Returns:
            None
        """
        DEFAULT_LOGGER.warning("truncate", extra={"label": self})
        self.rm()
        if self.persist:
            self.table.truncate()

    def drop(self):
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
        if self.options.no_drop:
            raise ValueError("no_drop is set, cannot drop the job")

        try:
            row = self.spark.sql(
                f"""
                select
                    count(*) as count,
                    array_join(sort_array(collect_set(j.job)), ', \n') as children
                from
                    fabricks.dependencies d
                    inner join fabricks.jobs j on d.job_id = j.job_id
                where
                    parent like '{self}'
                """
            ).collect()[0]
            if cast(int, row.count) > 0:
                DEFAULT_LOGGER.warning(f"{row.count} children found", extra={"label": self, "content": row.children})

        except Exception:
            pass

        self.cdc.drop()
        self.rm()

    def create(self):
        """
        Creates a table or view based on the specified mode.

        If `persist` is True, it creates a table by calling the `create_table` method.
        If `virtual` is True, it creates or replaces a view by calling the `create_or_replace_view` method.
        If neither `persist` nor `virtual` is True, it raises a ValueError.

        Raises:
            ValueError: If neither `persist` nor `virtual` is True.

        """
        if self.persist:
            self.create_table()
        elif self.virtual:
            self.create_or_replace_view()
        else:
            raise ValueError(f"{self.mode} not allowed")

    def register(self):
        """
        Register the job.

        If `persist` is True, the job's table is registered.
        If `virtual` is True, a view is created or replaced.
        Otherwise, a ValueError is raised.

        Raises:
            ValueError: If `persist` and `virtual` are both False.

        """
        if self.persist:
            self.table.register()
        elif self.virtual:
            self.create_or_replace_view()
        else:
            raise ValueError(f"{self.mode} not allowed")

    def create_or_replace_view(self):
        """
        Creates or replaces a view.

        This method is responsible for creating or replacing a view in the database.
        It should be implemented by subclasses to define the specific logic for creating or replacing the view.

        Raises:
            NotImplementedError: This method is meant to be overridden by subclasses.
        """
        ...

    def _get_partitioning_columns(self, df: DataFrame) -> Optional[List[str]]:
        columns = self.table_options.partition_by if self.table_options and self.table_options.partition_by else []
        if columns:
            return columns

        columns = [c for c in df.columns if c.startswith("__partition")]
        if columns:
            DEFAULT_LOGGER.debug(
                f"found {len(columns)} partitioning column(s) ({', '.join(columns)})",
                extra={"label": self},
            )
            return columns

        else:
            DEFAULT_LOGGER.debug("could not determine any partitioning column", extra={"label": self})
            return None

    def _get_clustering_columns(self, df: DataFrame) -> Optional[List[str]]:
        columns = self.table_options.cluster_by if self.table_options and self.table_options.cluster_by else []
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
                    extra={"label": self},
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
                extra={"label": self},
            )
            return columns

        else:
            DEFAULT_LOGGER.debug("could not determine any clustering column", extra={"label": self})
            return None

    def create_table(self):
        def _create_table(df: DataFrame, batch: Optional[int] = 0):
            df = self.base_transform(df)
            cdc_options = self.get_cdc_context(df)

            liquid_clustering = False
            cluster_by = []

            partitioning = False
            partition_by = []

            powerbi = self._get_option_hierarchy("powerbi", into="table", default=False)
            masks = self._get_option_hierarchy("masks", into="table", default=None)
            identity = False

            maximum_compatibility = self.table_options.maximum_compatibility if self.table_options else False

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

            if "__identity" in df.columns:
                identity = False
            else:
                identity = self.table_options.identity if self.table_options else False

            # first, check for partitioning columns
            partition_by = self._get_partitioning_columns(df)
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
                    cluster_by = self._get_clustering_columns(df)

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

            primary_key = self.table_options.primary_key or {} if self.table_options else {}
            foreign_keys = self.table_options.foreign_keys or {} if self.table_options else {}
            comments = self.table_options.comments or {} if self.table_options else {}

            generated_columns = self.table_options.generated_columns or {} if self.table_options else {}
            if generated_columns:
                for key in generated_columns.keys():
                    assert key.startswith("__generated_"), (
                        "generated column name must start with '__generated_' to avoid potential issue(s) with the CDC logic"
                    )

            # if dataframe, reference is passed (BUG)
            name = f"{self.step}_{self.topic}_{self.item}__init"
            global_temp_view = create_or_replace_global_temp_view(name=name, df=df.where("1 == 2"), job=self)
            sql = f"select * from {global_temp_view}"

            self.cdc.create_table(
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

        if not self.table.exists():
            DEFAULT_LOGGER.debug("create table", extra={"label": self})

            self.register_udfs()

            df = self.get_data(stream=self.stream, schema_only=True)
            if df:
                if self.stream:
                    # add dummy stream to be sure that the writeStream will start
                    spark = df.sparkSession

                    dummy_df = spark.readStream.table("fabricks.dummy")
                    # __metadata is always present
                    dummy_df = dummy_df.withColumn("__metadata", lit(None))
                    dummy_df = dummy_df.select("__metadata")

                    df = df.unionByName(dummy_df, allowMissingColumns=True)
                    path = self.paths.to_checkpoints.append("__init")
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

                constraints = self.table_options.constraints or {} if self.table_options else {}
                if constraints:
                    for key, value in constraints.items():
                        self.table.add_constraint(name=key, expr=str(value))

                comment = self.table_options.comment if self.table_options else None
                if comment:
                    self.table.add_table_comment(comment=comment)

        else:
            DEFAULT_LOGGER.debug("table already exists, skipped creation", extra={"label": self})

    def _update_schema(
        self,
        df: Optional[DataFrame] = None,
        overwrite: Optional[bool] = False,
        widen_types: Optional[bool] = False,
    ):
        def _update_schema(df: DataFrame, batch: Optional[int] = None):
            context = self.get_cdc_context(df, reload=True)
            if overwrite:
                self.cdc.overwrite_schema(df, **context)
            else:
                self.cdc.update_schema(df, widen_types=widen_types, **context)

        if self.persist:
            if df is not None:
                _update_schema(df)

            else:
                df = self.get_data(stream=self.stream, schema_only=True)
                assert df is not None
                df = self.base_transform(df)

                if self.stream:
                    path = self.paths.to_checkpoints.append("__schema")
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

        elif self.virtual:
            self.create_or_replace_view()

        else:
            raise ValueError(f"{self.mode} not allowed")

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False):
        self._update_schema(df=df, overwrite=False, widen_types=widen_types)

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        self._update_schema(df=df, overwrite=True)

    def update_comments(self, table: Optional[bool] = True, columns: Optional[bool] = True):
        if self.virtual:
            return

        if self.persist:
            self.table.drop_comments()

            if table:
                comment = self.table_options.comment if self.table_options else None
                if comment:
                    self.table.add_table_comment(comment=comment)

            if columns:
                comments = self.table_options.comments or {} if self.table_options else {}
                if comments:
                    for col, comment in comments.items():
                        self.table.add_column_comment(column=col, comment=str(comment))

    def get_differences_with_deltatable(self, df: Optional[DataFrame] = None):
        if df is None:
            df = self.get_data(stream=self.stream)
            assert df is not None
            df = self.base_transform(df)

        context = self.get_cdc_context(df, reload=True)

        return self.cdc.get_differences_with_deltatable(df, **context)

    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]:
        if df is None:
            df = self.get_data(stream=self.stream)
            assert df is not None
            df = self.base_transform(df)

        context = self.get_cdc_context(df, reload=True)

        return self.cdc.get_schema_differences(df, **context)

    def schema_drifted(self, df: Optional[DataFrame] = None) -> Optional[bool]:
        d = self.get_schema_differences(df)
        if d is None:
            return None
        return len(d) > 0

    def _register_external_table(self, file_format: str, uri: str):
        try:
            self.spark.sql(f"create table if not exists {self.qualified_name} using {file_format} location '{uri}'")

        except Exception as e:
            DEFAULT_LOGGER.exception("could not register external table", extra={"label": self})
            raise e

    def _drop_external_table(self):
        DEFAULT_LOGGER.warning("remove external table from metastore", extra={"label": self})
        self.spark.sql(f"drop table if exists {self.qualified_name}")
