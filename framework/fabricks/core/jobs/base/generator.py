from abc import abstractmethod
from typing import Optional, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit

from fabricks.cdc import SCD1
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.configurator import Configurator
from fabricks.metastore.view import create_or_replace_global_temp_view


class Generator(Configurator):
    def update_dependencies(self):
        DEFAULT_LOGGER.info("update dependencies", extra={"job": self})

        df = self.get_dependencies()
        if df:
            scd1 = SCD1("fabricks", self.step, "dependencies")
            scd1.delete_missing(df, keys=["dependency_id"], update_where=f"job_id = '{self.job_id}'", uuid=True)

    def add_dependency_details(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("__parent", expr("replace(parent, '__current', '')"))
        df = df.withColumn("parent_id", expr("md5(__parent)"))
        df = df.withColumn("dependency_id", expr("md5(concat_ws('*', job_id, parent))"))
        df = df.drop("__parent")
        return df

    @abstractmethod
    def get_dependencies(self) -> DataFrame:
        raise NotImplementedError()

    def rm(self):
        """
        Removes the schema folder and checkpoints associated with the generator.

        If the schema folder exists, it will be deleted. The method also calls the `rm_checkpoints` method to remove any checkpoints associated with the generator.
        """
        if self.paths.schema.exists():
            DEFAULT_LOGGER.info("delete schema folder", extra={"job": self})
            self.paths.schema.rm()
        self.rm_checkpoints()

    def rm_checkpoints(self):
        """
        Removes the checkpoints folder if it exists.

        This method checks if the checkpoints folder exists and deletes it if it does.
        """
        if self.paths.checkpoints.exists():
            DEFAULT_LOGGER.info("delete checkpoints folder", extra={"job": self})
            self.paths.checkpoints.rm()

    def rm_commit(self, id: Union[str, int]):
        """
        Remove a commit with the given ID.

        Args:
            id (Union[str, int]): The ID of the commit to remove.

        Returns:
            None
        """
        path = self.paths.commits.join(str(id))
        if path.exists():
            DEFAULT_LOGGER.warning(f"delete commit {id}", extra={"job": self})
            path.rm()

    def truncate(self):
        """
        Truncates the job by removing all data associated with it.

        This method removes the job from the system and, if the `persist` flag is set to True,
        it also truncates the associated table.

        Returns:
            None
        """
        DEFAULT_LOGGER.warning("truncate", extra={"job": self})
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
                DEFAULT_LOGGER.warning(f"{row.count} children found", extra={"job": self, "content": row.children})

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
        raise NotImplementedError()

    def create_table(self):
        def _create_table(df: DataFrame, batch: Optional[int] = 0):
            df = self.base_transform(df)
            cdc_options = self.get_cdc_context(df)

            cluster_by = []
            partition_by = []

            powerbi = False
            liquid_clustering = False
            partitioning = False
            identity = False

            # first take from job options, then from step options
            job_powerbi = self.options.table.get_boolean("powerbi", None)
            step_powerbi = self.step_conf.get("table_options", {}).get("powerbi", None)
            if job_powerbi is not None:
                powerbi = job_powerbi
            elif step_powerbi is not None:
                powerbi = step_powerbi

            if powerbi:
                properties = {
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                    "fabricks.last_version": "0",
                }
            else:
                properties = {
                    "delta.enableDeletionVectors": "true",
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                    "delta.feature.timestampNtz": "supported",
                    "fabricks.last_version": "0",
                }

            if "__identity" in df.columns:
                identity = False
            else:
                identity = self.options.table.get_boolean("identity", False)

            # first take from job options, then from step options
            liquid_clustering_job = self.options.table.get_boolean("liquid_clustering", None)
            liquid_clustering_step = self.step_conf.get("table_options", {}).get("liquid_clustering", None)
            if liquid_clustering_job is not None:
                liquid_clustering = liquid_clustering_job
            elif liquid_clustering_step:
                liquid_clustering = liquid_clustering_step

            if liquid_clustering:
                cluster_by = self.options.table.get_list("cluster_by") or []
                if not cluster_by:
                    if "__source" in df.columns:
                        cluster_by.append("__source")
                    if "__is_current" in df.columns:
                        cluster_by.append("__is_current")
                    if "__key" in df.columns:
                        cluster_by.append("__key")
                    elif "__hash" in df.columns:
                        cluster_by.append("__hash")

                if not cluster_by:
                    DEFAULT_LOGGER.warning(
                        "liquid clustering disabled (no clustering columns found)", extra={"job": self}
                    )
                    liquid_clustering = False
                    cluster_by = None

            if not liquid_clustering:
                cluster_by = None
                partition_by = self.options.table.get_list("partition_by")
                if partition_by:
                    partitioning = True

            if not powerbi:
                # first take from job options, then from step options
                if self.options.table.get_dict("properties"):
                    properties = self.options.table.get_dict("properties")
                elif self.step_conf.get("table_options", {}).get("properties", {}):
                    properties = self.step_conf.get("table_options", {}).get("properties", {})

            # if dataframe, reference is passed (BUG)
            name = f"{self.step}_{self.topic}_{self.item}__init"
            global_temp_view = create_or_replace_global_temp_view(name=name, df=df.where("1 == 2"))
            sql = f"select * from {global_temp_view}"

            self.cdc.create_table(
                sql,
                identity=identity,
                liquid_clustering=liquid_clustering,
                cluster_by=cluster_by,
                partitioning=partitioning,
                partition_by=partition_by,
                properties=properties,
                **cdc_options,
            )

        if not self.table.exists():
            df = self.get_data(self.stream)
            if df:
                if self.stream:
                    # add dummy stream to be sure that the writeStream will start
                    spark = df.sparkSession

                    dummy_df = spark.readStream.table("fabricks.dummy")
                    # __metadata is always present
                    dummy_df = dummy_df.withColumn("__metadata", lit(None))
                    dummy_df = dummy_df.select("__metadata")

                    df = df.unionByName(dummy_df, allowMissingColumns=True)
                    path = self.paths.checkpoints.append("__init")
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

                constraints = self.options.table.get_dict("constraints")
                if constraints:
                    for key, value in constraints.items():
                        self.table.add_constraint(name=key, expr=value)

                comment = self.options.table.get("comment")
                if comment:
                    self.table.add_comment(comment=comment)

    def _update_schema(self, df: Optional[DataFrame] = None, overwrite: Optional[bool] = False):
        def _update_schema(df: DataFrame, batch: Optional[int] = None):
            context = self.get_cdc_context(df, reload=True)
            if overwrite:
                self.cdc.overwrite_schema(df, **context)
            else:
                self.cdc.update_schema(df, **context)

        if self.persist:
            if df is not None:
                _update_schema(df)
            else:
                df = self.get_data(self.stream)
                assert df is not None
                df = self.base_transform(df)

                if self.stream:
                    path = self.paths.checkpoints.append("__schema")
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

    def update_schema(self, df: Optional[DataFrame] = None):
        DEFAULT_LOGGER.info("update schema", extra={"job": self})
        self._update_schema(df=df, overwrite=False)

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        DEFAULT_LOGGER.info("overwrite schema", extra={"job": self})
        self._update_schema(df=df, overwrite=True)

    def get_differences_with_deltatable(self, df: Optional[DataFrame] = None):
        if df is None:
            df = self.get_data(self.stream)
            assert df is not None
            df = self.base_transform(df)

        context = self.get_cdc_context(df, reload=True)

        return self.cdc.get_differences_with_deltatable(df, **context)

    def schema_drifted(self, df: Optional[DataFrame] = None) -> Optional[bool]:
        if df is None:
            df = self.get_data(self.stream)
            assert df is not None
            df = self.base_transform(df)

        context = self.get_cdc_context(df, reload=True)

        return self.cdc.schema_drifted(df, **context)

    def enable_liquid_clustering(self):
        df = self.table.dataframe
        enable = False

        # first take from job options, then from step options
        enable_job = self.options.table.get_boolean("liquid_clustering", None)
        enable_step = self.step_conf.get("table_options", {}).get("liquid_clustering", None)
        if enable_job is not None:
            enable = enable_job
        elif enable_step:
            enable = enable_step

        if enable:
            cluster_by = self.options.table.get_list("cluster_by") or []
            if not cluster_by:
                if "__source" in df.columns:
                    cluster_by.append("__source")
                if "__is_current" in df.columns:
                    cluster_by.append("__is_current")
                if "__key" in df.columns:
                    cluster_by.append("__key")
                elif "__hash" in df.columns:
                    cluster_by.append("__hash")

                if len(cluster_by) > 0:
                    self.table.enable_liquid_clustering(cluster_by)
                else:
                    DEFAULT_LOGGER.warning(
                        "liquid clustering not enabled (no clustering column found)", extra={"job": self}
                    )

        else:
            DEFAULT_LOGGER.debug("liquid clustering not enabled", extra={"job": self})
