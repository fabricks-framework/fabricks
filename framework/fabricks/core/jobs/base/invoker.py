import json
from typing import Optional

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.checker import Checker
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.core.jobs.get_schedule import get_schedule
from fabricks.utils.path import Path


class Invoker(Checker):
    def invoke(self, schedule: Optional[str] = None, **kwargs):
        return self._invoke_job(
            position="run",
            schedule=schedule,
            **kwargs,
        )  # kwargs and return needed for get_data in gold

    def invoke_pre_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="pre_run", schedule=schedule)
        self._invoke_step(position="pre_run", schedule=schedule)

    def invoke_post_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="post_run", schedule=schedule)
        self._invoke_step(position="post_run", schedule=schedule)

    def _invoke_job(self, position: str, schedule: Optional[str] = None, **kwargs):
        invokers = self.options.invokers.get_list(position)
        if position == "run":
            invokers = invokers if len(invokers) > 0 else [{}]  # run must work even without run invoker options

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke ({i}, {position})", extra={"label": self})
                try:
                    path = kwargs.get("path")
                    if path is None:
                        notebook = invoker.get("notebook")
                        assert notebook, "notebook mandatory"
                        path = PATH_RUNTIME.joinpath(notebook)

                    assert path is not None, "path mandatory"

                    arguments = invoker.get("arguments") or {}
                    timeout = invoker.get("timeout")

                    schema_only = kwargs.get("schema_only")
                    if schema_only is not None:
                        arguments["schema_only"] = schema_only

                    if len(invokers) == 1 and position == "run":
                        return self._run_notebook(
                            path=path,
                            arguments=arguments,
                            timeout=timeout,
                            schedule=schedule,
                        )
                    else:
                        self._run_notebook(
                            path=path,
                            arguments=arguments,
                            timeout=timeout,
                            schedule=schedule,
                        )

                except Exception as e:
                    DEFAULT_LOGGER.warning(f"fail to run invoker ({i}, {position})", extra={"label": self})

                    if position == "pre_run":
                        errors.append(PreRunInvokeException(e))
                    elif position == "post_run":
                        errors.append(PostRunInvokeException(e))
                    else:
                        errors.append(e)

        if errors:
            raise Exception(errors)

    def _invoke_step(self, position: str, schedule: Optional[str] = None):
        invokers = self.step_conf.get("invoker_options", {}).get(position, [])

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke by step ({i}, {position})", extra={"label": self})
                try:
                    notebook = invoker.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.joinpath(notebook)

                    arguments = invoker.get("arguments", {})
                    timeout = invoker.get("timeout")

                    self._run_notebook(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                    )

                except Exception as e:
                    DEFAULT_LOGGER.warning(f"fail to run invoker by step ({i}, {position})", extra={"label": self})

                    if position == "pre_run":
                        errors.append(PreRunInvokeException(e))
                    elif position == "post_run":
                        errors.append(PostRunInvokeException(e))
                    else:
                        errors.append(e)

        if errors:
            raise Exception(errors)

    def _run_notebook(
        self,
        path: Path,
        arguments: Optional[dict] = None,
        timeout: Optional[int] = None,
        schedule: Optional[str] = None,
    ):
        """
        Invokes a notebook job.

        Args:
            path (Optional[Path]): The path to the notebook file. If not provided, it will be retrieved from the invoker options.
            arguments (Optional[dict]): Additional arguments to pass to the notebook job. If not provided, it will be retrieved from the invoker options.
            schedule (Optional[str]): The schedule for the job. If provided, schedule variables will be retrieved.

        Raises:
            AssertionError: If the specified path does not exist.

        """
        from databricks.sdk.runtime import dbutils

        for file_format in [None, ".py", ".ipynb"]:
            path_with_file_format = path.append(file_format) if file_format else path
            if path_with_file_format.exists():
                path = path_with_file_format
                break

        if timeout is None:
            timeout = self.timeout

        assert timeout is not None

        variables = None
        if schedule is not None:
            variables = get_schedule(name=schedule).get("options", {}).get("variables", {})

        if variables is None:
            variables = {}

        if arguments is None:
            arguments = {}

        return dbutils.notebook.run(
            path=path.get_notebook_path(),  # type: ignore
            timeout_seconds=timeout,  # type: ignore
            arguments={  # type: ignore
                "step": self.step,
                "topic": self.topic,
                "item": self.item,
                **arguments,
                "job_options": json.dumps(self.options.job.options),
                "schedule_variables": json.dumps(variables),
            },
        )

    def extend_job(self, df: DataFrame) -> DataFrame:
        from fabricks.core.extenders import get_extender

        extenders = self.options.extenders
        for e in extenders:
            name = e.get("extender")
            DEFAULT_LOGGER.debug(f"extend ({name})", extra={"label": self})
            arguments = e.get("arguments") or {}

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend_step(self, df: DataFrame) -> DataFrame:
        from fabricks.core.extenders import get_extender

        extenders = self.step_conf.get("extender_options", {})
        for e in extenders:
            name = e.get("extender")
            DEFAULT_LOGGER.debug(f"extend by step ({name})", extra={"label": self})
            arguments = e.get("arguments", {})

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend(self, df: DataFrame) -> DataFrame:
        df = self.extend_job(df)
        df = self.extend_step(df)
        return df
