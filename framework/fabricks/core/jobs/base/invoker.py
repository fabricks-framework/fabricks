import json
from typing import Optional

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.checker import Checker
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.core.schedules import get_schedules
from fabricks.utils.path import Path


class Invoker(Checker):
    def invoke(self, schedule: Optional[str] = None):
        self._invoke_job(position="run", schedule=schedule)

    def invoke_pre_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="pre_run", schedule=schedule)
        self._invoke_step(position="pre_run", schedule=schedule)

    def invoke_post_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="post_run", schedule=schedule)
        self._invoke_step(position="post_run", schedule=schedule)

    def _invoke_job(self, position: str, schedule: Optional[str] = None):
        invokers = self.options.invokers.get_list(position)

        errors = []

        if invokers:
            for i in invokers:
                DEFAULT_LOGGER.info(f"{position}-invoke", extra={"job": self})
                try:
                    notebook = i.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.joinpath(notebook)

                    arguments = i.get("arguments") or {}
                    timeout = i.get("timeout")

                    self._run_notebook(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                    )

                except Exception as e:
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
            for i in invokers:
                DEFAULT_LOGGER.info(f"{position}-invoke", extra={"step": self.step})
                try:
                    notebook = i.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.joinpath(notebook)

                    arguments = i.get("arguments", {})
                    timeout = i.get("timeout")

                    self._run_notebook(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                    )

                except Exception as e:
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
            variables = (
                next(s for s in get_schedules() if s.get("name") == schedule).get("options", {}).get("variables", {})
            )

        if variables is None:
            variables = {}

        if arguments is None:
            arguments = {}

        dbutils.notebook.run(
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
            DEFAULT_LOGGER.info(f"calling {name}", extra={"job": self})
            arguments = e.get("arguments") or {}

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend_step(self, df: DataFrame) -> DataFrame:
        from fabricks.core.extenders import get_extender

        extenders = self.step_conf.get("extender_options", {})
        for e in extenders:
            name = e.get("extender")
            DEFAULT_LOGGER.info(f"calling {name}", extra={"step": self.step})
            arguments = e.get("arguments", {})

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend(self, df: DataFrame) -> DataFrame:
        df = self.extend_job(df)
        df = self.extend_step(df)
        return df
