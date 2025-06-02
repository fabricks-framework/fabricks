import json
from typing import Optional, overload

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.checker import Checker
from fabricks.core.jobs.base.error import PostRunInvokeException, PreRunInvokeException
from fabricks.core.schedules import get_schedules
from fabricks.utils.path import Path


class Invoker(Checker):
    def invoke_pre_run(self, schedule: Optional[str] = None):
        self.invoke_job(position="pre_run", schedule=schedule)
        self.invoke_step(position="pre_run", schedule=schedule)

    def invoke_post_run(self, schedule: Optional[str] = None):
        self.invoke_job(position="post_run", schedule=schedule)
        self.invoke_step(position="post_run", schedule=schedule)

    def invoke_job(self, position: str, schedule: Optional[str] = None):
        invokers = self.options.invokers.get_list(position)

        if invokers:
            for i in invokers:
                DEFAULT_LOGGER.info(f"{position}-invoke", extra={"job": self})
                try:
                    notebook = i.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.join(notebook)

                    arguments = i.get("arguments") or {}
                    timeout = i.get("timeout")

                    self.invoke(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                        position=position,
                    )

                except Exception as e:
                    if position == "pre_run":
                        raise PreRunInvokeException(e)
                    elif position == "post_run":
                        raise PostRunInvokeException(e)
                    else:
                        raise e

    def invoke_step(self, position: str, schedule: Optional[str] = None):
        invokers = self.step_conf.get("invoker_options", {}).get(position, [])

        if invokers:
            for i in invokers:
                DEFAULT_LOGGER.info(f"{position}-invoke", extra={"step": self.step})
                try:
                    notebook = i.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.join(notebook)

                    arguments = i.get("arguments", {})
                    timeout = i.get("timeout")

                    self.invoke(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                        position=position,
                    )

                except Exception as e:
                    if position == "pre_run":
                        raise PreRunInvokeException(e)
                    elif position == "post_run":
                        raise PostRunInvokeException(e)
                    else:
                        raise e

    @overload
    def invoke(
        self,
        path: Path,
        arguments: dict,
        timeout: Optional[int] = None,
        schedule: Optional[str] = None,
        position: Optional[str] = None,
    ): ...

    @overload
    def invoke(self, *, schedule: Optional[str] = None): ...

    def invoke(
        self,
        path: Optional[Path] = None,
        arguments: Optional[dict] = None,
        timeout: Optional[int] = None,
        schedule: Optional[str] = None,
        position: Optional[str] = None,
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

        if position is None:
            invokers = self.options.invokers.get_list("run")
            assert len(invokers) == 1, "Only one run invoker is allowed"
            invoker = invokers[0]

            notebook = invoker.get("notebook")
            path = PATH_RUNTIME.join(notebook)
            assert path.exists(), f"{path} not found"

            arguments = invoker.get("arguments", {})
            timeout = invoker.get("timeout")

        if timeout is None:
            timeout = self.timeout

        variables = None
        if schedule is not None:
            variables = (
                next(s for s in get_schedules() if s.get("name") == schedule).get("options", {}).get("variables", {})
            )

        if variables is None:
            variables = {}

        if arguments is None:
            arguments = {}

        assert path is not None
        assert timeout is not None

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
