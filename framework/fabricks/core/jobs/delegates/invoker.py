from __future__ import annotations

import json
from typing import Any, Optional

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.extenders import get_extender
from fabricks.core.jobs.get_schedule import get_schedule
from fabricks.core.jobs.protocols import InvocableJob
from fabricks.models.common import BaseInvokerOptions, ExtenderOptions
from fabricks.models.exceptions import CustomException


class PreRunInvokeException(CustomException):
    pass


class PostRunInvokeException(CustomException):
    pass


class JobInvoker:
    def __init__(self, job: InvocableJob):
        self._job = job

    def invoke(self, schedule: Optional[str] = None, **kwargs):
        return self._invoke_job(position="run", schedule=schedule, **kwargs)

    def invoke_pre_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="pre_run", schedule=schedule)
        self._invoke_step(position="pre_run", schedule=schedule)

    def invoke_post_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="post_run", schedule=schedule)
        self._invoke_step(position="post_run", schedule=schedule)

    def _invoke_notebook(
        self,
        invoker: dict[str, Any] | BaseInvokerOptions,
        schedule: Optional[str] = None,
        **kwargs,
    ):
        path = kwargs.get("path")
        if path is None:
            notebook = invoker.get("notebook") if isinstance(invoker, dict) else invoker.notebook
            assert notebook, "notebook mandatory"
            path = PATH_RUNTIME.joinpath(notebook)

        assert path is not None, "path could not be resolved"

        timeout = invoker.get("timeout") if isinstance(invoker, dict) else invoker.timeout
        arguments = invoker.get("arguments") if isinstance(invoker, dict) else invoker.arguments
        arguments = arguments or {}

        schema_only = kwargs.get("schema_only")
        if schema_only is not None:
            arguments["schema_only"] = schema_only

        return self._run_notebook(
            path=path,
            arguments=arguments,
            schedule=schedule,
            timeout=timeout,
        )

    def _invoke_job(self, position: str, schedule: Optional[str] = None, **kwargs):
        invokers = getattr(self._job.config.invoker_options, position, None) or [] if self._job.config.invoker_options else []
        if position == "run":
            invokers = invokers if len(invokers) > 0 else [{}]

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke ({i}, {position})", extra={"label": self._job})
                try:
                    if len(invokers) == 1 and position == "run":
                        return self._invoke_notebook(invoker, schedule=schedule, **kwargs)
                    else:
                        self._invoke_notebook(invoker=invoker, schedule=schedule, **kwargs)

                except Exception as e:
                    DEFAULT_LOGGER.warning(f"fail to run invoker ({i}, {position})", extra={"label": self._job})

                    if position == "pre_run":
                        errors.append(PreRunInvokeException(e))
                    elif position == "post_run":
                        errors.append(PostRunInvokeException(e))
                    else:
                        errors.append(e)

        if errors:
            raise Exception(errors)

    def _invoke_step(self, position: str, schedule: Optional[str] = None):
        step_invoker_options = self._job.config.step_conf.invoker_options if self._job.config.step_conf else None
        invokers = getattr(step_invoker_options, position, []) if step_invoker_options else []

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke by step ({i}, {position})", extra={"label": self._job})
                try:
                    self._invoke_notebook(invoker=invoker, schedule=schedule)

                except Exception as e:
                    DEFAULT_LOGGER.warning(
                        f"fail to run invoker by step ({i}, {position})", extra={"label": self._job}
                    )

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
        path,
        arguments: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
        schedule: Optional[str] = None,
    ):
        from databricks.sdk.runtime import dbutils

        for file_format in [None, ".py", ".ipynb"]:
            path_with_file_format = path.append(file_format) if file_format else path
            if path_with_file_format.exists():
                path = path_with_file_format
                break

        if timeout is None:
            timeout = self._job.config.timeout

        assert timeout is not None

        variables = None
        if schedule is not None:
            variables = get_schedule(name=schedule).get("options", {}).get("variables", {})

        if variables is None:
            variables = {}

        if arguments is None:
            arguments = {}

        return dbutils.notebook.run(
            path=path.get_notebook_path(),  # pyright: ignore[reportCallIssue]
            timeout_seconds=timeout,  # pyright: ignore[reportCallIssue]
            arguments={  # pyright: ignore[reportCallIssue]
                "step": self._job.config.step,
                "topic": self._job.config.topic,
                "item": self._job.config.item,
                **arguments,
                "job_options": json.dumps(self._job.options.model_dump()),
                "schedule_variables": json.dumps(variables),
            },
        )

    def extend_job(self, df: DataFrame) -> DataFrame:
        extenders = self._job.config.extender_options or []
        return self._extend(df, extenders, extended="job")

    def extend_step(self, df: DataFrame) -> DataFrame:
        extenders = self._job.config.step_conf.extender_options or [] if self._job.config.step_conf else []
        return self._extend(df, extenders, extended="step")

    def _extend(self, df: DataFrame, extenders: list[ExtenderOptions], extended: str) -> DataFrame:
        for e in extenders:
            name = e.extender
            DEFAULT_LOGGER.debug(f"extend {extended} ({name})", extra={"label": self._job})
            arguments = e.arguments or {}
            extender = get_extender(name)
            df = extender(df, **arguments)
        return df

    def extend(self, df: DataFrame) -> DataFrame:
        df = self.extend_job(df)
        df = self.extend_step(df)
        return df


# Backward-compatible name
Invoker = JobInvoker
