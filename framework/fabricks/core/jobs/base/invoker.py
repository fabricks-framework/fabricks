from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_fixed

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.extenders import get_extender
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.core.jobs.get_schedule import get_schedule
from fabricks.models.common import BaseInvokerOptions, ExtenderOptions
from fabricks.utils.path import GitPath

if TYPE_CHECKING:
    from fabricks.core.jobs.base.job import BaseJob

# Exception names a `retry_on_error` invoker option can pick from to retry
# only on those types. See _is_transient below.
_KNOWN_RETRY_EXCEPTIONS: dict[str, type[BaseException]] = {
    "Py4JJavaError": Py4JJavaError,
    "ConnectionError": ConnectionError,
    "TimeoutError": TimeoutError,
    "OSError": OSError,
    "RuntimeError": RuntimeError,
}


def _is_transient(include_names: list[str] | None) -> retry_if_exception:
    # No names given: retry on any exception, transient or not. Named:
    # retry ONLY on those types.
    if not include_names:
        return retry_if_exception(lambda _e: True)

    unknown = [name for name in include_names if name not in _KNOWN_RETRY_EXCEPTIONS]
    allowed = sorted(_KNOWN_RETRY_EXCEPTIONS)
    assert not unknown, f"retry_on_error: unknown exception name(s) {unknown}, must be one of {allowed}"

    include_types = tuple(_KNOWN_RETRY_EXCEPTIONS[name] for name in include_names)
    return retry_if_exception(lambda e: isinstance(e, include_types))


def _warn_on_error(invoker: dict | BaseInvokerOptions) -> bool | None:
    return invoker.get("warn_on_error") if isinstance(invoker, dict) else invoker.warn_on_error


def _get_invoker_option(invoker: dict | BaseInvokerOptions, key: str) -> Any:  # noqa: ANN401 - heterogeneous options bag
    return invoker.get(key) if isinstance(invoker, dict) else getattr(invoker, key)


def _raise_invoke_errors(position: str, errors: list[Exception]) -> None:
    # str(Exception(errors)) on a list of exception objects falls back to
    # each one's repr, which drops the real message (e.g. Py4JJavaError's
    # repr is just the bare gateway call description) -- join str() of each
    # instead so the actual cause survives. Raise the typed exception (not a
    # bare Exception) so job.py's run() can tell an invoker failure apart
    # from a real run failure and skip the table restore for it.
    message = "; ".join(str(e) for e in errors)
    if position == "pre_run":
        raise PreRunInvokeException(message)
    if position == "post_run":
        raise PostRunInvokeException(message)
    raise Exception(message)


class JobInvoker:
    """Notebook invocation (pre/post-run, per job and per step) and extenders."""

    def __init__(self, job: BaseJob) -> None:
        self.job = job

    def pre_run(self, schedule: str | None = None) -> None:
        self.invoke_job(position="pre_run", schedule=schedule)
        self._invoke_step(position="pre_run", schedule=schedule)

    def post_run(self, schedule: str | None = None) -> None:
        self.invoke_job(position="post_run", schedule=schedule)
        self._invoke_step(position="post_run", schedule=schedule)

    def _invoke_notebook(
        self,
        invoker: dict | BaseInvokerOptions,
        schedule: str | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded to notebook invokers
    ) -> str:
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

        retry = _get_invoker_option(invoker, "retry")
        retry_on_error = _get_invoker_option(invoker, "retry_on_error")

        return self._run_notebook(
            path=path,
            arguments=arguments,
            schedule=schedule,
            timeout=timeout,
            retry=retry,
            retry_on_error=retry_on_error,
        )

    def invoke_job(
        self,
        position: str,
        schedule: str | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded to notebook invokers
    ) -> str | None:
        invoker_options = self.job._resolver.invoker_options
        invokers = getattr(invoker_options, position, None) or [] if invoker_options else []
        if position == "run":
            invokers = invokers if len(invokers) > 0 else [{}]  # run must work even without run invoker options

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke ({i}, {position})", extra={"label": self.job})
                try:
                    if len(invokers) == 1 and position == "run":
                        return self._invoke_notebook(invoker, schedule=schedule, **kwargs)
                    self._invoke_notebook(invoker=invoker, schedule=schedule, **kwargs)

                except Exception as e:
                    if _warn_on_error(invoker) is True:
                        DEFAULT_LOGGER.warning(f"invoker failed, ignored ({i}, {position})", extra={"label": self.job})
                        continue

                    DEFAULT_LOGGER.warning(f"fail to run invoker ({i}, {position})", extra={"label": self.job})

                    if position == "pre_run":
                        errors.append(PreRunInvokeException(e))
                    elif position == "post_run":
                        errors.append(PostRunInvokeException(e))
                    else:
                        errors.append(e)

        if errors:
            _raise_invoke_errors(position, errors)
        return None

    def _invoke_step(self, position: str, schedule: str | None = None) -> None:
        invokers = (
            getattr(self.job.step_conf.invoker_options, position, []) if self.job.step_conf.invoker_options else []
        )

        errors = []

        if invokers:
            for i, invoker in enumerate(invokers):
                DEFAULT_LOGGER.debug(f"invoke by step ({i}, {position})", extra={"label": self.job})
                try:
                    self._invoke_notebook(invoker=invoker, schedule=schedule)

                except Exception as e:
                    if _warn_on_error(invoker) is True:
                        DEFAULT_LOGGER.warning(
                            f"invoker by step failed, ignored ({i}, {position})", extra={"label": self.job}
                        )
                        continue

                    DEFAULT_LOGGER.warning(f"fail to run invoker by step ({i}, {position})", extra={"label": self.job})

                    if position == "pre_run":
                        errors.append(PreRunInvokeException(e))
                    elif position == "post_run":
                        errors.append(PostRunInvokeException(e))
                    else:
                        errors.append(e)

        if errors:
            _raise_invoke_errors(position, errors)

    def _run_notebook(
        self,
        path: GitPath,
        arguments: dict | None = None,
        timeout: int | None = None,
        schedule: str | None = None,
        retry: bool | None = None,
        retry_on_error: list[str] | None = None,
    ) -> str:
        """
        Invokes a notebook job.

        Args:
            path (Optional[GitPath]): The path to the notebook file. If not provided, it will be
                retrieved from the invoker options.
            arguments (Optional[dict]): Additional arguments to pass to the notebook job. If not
                provided, it will be retrieved from the invoker options.
            schedule (Optional[str]): The schedule for the job. If provided, schedule variables will be retrieved.
            retry (Optional[bool]): Retry once on a failure instead of raising immediately. Off by
                default. With no `retry_on_error`, retries on any exception; with it, retries only
                on the named exception type(s).
            retry_on_error (Optional[list[str]]): Names of exception types to retry on (e.g.
                ["Py4JJavaError", "TimeoutError"]). See _KNOWN_RETRY_EXCEPTIONS for the allowed names.

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
            timeout = self.job._resolver.timeout

        assert timeout is not None

        variables = None
        if schedule is not None:
            variables = get_schedule(name=schedule).get("options", {}).get("variables", {})

        if variables is None:
            variables = {}

        if arguments is None:
            arguments = {}

        def call() -> str:
            return dbutils.notebook.run(
                path=path.get_notebook_path(),  # type: ignore
                timeout_seconds=timeout,  # type: ignore
                arguments={  # type: ignore
                    "step": self.job.step,
                    "topic": self.job.topic,
                    "item": self.job.item,
                    **arguments,
                    "job_options": json.dumps(self.job.options.model_dump()),
                    "schedule_variables": json.dumps(variables),
                },
            )

        if not retry:
            return call()

        retrying = Retrying(
            stop=stop_after_attempt(2), wait=wait_fixed(60), retry=_is_transient(retry_on_error), reraise=True
        )
        return retrying(call)

    def extend_job(self, df: DataFrame) -> DataFrame:
        extenders = self.job._resolver.extender_options or []
        return self._extend(df, extenders, extended="job")

    def extend_step(self, df: DataFrame) -> DataFrame:
        extenders = self.job.step_conf.extender_options or []
        return self._extend(df, extenders, extended="step")

    def _extend(self, df: DataFrame, extenders: list[ExtenderOptions], extended: str) -> DataFrame:
        for e in extenders:
            name = e.extender
            DEFAULT_LOGGER.debug(f"extend {extended} ({name})", extra={"label": self.job})
            arguments = e.arguments or {}

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend(self, df: DataFrame) -> DataFrame:
        df = self.extend_job(df)
        return self.extend_step(df)
