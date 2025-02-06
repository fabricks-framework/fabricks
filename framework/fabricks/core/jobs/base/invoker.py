import json
from typing import Optional, overload

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import Logger
from fabricks.core.jobs.base.checker import Checker
from fabricks.core.jobs.base.error import InvokerFailedException
from fabricks.core.schedules import get_schedule
from fabricks.utils.path import Path


class Invoker(Checker):
    def invoke_pre_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="pre_run", schedule=schedule)
        self._invoke_step(position="pre_run", schedule=schedule)

    def invoke_post_run(self, schedule: Optional[str] = None):
        self._invoke_job(position="post_run", schedule=schedule)
        self._invoke_step(position="post_run", schedule=schedule)

    def _invoke_job(self, position: str, schedule: Optional[str] = None):
        invokers = [i for i in self.options.invokers if i.get("position") == position]

        if invokers:
            for i in invokers:
                Logger.info(f"{position}-invoke", extra={"job": self})
                try:
                    options = i.get_dict("options")
                    assert options

                    notebook = options.notebook
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.join(notebook)

                    arguments = options.arguments or {}

                    if position == "pre_run":
                        timeout = self.timeouts.pre_run
                    elif position == "post_run":
                        timeout = self.timeouts.post_run
                    else:
                        timeout = None

                    self.invoke(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                    )

                except Exception:
                    raise InvokerFailedException(position)

    def _invoke_step(self, position: str, schedule: Optional[str] = None):
        invokers = [i for i in self.step_conf.get("invokers", []) if i.get("position") == position]

        if invokers:
            for i in invokers:
                Logger.info(f"{self.step} - {position}-invoke")
                try:
                    options = i.get_dict("options")
                    assert options

                    notebook = options.get("notebook")
                    assert notebook, "notebook mandatory"
                    path = PATH_RUNTIME.join(notebook)

                    arguments = options.get("arguments", {})

                    if position == "pre_run":
                        timeout = self.timeouts.pre_run
                    elif position == "post_run":
                        timeout = self.timeouts.post_run

                    self.invoke(
                        path=path,
                        arguments=arguments,
                        timeout=timeout,
                        schedule=schedule,
                    )

                except Exception:
                    raise InvokerFailedException(position)

    @overload
    def invoke(self, path: Path, arguments: dict, timeout: Optional[int] = None, schedule: Optional[str] = None): ...

    @overload
    def invoke(self, *, schedule: Optional[str] = None): ...

    def invoke(
        self,
        path: Optional[Path] = None,
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
        if path is None or arguments is None or timeout is None or schedule is None:
            invoker = [i for i in self.step_conf.get("invokers", []) if i.get("position") == "run"]
            assert len(invoker) == 1, "Only one run invoker is allowed"
            invoker = invoker[0].get_dict("options")

        if path is None:
            notebook = invoker.get("notebook")
            path = PATH_RUNTIME.join(notebook)

        assert path.exists(), f"{path} not found"

        if arguments is None:
            arguments = invoker.get_dict("arguments") or {}

        if schedule is not None:
            variables = get_schedule(schedule).select("options.variables").collect()[0][0]
        else:
            variables = {}

        if timeout is None:
            timeout = self.timeouts.job

        self.dbutils.notebook.run(
            path.get_notebook_path(),
            timeout,
            {
                "step": self.step,
                "topic": self.topic,
                "item": self.item,
                **arguments,
                "job_options": json.dumps(self.options.job.options),
                "schedule_variables": json.dumps(variables),
            },
        )

    def _job_extender(self, df: DataFrame) -> DataFrame:
        from fabricks.core.extenders import get_extender

        extenders = self.options.extenders

        for e in extenders:
            name = e.get("extender")
            arguments = e.get_dict("arguments")

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def _step_extender(self, df: DataFrame) -> DataFrame:
        from fabricks.core.extenders import get_extender

        extenders = self.options.extenders

        for e in extenders:
            name = e.get("extender")
            arguments = e.get_dict("arguments")

            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extender(self, df: DataFrame) -> DataFrame:
        extenders = self.options.extenders

        for e in extenders:
            if not name:
                name = self.step_conf.get("extender_options", {}).get("extender", None)

            if name:
                from fabricks.core.extenders import get_extender

                Logger.debug(f"extend ({name})", extra={"job": self})

                arguments = self.options.extenders.get("arguments")
                if arguments is None:
                    arguments = self.step_conf.get("extender_options", {}).get("arguments", None)
                if arguments is None:
                    arguments = {}

                extender = get_extender(name)
                df = extender(df, **arguments)

            return df
