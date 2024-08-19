import json
from typing import Optional, overload

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import Logger
from fabricks.core.jobs.base.checker import Checker
from fabricks.core.jobs.base.error import InvokerFailedException
from fabricks.core.schedules import get_schedule
from fabricks.utils.path import Path


class Invoker(Checker):
    def pre_run_invoke(self, schedule: Optional[str] = None):
        self._job_position_invoke(position="pre_run", schedule=schedule)
        self._step_position_invoke(position="pre_run", schedule=schedule)

    def post_run_invoke(self, schedule: Optional[str] = None):
        self._job_position_invoke(position="post_run", schedule=schedule)
        self._step_position_invoke(position="post_run", schedule=schedule)

    def _job_position_invoke(self, position: str, schedule: Optional[str] = None):
        if self.options.invoker.get(position):
            Logger.info(f"{position}-invoke", extra={"job": self})
            try:
                options = self.options.invoker.get_dict(position)
                assert options

                notebook = options.notebook  # type: ignore
                assert notebook, "notebook mandatory"
                path = PATH_RUNTIME.join(notebook)

                arguments = options.arguments or {}  # type: ignore
                timeout = arguments.get("timeout")
                if timeout is None:
                    if position == "pre_run":
                        timeout = self.timeouts.pre_run
                    elif position == "post_run":
                        timeout = self.timeouts.post_run

                self.invoke(path, arguments, timeout, schedule)
            except Exception:
                raise InvokerFailedException(position)

    def _step_position_invoke(self, position: str, schedule: Optional[str] = None):
        if self.step_conf.get("options", {}).get(position, None):
            Logger.info(f"{self.step} - {position}-invoke")
            try:
                options = self.step_conf.get("options", {}).get(position, None)
                assert options

                notebook = options.get("notebook")  # type: ignore
                assert notebook, "notebook mandatory"
                path = PATH_RUNTIME.join(notebook)

                arguments = options.get("arguments", {})  # type: ignore
                timeout = arguments.get("timeout")
                if timeout is None:
                    if position == "pre_run":
                        timeout = self.timeouts.pre_run
                    elif position == "post_run":
                        timeout = self.timeouts.post_run

                self.invoke(path, arguments, timeout, schedule)
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
        if path is None:
            notebook = self.options.invoker.get_dict("notebook")
            path = PATH_RUNTIME.join(notebook)
        assert path.exists(), f"{path} not found"

        if arguments is None:
            arguments = self.options.invoker.get_dict("arguments") or {}

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
