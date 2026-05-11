import logging
from typing import Callable, Optional, Union

from fabricks.context import FABRICKS_STORAGE, Steps
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.steps import get_step
from fabricks.deploy.masks import deploy_masks
from fabricks.deploy.notebooks import deploy_notebooks
from fabricks.deploy.runtime import deploy_runtime
from fabricks.deploy.schedules import deploy_schedules
from fabricks.deploy.tables import deploy_tables
from fabricks.deploy.udfs import deploy_udfs
from fabricks.deploy.utils import print_atomic_bomb
from fabricks.deploy.variables import deploy_variables
from fabricks.deploy.views import deploy_views
from fabricks.metastore.database import Database


class Deploy:
    @staticmethod
    def tables(drop: bool = False, update: bool = False):
        deploy_tables(drop=drop, update=update)

    @staticmethod
    def variables():
        deploy_variables(deploy_runtime_first=True)

    @staticmethod
    def runtime():
        deploy_runtime()

    @staticmethod
    def views():
        deploy_views()

    @staticmethod
    def udfs(overwrite=True):
        deploy_udfs(overwrite=overwrite)

    @staticmethod
    def masks(overwrite=True):
        deploy_masks(overwrite=overwrite)

    @staticmethod
    def notebooks(overwrite=False):
        deploy_notebooks(overwrite=overwrite)

    @staticmethod
    def schedules():
        deploy_schedules()

    @staticmethod
    def step(step: str):
        Deploy.tables()
        s = get_step(step)
        s.create()

        Deploy.views()
        Deploy.schedules()

    @staticmethod
    def job(step: str):
        s = get_step(step)
        s.create()

    @staticmethod
    def armageddon(steps: Optional[Union[str, list[str]]] = None, nowait: bool = False):
        def _call(func: Callable, operation: str):
            try:
                func()
            except Exception as e:
                DEFAULT_LOGGER.exception(f"fail to deploy {operation}", extra={"label": "armageddon"})
                errors.append({"operation": operation, "error": e})

        DEFAULT_LOGGER.warning("(╯°□°）╯︵ ┻━┻", extra={"label": "armageddon"})
        print_atomic_bomb(nowait=nowait)

        DEFAULT_LOGGER.setLevel(logging.INFO)

        if steps is None:
            steps = Steps

        assert steps is not None

        if isinstance(steps, str):
            steps = [steps]
        elif isinstance(steps, list):
            steps = [s for s in steps]

        fabricks = Database("fabricks")
        fabricks.drop()

        for s in steps:
            step = get_step(s)
            step.drop()

        tmp = FABRICKS_STORAGE.joinpath("tmp")
        tmp.rm()

        checkpoint = FABRICKS_STORAGE.joinpath("checkpoints")
        checkpoint.rm()

        schema = FABRICKS_STORAGE.joinpath("schemas")
        schema.rm()

        schedule = FABRICKS_STORAGE.joinpath("schedules")
        schedule.rm()

        fabricks.create()

        errors = []

        _call(lambda: Deploy.tables(drop=True), "deploy-tables")
        _call(lambda: Deploy.udfs(overwrite=True), "deploy-udfs")
        _call(lambda: Deploy.masks(overwrite=True), "deploy-masks")
        _call(lambda: Deploy.notebooks(overwrite=True), "deploy-notebooks")

        for s in steps:
            _call(lambda s=s: get_step(s).create(), f"create-{s}")

        _call(Deploy.views, "deploy-views")
        _call(Deploy.schedules, "deploy-schedules")
        _call(Deploy.variables, "deploy-variables")
        _call(Deploy.runtime, "deploy-runtime")

        if errors:
            raise ValueError(f"armageddon completed with {len(errors)} error(s). Check logs for details")
