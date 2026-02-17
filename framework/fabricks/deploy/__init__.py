import logging
from typing import Optional, Union

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
        deploy_variables()

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
        DEFAULT_LOGGER.warning("!💥 armageddon 💥!", extra={"label": "fabricks"})
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

        Deploy.tables(drop=True)
        Deploy.udfs(overwrite=True)
        Deploy.masks(overwrite=True)
        Deploy.notebooks(overwrite=True)

        for s in steps:
            step = get_step(s)
            step.create()

        Deploy.views()
        Deploy.schedules()
        Deploy.variables()
        Deploy.runtime()
