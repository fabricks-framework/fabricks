import logging
from typing import Optional, Union

from fabricks.context import FABRICKS_STORAGE, Steps
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.steps.base import BaseStep
from fabricks.deploy.masks import deploy_masks
from fabricks.deploy.notebooks import deploy_notebooks
from fabricks.deploy.schedules import deploy_schedules
from fabricks.deploy.tables import deploy_tables
from fabricks.deploy.udfs import deploy_udfs
from fabricks.deploy.utils import print_atomic_bomb
from fabricks.deploy.views import deploy_views
from fabricks.metastore.database import Database


class Deploy:
    @staticmethod
    def tables(drop: bool = False):
        deploy_tables(drop=drop)

    @staticmethod
    def views():
        deploy_views()

    @staticmethod
    def udfs(override: bool = True):
        deploy_udfs(override=override)

    @staticmethod
    def masks(override: bool = True):
        deploy_masks(override=override)

    @staticmethod
    def notebooks():
        deploy_notebooks()

    @staticmethod
    def schedules():
        deploy_schedules()

    @staticmethod
    def armageddon(steps: Optional[Union[str, list[str]]] = None, nowait: bool = False):
        DEFAULT_LOGGER.warning("!💥 armageddon 💥!")
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
            step = BaseStep(s)
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
        Deploy.udfs()
        Deploy.masks()
        Deploy.notebooks()

        for s in steps:
            step = BaseStep(s)
            step.create()

        Deploy.views()
        Deploy.schedules()
