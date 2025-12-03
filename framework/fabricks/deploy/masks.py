from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.masks import register_all_masks


def deploy_masks(override: bool = True):
    DEFAULT_LOGGER.info("create or replace masks")

    register_all_masks(override=override)
