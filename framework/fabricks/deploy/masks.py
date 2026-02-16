from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.masks import register_all_masks


def deploy_masks(overwrite=True):
    DEFAULT_LOGGER.info("create or replace masks", extra={"label": "fabricks"})

    register_all_masks(overwrite=overwrite)
