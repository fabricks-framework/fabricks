from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.schedules import create_or_replace_views
from fabricks.core.views import create_or_replace_views as create_or_replace_custom_views


def deploy_schedules():
    DEFAULT_LOGGER.info("create or replace schedules")

    create_or_replace_custom_views()
    create_or_replace_views()
