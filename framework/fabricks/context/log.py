import logging
from functools import wraps
from typing import Callable, cast

from framework.fabricks.context.runtime import FABRICKS_STORAGE, SECRET_SCOPE
from framework.fabricks.utils.azure_table import AzureTable
from framework.fabricks.utils.log import get_logger
from framework.fabricks.utils.secret import AccessKey, get_secret_from_secret_scope

storage_account = FABRICKS_STORAGE.get_storage_account()
secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
access_key = cast(AccessKey, secret).key

table = AzureTable("logs", storage_account=storage_account, access_key=access_key)
Logger, TableLogger = get_logger("logs", logging.DEBUG, table=table)


def flush(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            TableLogger.flush()

    return wrapper
