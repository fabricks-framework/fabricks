import logging
from typing import cast

from fabricks.context.runtime import FABRICKS_STORAGE, SECRET_SCOPE
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import get_logger
from fabricks.utils.secret import AccessKey, get_secret_from_secret_scope

storage_account = FABRICKS_STORAGE.get_storage_account()
secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
access_key = cast(AccessKey, secret).key

table = AzureTable("dags", storage_account=storage_account, access_key=access_key)
DagsLogger, DagsTableLogger = get_logger("dags", logging.DEBUG, table=table)
