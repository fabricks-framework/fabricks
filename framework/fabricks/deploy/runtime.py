import json

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.runtime import CONF_RUNTIME
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_runtime():
    """
    Deploy runtime to the fabricks.runtime view.
    """
    runtime = CONF_RUNTIME.model_dump()
    content = json.dumps(runtime, default=str).replace("\\\\$", "").replace("$", "").replace("\\", "\\\\")
    ddl = """select parse_json('{"runtime": """ + content + "}') as runtime"

    sql = f"""create or replace view fabricks.runtime with schema evolution as {ddl}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.runtime", extra={"sql": sql})
    SPARK.sql(sql)
