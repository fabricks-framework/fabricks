from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.runtime import VARIABLES
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_variables():
    """
    Deploy variables to the fabricks.variables view.
    """
    backslash = "\\"
    values = [f"('{key.replace(backslash, '')}', '{value}')" for key, value in VARIABLES.items()]

    ddl = "select * from values " + ", ".join(values) + " as t(key, value)"
    sql = f"""create or replace view fabricks.variables with schema evolution as {ddl}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.variables", extra={"sql": sql})
    SPARK.sql(sql)
