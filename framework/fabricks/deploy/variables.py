from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_variables(deploy_runtime_first: bool = True):
    """
    Deploy variables to the fabricks.variables view.
    """
    if deploy_runtime_first:
        from fabricks.deploy.runtime import deploy_runtime

        deploy_runtime()

    ddl = """
    with variables as (
      select
        runtime:runtime:variables as variables
      from
        fabricks.runtime
    )
    select
      explode(variant_get(variables, '$', 'MAP<STRING, STRING>'))
    from
      variables
    """
    sql = f"""create or replace view fabricks.variables with schema evolution as {ddl}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.variables", extra={"sql": sql})
    SPARK.sql(sql)
