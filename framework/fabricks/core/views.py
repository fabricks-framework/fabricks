from databricks.sdk.runtime import spark

from fabricks.context import PATH_VIEWS
from fabricks.context.log import Logger
from fabricks.utils.path import Path
from fabricks.utils.sqlglot import fix as fix_sql


def _create_or_replace_view(path: Path):
    sql = path.get_sql()
    file_name = path.get_file_name().split(".")[0]
    sql = f"""
    create or replace view fabricks.{file_name}
    as
    {sql}
    """
    sql = fix_sql(sql)
    Logger.debug(f"schedule - %sql\n---\n{sql}\n---")

    spark.sql(sql)


def create_or_replace_view(name: str):
    p = PATH_VIEWS.join(f"{name}.sql")
    try:
        _create_or_replace_view(p)
    except Exception:
        Logger.warning(f"schedule - {name} not created nor replace")


def create_or_replace_views():
    for p in PATH_VIEWS.walk(file_format="sql", convert=True):
        try:
            _create_or_replace_view(p)
        except Exception:
            Logger.warning(f"schedule - {p.get_file_name()} not created nor replace")
