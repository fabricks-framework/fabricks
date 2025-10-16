from fabricks.context import PATH_VIEWS, SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.path import Path
from fabricks.utils.sqlglot import fix as fix_sql


def create_or_replace_view_internal(path: Path):
    sql = path.get_sql()
    file_name = path.get_file_name().split(".")[0]

    try:
        sql = f"""
        create or replace view fabricks.{file_name}
        as
        {sql}
        """
        sql = fix_sql(sql)
        DEFAULT_LOGGER.debug("create or replace (custom) view", extra={"label": f"fabricks.{file_name}", "sql": sql})

        SPARK.sql(sql)

    except Exception as e:
        DEFAULT_LOGGER.exception(
            "could not create nor replace (custom) view", extra={"label": f"fabricks.{file_name}", "exc_info": e}
        )
        raise e


def create_or_replace_view(name: str):
    p = PATH_VIEWS.joinpath(f"{name}.sql")
    create_or_replace_view_internal(p)


def create_or_replace_views():
    DEFAULT_LOGGER.info("create or replace (custom) views")

    for p in PATH_VIEWS.walk(file_format="sql", convert=True):
        try:
            create_or_replace_view_internal(p)
        except Exception:
            pass
