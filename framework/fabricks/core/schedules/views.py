from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.schedules.get_schedule import get_schedule
from fabricks.core.schedules.get_schedules import get_schedules_df
from fabricks.utils.sqlglot import fix as fix_sql


def create_or_replace_view_internal(name: str, options: dict):
    step = "-- no step provided"
    tag = "-- no tag provided"
    view = "-- no view provided"

    assert isinstance(options, dict), "options must be a dict"

    if options.get("steps") is not None:
        steps = [f"'{s}'" for s in options.get("steps")]  # type: ignore
        step = f"and j.step in ({', '.join(steps)})"

    if options.get("tag") is not None:
        tag = f"""and array_contains(j.tags, '{options.get("tag")}')"""

    if options.get("view") is not None:
        view = f"""inner join fabricks.{options.get("view")} v on j.job_id = v.job_id"""

    sql = f"""
    create or replace view fabricks.{name}_schedule
    as
    select
        j.*
    from
        fabricks.jobs j
        {view}
    where
        true 
        {step}
        {tag}
        and j.type not in ('manual')
    """
    sql = fix_sql(sql)
    DEFAULT_LOGGER.debug("create or replace (schedule) view", extra={"label": f"fabricks.{name}_schedule", "sql": sql})

    SPARK.sql(sql)


def create_or_replace_view(name: str):
    sc = get_schedule(name=name)
    try:
        create_or_replace_view_internal(sc["name"], sc["options"])
    except Exception as e:
        DEFAULT_LOGGER.exception(f"could not create nor replace view {sc['name']}", exc_info=e)


def create_or_replace_views():
    DEFAULT_LOGGER.info("create or replace (schedule) views")

    df = get_schedules_df()
    for row in df.collect():
        try:
            create_or_replace_view_internal(row.name, row.options.asDict())
        except Exception as e:
            DEFAULT_LOGGER.exception(f"could not create nor replace view {row.name}", exc_info=e)
