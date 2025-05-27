from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import Steps
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_views():
    DEFAULT_LOGGER.info("🌟 (create or replace views)")

    create_or_replace_jobs_view()
    create_or_replace_tables_view()
    create_or_replace_views_view()

    create_or_replace_logs_pivot_view()
    create_or_replace_last_schedule_view()
    create_or_replace_last_status_view()
    create_or_replace_previous_schedule_view()

    create_or_replace_schedules_view()

    create_or_replace_dependencies_view()
    create_or_replace_dependencies_flat_view()
    create_or_replace_dependencies_unpivot_view()
    create_or_replace_dependencies_circular_view()

    create_or_replace_jobs_to_be_updated_view()


def create_or_replace_jobs_view():
    dmls = []

    for step in Steps:
        table = f"{step}_jobs"
        try:
            try:
                SPARK.sql(f"select options.change_data_capture from fabricks.{table}")
                change_data_capture = "coalesce(options.change_data_capture, 'nocdc') as change_data_capture"
            except Exception:
                change_data_capture = "'nocdc' as change_data_capture"

            dml = f"""
                select 
                  j.step, 
                  s.expand,
                  j.job_id, 
                  j.topic, 
                  j.item, 
                  concat(j.step, '.', j.topic, '_', j.item) as job,
                  j.options.mode,
                  {change_data_capture},
                  coalesce(j.options.type, 'default') as type,
                  tags,
                  case
                    when s.expand == "bronze" then if(j.options.mode in ("append", "register"), "table", null)
                    when
                      s.expand == "silver"
                    then
                      if(
                        j.options.mode in ("update", "append", "latest"),
                        "table",
                        if(j.options.mode in ("combine", "memory"), "view", null)
                      )
                    when
                      s.expand == "gold"
                    then
                      if(j.options.mode in ("update", "append", "complete"), "table", if(j.options.mode in ("memory"), "view", null))
                  end as object_type
                from
                  fabricks.{table} j
                  left join fabricks.steps s on s.step = j.step
            """
            SPARK.sql(dml)  # Check if the table exists
            dmls.append(dml)

        except Exception:
            DEFAULT_LOGGER.warning(f"fabricks.{table} not found")

    sql = f"""create or replace view fabricks.jobs with schema evolution as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.jobs", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_tables_view():
    dmls = []

    for step in Steps:
        table = f"{step}_tables"
        try:
            dml = f"""
                select 
                  '{step}' as step, 
                  job_id, 
                  table 
                from
                  fabricks.{table}
                """
            SPARK.sql(dml)  # Check if the table exists
            dmls.append(dml)

        except Exception:
            DEFAULT_LOGGER.warning(f"fabricks.{step}_tables not found")

    sql = f"""create or replace view fabricks.tables with schema evolution as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.tables", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_views_view():
    dmls = []

    for step in Steps:
        table = f"{step}_views"
        try:
            dml = f"""
                select 
                  '{step}' as step, 
                  job_id, 
                  view
                from 
                  fabricks.{table}
                """
            SPARK.sql(dml)  # Check if the table exists
            dmls.append(dml)

        except Exception:
            DEFAULT_LOGGER.warning(f"fabricks.{step}_views not found")

    sql = f"""create or replace view fabricks.views with schema evolution as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.views", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_view():
    dmls = []

    for step in Steps:
        f"{step}_dependencies"
        try:
            dml = f"""
              select
                '{step}' as step,
                dependency_id,
                job_id,
                parent_id,
                parent,
                origin
              from
                fabricks.{step}_dependencies d
              """
            SPARK.sql(dml)  # Check if the table exists
            dmls.append(dml)

        except Exception:
            DEFAULT_LOGGER.warning(f"fabricks.{step}_dependencies not found")

    sql = f"""create or replace view fabricks.dependencies with schema evolution  as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.dependencies", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_flat_view():
    parent = ",\n  ".join([f"d{i + 1}.parent_id as parent_{i + 1}" for i in range(10)])
    join = "\n  ".join(
        [f"left join fabricks.dependencies d{i + 1} on d{i}.parent_id = d{i + 1}.job_id" for i in range(10)]
    )

    sql = f"""
    create or replace view fabricks.dependencies_flat with schema evolution as
    select
      d0.job_id,
      d0.parent_id as parent_0,
      {parent}
    from 
      fabricks.dependencies d0 
      {join}
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.dependencies_flat", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_unpivot_view():
    sql = """
    create or replace view fabricks.dependencies_unpivot with schema evolution as
    with unpvt as (
    select
      *
    from
      fabricks.dependencies_flat unpivot (
      (parent_id) for depth in (
        (parent_0) as depth_00,
        (parent_1) as depth_01,
        (parent_2) as depth_02,
        (parent_3) as depth_03,
        (parent_4) as depth_04,
        (parent_5) as depth_05,
        (parent_6) as depth_06,
        (parent_7) as depth_07,
        (parent_8) as depth_08,
        (parent_9) as depth_09,
        (parent_10) as depth_10
        )
      ) p
    )
    select
      job_id,
      cast(replace(depth, 'depth_', '') as int) as depth,
      parent_id
    from
    unpvt qualify row_number() over (
        partition by job_id,
        parent_id
        order by
        depth asc
    ) = 1
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.dependencies_unpivot", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_circular_view():
    sql = """
    create or replace view fabricks.dependencies_circular with schema evolution as
    with d as (
      select
        d1.job_id,
        j1.job,
        p.job_id as parent_id,
        p.job as parent
      from
        fabricks.dependencies d1
        left join fabricks.dependencies_unpivot d2 on d2.parent_id = d1.job_id
        left join fabricks.jobs j1 on d1.job_id = j1.job_id
        left join fabricks.jobs p on d1.parent_id = p.job_id
      where
        true
        and d1.job_id = d2.job_id
      group by
        all
    )
    select
      *
    from
      d
    where
      true
      and exists (
        select
          1
        from
          d d1
        where
          d1.job_id = d.parent_id
      )
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.dependencies_circular", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_logs_pivot_view():
    sql = """
    create or replace view fabricks.logs_pivot with schema evolution as
    with groupby as (
      select
        l.schedule,
        l.schedule_id,
        l.step,
        l.job,
        l.job_id,
        collect_set(l.status) as statuses,
        array_contains(statuses, 'skipped') as skipped,
        array_contains(statuses, 'warned') as warned,
        array_contains(statuses, 'done') or warned as done,
        array_contains(statuses, 'failed') or (not done and not skipped) as failed,
        not done and not failed and not skipped and array_contains(statuses, 'running') as timed_out,
        not array_contains(statuses, 'running') as cancelled,
        max(l.notebook_id) as notebook_id,
        max(l.timestamp) filter(where l.status = 'running') as start_time,
        max(l.timestamp) filter(where l.status = 'ok') as end_time,
        max(l.exception) as exception
      from
        fabricks.logs l
      group by 
        l.schedule, l.schedule_id, l.step, l.job, l.job_id
    )
    select 
      g.schedule,
      g.schedule_id,
      g.job,
      g.step,
      j.topic,
      j.item,
      g.job_id,
      g.done,
      g.failed,
      g.timed_out,
      g.cancelled,
      g.skipped,
      g.warned,
      g.notebook_id,
      g.start_time,
      g.end_time,
      if(g.timed_out, null, date_diff(SECOND, start_time, end_time)) as duration,
      g.exception
    from
      groupby g
      left join fabricks.jobs j on g.job_id = j.job_id
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.logs_pivot", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_last_schedule_view():
    sql = """
    create or replace view fabricks.last_schedule with schema evolution as
    with lst as (
      select
        schedule_id as last_schedule_id
      from
        fabricks.logs_pivot
      where
        schedule_id is not null
      order by
        start_time desc
      limit
        1
    )
    select
      l.*
    from
      fabricks.logs_pivot l
      inner join lst on schedule_id = last_schedule_id
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.last_schedule", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_last_status_view():
    sql = """
    create or replace view fabricks.last_status with schema evolution as
    select
      job_id,
      job,
      step,
      start_time as time,
      done,
      failed,
      cancelled,
      timed_out,
      exception
    from
      fabricks.logs_pivot
    qualify row_number() over (
        partition by job_id
        order by
          start_time desc
      ) = 1
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.last_status", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_previous_schedule_view():
    sql = """
    create or replace view fabricks.previous_schedule with schema evolution as
    with lst_2 as (
      select
        schedule_id as last_schedule_id,
        max(start_time) as start_time
      from
        fabricks.logs_pivot
      where
        schedule_id is not null
      group by
        all
      order by
        start_time desc
      limit
        2
    ), lst as (
      select
        last_schedule_id
      from
        lst_2
      order by
        start_time asc
      limit
        1
    )
    select
      l.*
    from
      fabricks.logs_pivot l
      inner join lst on schedule_id = last_schedule_id
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.previous_schedule", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_schedules_view():
    sql = """
    create or replace view fabricks.schedules with schema evolution as
    select
      schedule,
      schedule_id,
      min(start_time) as start_time,
      max(end_time) as end_time,
      max(start_time) :: date as date,
      sum(duration) as duration,
      count(*) as logs,
      count_if(failed) as failed,
      count_if(done) as done,
      count_if(timed_out) as timed_out
    from
      fabricks.logs_pivot
    group by
      all
    order by date desc, start_time desc
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.schedules", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_jobs_to_be_updated_view():
    sql = """
    create or replace view fabricks.jobs_to_be_updated with schema evolution as
    with base as (
      select
        j.job,
        j.job_id,
        j.step,
        j.topic,
        j.item,
        s.expand,
        j.mode as mode,
        j.object_type as object_type
      from
        fabricks.jobs j
          inner join fabricks.steps s
            on j.step = s.step
    ),
    objects as (
      select
        `table` as job,
        job_id,
        'table' as object_type
      from
        fabricks.tables
      union
      select
        `view` as job,
        job_id,
        'view' as object_type
      from
        fabricks.views
    )
    select
      b.job,
      b.job_id,
      b.step,
      b.topic,
      b.item,
      b.expand,
      b.mode,
      o.object_type as old_object_type,
      b.object_type as new_object_type,
      array(old_object_type, new_object_type) as object_types,
      (old_object_type is not null and new_object_type is null) or (not old_object_type <=> new_object_type and old_object_type is not null ) as is_to_drop,
      (is_to_drop and new_object_type is not null) or (old_object_type is null and new_object_type is not null) as is_to_register
    from
      base b
        left join objects o
          on b.job_id = o.job_id    
    """
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.jobs_to_be_updated", extra={"sql": sql})
    SPARK.sql(sql)
