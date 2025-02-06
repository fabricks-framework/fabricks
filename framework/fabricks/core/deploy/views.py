from fabricks.context import SPARK
from fabricks.context.log import Logger
from fabricks.core.jobs.base._types import Steps
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_views():
    Logger.info("🌟 (create or replace views)")

    create_or_replace_jobs_view()

    create_or_replace_logs_pivot_view()
    create_or_replace_last_schedule_view()
    create_or_replace_last_status_view()
    create_or_replace_previous_schedule_view()

    create_or_replace_schedules_view()

    create_or_replace_dependencies_view()
    create_or_replace_dependencies_flat_view()
    create_or_replace_dependencies_unpivot_view()
    create_or_replace_dependencies_circular_view()

    create_or_replace_tables_view()
    create_or_replace_views_view()


def create_or_replace_jobs_view():
    dmls = []

    for step in Steps:
        table = f"{step}_jobs"

        df = SPARK.sql("show tables in fabricks").where(f"tableName like '{table}'")
        if not df.isEmpty():
            try:
                SPARK.sql(f"select options.change_data_capture from fabricks.{table}")
                change_data_capture = "coalesce(options.change_data_capture, 'nocdc') as change_data_capture"
            except Exception:
                change_data_capture = "'nocdc' as change_data_capture"

            dmls.append(
                f"""
                select 
                  step, 
                  job_id, 
                  topic, 
                  item, 
                  concat(step, '.', topic, '_', item) as job,
                  options.mode,
                  {change_data_capture},
                  coalesce(options.type, 'default') as type,
                  tags
                from
                  fabricks.{table}
            """
            )

    sql = f"""create or replace view fabricks.jobs as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.jobs", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_tables_view():
    dmls = []

    for step in Steps:
        table = f"{step}_tables"

        df = SPARK.sql("show tables in fabricks").where(f"tableName like '{table}'")
        if not df.isEmpty():
            dmls.append(
                f"""
                select 
                  '{step}' as step, 
                  job_id, 
                  table 
                from
                  fabricks.{table}
                """
            )

    sql = f"""create or replace view fabricks.tables as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.tables", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_views_view():
    dmls = []

    for step in Steps:
        table = f"{step}_views"

        df = SPARK.sql("show tables in fabricks").where(f"tableName like '{table}'")
        if not df.isEmpty():
            dmls.append(
                f"""
                select 
                  '{step}' as step, 
                  job_id, 
                  view
                from 
                  fabricks.{table}
                """
            )

    sql = f"""create or replace view fabricks.views as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.views", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_view():
    dmls = []

    for step in Steps:
        table = f"{step}_dependencies"

        df = SPARK.sql("show tables in fabricks").where(f"tableName like '{table}'")
        if not df.isEmpty():
            dmls.append(
                f"""
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
            )

    sql = f"""create or replace view fabricks.dependencies as {" union all ".join(dmls)}"""
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.dependencies", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_flat_view():
    parent = ",\n  ".join([f"d{i + 1}.parent_id as parent_{i + 1}" for i in range(10)])
    join = "\n  ".join(
        [f"left join fabricks.dependencies d{i + 1} on d{i}.parent_id = d{i + 1}.job_id" for i in range(10)]
    )

    sql = f"""
    create or replace view fabricks.dependencies_flat as
    select
      d0.job_id,
      d0.parent_id as parent_0,
      {parent}
    from 
      fabricks.dependencies d0 
      {join}
    """
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.dependencies_flat", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_unpivot_view():
    sql = """
    create or replace view fabricks.dependencies_unpivot as
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
    Logger.debug("create or replace fabricks.dependencies_unpivot", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_dependencies_circular_view():
    sql = """
    create or replace view fabricks.dependencies_circular as
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
    Logger.debug("create or replace fabricks.dependencies_circular", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_logs_pivot_view():
    sql = """
    create or replace view fabricks.logs_pivot as
    with groupby as (
      select
        l.schedule,
        l.schedule_id,
        l.step,
        l.job,
        l.job_id,
        collect_set(l.status) as statuses,
        array_contains(statuses, 'done') as done,
        array_contains(statuses, 'failed') or not done as failed,
        not array_contains(statuses, 'failed') and not array_contains(statuses, 'done') and array_contains(statuses, 'running') as timed_out,
        not array_contains(statuses, 'running') as cancelled,
        max(l.notebook_id) as notebook_id,
        max(l.timestamp) filter(where l.status = 'scheduled') as scheduled_time,
        max(l.timestamp) filter(where l.status = 'waiting') as waiting_time,
        max(l.timestamp) filter(where l.status = 'running') as running_time,
        max(l.timestamp) filter(where l.status = 'done') as done_time,
        max(l.timestamp) filter(where l.status = 'failed') as failed_time,
        max(l.timestamp) filter(where l.status = 'ok') as ok_time,
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
      g.notebook_id,
      g.running_time as start_time,
      g.ok_time as end_time,
      g.scheduled_time,
      g.waiting_time,
      g.running_time,
      g.done_time,
      g.failed_time,
      g.ok_time,
      if(g.timed_out, null, date_diff(SECOND, start_time, end_time)) as duration,
      g.exception
    from
      groupby g
      left join fabricks.jobs j on g.job_id = j.job_id
    """
    sql = fix_sql(sql)
    Logger.debug("create or replace fabricks.logs_pivot", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_last_schedule_view():
    sql = """
    create or replace view fabricks.last_schedule as
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
    Logger.debug("create or replace fabricks.last_schedule", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_last_status_view():
    sql = """
    create or replace view fabricks.last_status as
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
    Logger.debug("create or replace fabricks.last_status", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_previous_schedule_view():
    sql = """
    create or replace view fabricks.previous_schedule as
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
    Logger.debug("create or replace fabricks.previous_schedule", extra={"sql": sql})
    SPARK.sql(sql)


def create_or_replace_schedules_view():
    sql = """
    create or replace view fabricks.schedules as
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
    Logger.debug("create or replace fabricks.schedules", extra={"sql": sql})
    SPARK.sql(sql)
