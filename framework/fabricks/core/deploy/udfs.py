from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import Steps
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_udfs():
    DEFAULT_LOGGER.info("🌟 (create or replace tables)")

    create_udf_get_job_id(drop)


def create_udf_get_job_id(drop):
    sql = ""create or replace function fabricks.udf_job_id(job string) returns string return md5(job)"
    sql = fix_sql(sql)
    
    DEFAULT_LOGGER.debug("create or replace fabricks.udf_job_id", extra={"sql": sql})
    SPARK.sql(sql)