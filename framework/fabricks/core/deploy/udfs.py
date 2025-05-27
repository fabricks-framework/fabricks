from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.sqlglot import fix as fix_sql


def deploy_udfs():
    DEFAULT_LOGGER.info("🌟 (create or replace udfs)")

    create_or_replace_udf_job_id()


def create_or_replace_udf_job_id():
    sql = "create or replace function fabricks.udf_job_id(job string) returns string return md5(job)"
    sql = fix_sql(sql)

    DEFAULT_LOGGER.debug("create or replace fabricks.udf_job_id", extra={"sql": sql})
    SPARK.sql(sql)
