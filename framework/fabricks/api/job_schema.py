from fabricks.core.job_schema import get_job_schema, print_job_schema

job_schema = get_job_schema()
job_schema_bronze = get_job_schema("bronze")
job_schema_silver = get_job_schema("silver")
job_schema_gold = get_job_schema("gold")

__all__ = [
    "get_job_schema",
    "print_job_schema",
    "job_schema",
    "job_schema_bronze",
    "job_schema_silver",
    "job_schema_gold",
]
