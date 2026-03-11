from fabricks.core.job_schema import get_job_schema, print_job_schema

job_schema = get_job_schema()
bronze_job_schema = get_job_schema("bronze")
silver_job_schema = get_job_schema("silver")
gold_job_schema = get_job_schema("gold")

__all__ = [
    "get_job_schema",
    "print_job_schema",
    "job_schema",
    "bronze_job_schema",
    "silver_job_schema",
    "gold_job_schema",
]
