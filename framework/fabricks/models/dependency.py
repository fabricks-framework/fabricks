"""Job configuration models."""

from pydantic import BaseModel, ConfigDict, model_validator
from pyspark.sql.types import StringType, StructField, StructType

from fabricks.core.jobs.get_job_id import get_dependency_id, get_job_id
from fabricks.models.common import AllowedOrigins


class JobDependency(BaseModel):
    """Job dependency tracking."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    origin: AllowedOrigins
    job_id: str
    parent: str
    parent_id: str
    dependency_id: str

    def __str__(self) -> str:
        return f"{self.job_id} -> {self.parent}"

    @model_validator(mode="after")
    def check_no_circular_dependency(self):
        if self.job_id == self.parent_id:
            raise ValueError("Circular dependency detected")
        return self

    @staticmethod
    def from_parts(job_id: str, parent: str, origin: AllowedOrigins):
        parent = parent.removesuffix("__current")
        return JobDependency(
            job_id=job_id,
            origin=origin,
            parent=parent,
            parent_id=get_job_id(job=parent),
            dependency_id=get_dependency_id(parent=parent, job_id=job_id),
        )


SchemaDependencies = StructType(
    [
        StructField("dependency_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("parent", StringType(), True),
    ]
)
