"""A job identified only by step/topic/item, with no resolved config --
see https://github.com/fabricks-framework/fabricks/issues/198.

Recovers storage/metadata orphaned by removing a job from the runtime
without calling drop() on it first: once gone from the runtime, get_job()
can no longer build a real Bronze/Silver/Gold job object for it (there's
no config to resolve), so its Delta table/view and schema/checkpoint
folders are stuck with no supported way to remove them. Only drop() is
exposed here, and unlike a regular job's drop() (fabricks/core/jobs/base/
generator.py's Generator.drop()), no_drop is not honored, since the
config that would have set it no longer exists.
"""

from functools import cached_property
from typing import cast

from fabricks.cdc import NoCDC
from fabricks.context import PATHS_STORAGE, SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.models.utils import get_job_id


class OrphanJob:
    def __init__(self, step: str, topic: str, item: str) -> None:
        self.step = step
        self.topic = topic
        self.item = item
        self.job_id = get_job_id(step=step, topic=topic, item=item)
        self.spark = SPARK

    def __str__(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    @cached_property
    def cdc(self) -> NoCDC:
        # NoCDC.table.drop() drops whichever the metastore actually has --
        # table or view -- so the job's original cdc type doesn't matter
        # for a drop, only its step/topic/item identity.
        return NoCDC(self.step, self.topic, self.item, spark=self.spark)

    def drop(self) -> None:
        try:
            row = self.spark.sql(
                f"""
                select
                    count(*) as count,
                    array_join(sort_array(collect_set(j.job)), ', \n') as children
                from
                    fabricks.dependencies d
                    inner join fabricks.jobs j on d.job_id = j.job_id
                where
                    parent like '{self}'
                """
            ).collect()[0]
            if cast(int, row.count) > 0:
                DEFAULT_LOGGER.warning(
                    f"{row.count} children found", extra={"label": str(self), "content": row.children}
                )

        except Exception:
            pass

        self.cdc.drop()

        storage = PATHS_STORAGE.get(self.step)
        assert storage, f"no storage configured for step {self.step}"

        for folder in ("schema", "checkpoints"):
            path = storage.joinpath(folder, self.topic, self.item)
            if path.exists():
                DEFAULT_LOGGER.info(f"delete {folder} folder", extra={"label": str(self)})
                path.rm()


def get_orphan_job(step: str, topic: str, item: str) -> OrphanJob:
    return OrphanJob(step=step, topic=topic, item=item)
