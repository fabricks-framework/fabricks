from __future__ import annotations

import json

from databricks.sdk.runtime import dbutils

from fabricks.context import PATH_NOTEBOOKS
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.queue import DagQueue
from fabricks.core.dags.run import run


class DagReceiver:
    def receive(self, ctx: DagQueue):
        assert ctx.step is not None
        with ctx.get_azure_queue() as queue, ctx.get_azure_table() as azure_table:
            while True:
                response = queue.receive()
                if response == queue.sentinel:
                    LOGGER.info("no more job to process", extra={"label": str(ctx.step)})
                    break

                elif response:
                    j = json.loads(response)
                    j["Status"] = "starting"
                    azure_table.upsert(j)
                    LOGGER.info("start", extra=ctx.extra(j))

                    try:
                        if ctx.notebook:
                            path: str = PATH_NOTEBOOKS.joinpath("run").get_notebook_path()
                            dbutils.notebook.run(
                                path=path,  # pyright: ignore[reportCallIssue]
                                timeout_seconds=ctx.step.timeouts.job,  # pyright: ignore[reportCallIssue]
                                arguments={  # pyright: ignore[reportCallIssue]
                                    "schedule_id": ctx.schedule_id,
                                    "schedule": ctx.schedule,
                                    "step": str(ctx.step),
                                    "job_id": j.get("JobId"),
                                    "job": j.get("Job"),
                                },
                            )
                        else:
                            run(
                                step=str(ctx.step),
                                job_id=j.get("JobId"),
                                schedule_id=ctx.schedule_id,
                                schedule=ctx.schedule,
                            )

                    except Exception:
                        LOGGER.warning("fail", extra={"label": j.get("Job")})

                    finally:
                        j["Status"] = "ok"
                        azure_table.upsert(j)
                        LOGGER.info("end", extra=ctx.extra(j))
                        TABLE_LOG_HANDLER.flush()

                    dependencies = azure_table.query(
                        f"PartitionKey eq 'dependencies' and ParentId eq '{j.get('JobId')}'"
                    )
                    azure_table.delete(dependencies)
