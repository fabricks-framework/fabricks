from __future__ import annotations

import json

from databricks.sdk.runtime import dbutils

from fabricks.context import PATH_NOTEBOOKS
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.protocols import DagsProtocol
from fabricks.core.dags.run import run


class DagReceiver:
    def __init__(self, dags: DagsProtocol):
        self._dags = dags

    def receive(self):
        dags = self._dags
        assert dags.step is not None
        with dags.get_azure_queue() as queue, dags.get_azure_table() as azure_table:
            while True:
                response = queue.receive()
                if response == queue.sentinel:
                    LOGGER.info("no more job to process", extra={"label": str(dags.step)})
                    break

                elif response:
                    j = json.loads(response)
                    j["Status"] = "starting"
                    azure_table.upsert(j)
                    LOGGER.info("start", extra=dags.extra(j))

                    try:
                        if dags.notebook:
                            path: str = PATH_NOTEBOOKS.joinpath("run").get_notebook_path()
                            dbutils.notebook.run(
                                path=path,  # ty:ignore[unknown-argument]
                                timeout_seconds=dags.step.timeouts.job,  # ty:ignore[unknown-argument]
                                arguments={  # ty:ignore[unknown-argument]
                                    "schedule_id": dags.schedule_id,
                                    "schedule": dags.schedule,
                                    "step": str(dags.step),
                                    "job_id": j.get("JobId"),
                                    "job": j.get("Job"),
                                },
                            )
                        else:
                            run(
                                step=str(dags.step),
                                job_id=j.get("JobId"),
                                schedule_id=dags.schedule_id,
                                schedule=dags.schedule,
                            )

                    except Exception:
                        LOGGER.warning("fail", extra={"label": j.get("Job")})

                    finally:
                        j["Status"] = "ok"
                        azure_table.upsert(j)
                        LOGGER.info("end", extra=dags.extra(j))
                        TABLE_LOG_HANDLER.flush()

                    dependencies = azure_table.query(
                        f"PartitionKey eq 'dependencies' and ParentId eq '{j.get('JobId')}'"
                    )
                    azure_table.delete(dependencies)
