from typing import Iterable

from databricks.sdk.runtime import spark

from fabricks.cdc import NoCDC
from fabricks.context.runtime import BRONZE, GOLD, SILVER


def collect_steps():
    steps = []

    def _collect(extend: str, iterable: Iterable):
        for i in iterable:
            steps.append(
                {
                    "extend": extend,
                    "step": i.get("name"),
                    "order": i.get("options", {}).get("order", 0),
                },
            )

    _collect("bronze", BRONZE)
    _collect("silver", SILVER)
    _collect("gold", GOLD)

    df = spark.createDataFrame(steps)
    NoCDC("fabricks", "steps").overwrite(df)
