from typing import Iterable

from fabricks.cdc import NoCDC
from fabricks.context import SPARK
from fabricks.context.runtime import BRONZE, GOLD, SILVER


def collect_steps():
    steps = []

    def _collect(expand: str, iterable: Iterable):
        for i in iterable:
            steps.append(
                {
                    "expand": expand,
                    "step": i.get("name"),
                    "order": i.get("options", {}).get("order", 0),
                },
            )

    _collect("bronze", BRONZE)
    _collect("silver", SILVER)
    _collect("gold", GOLD)

    df = SPARK.createDataFrame(steps)
    NoCDC("fabricks", "steps").overwrite(df)
