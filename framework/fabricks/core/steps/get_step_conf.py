from typing import Union, cast

from fabricks.core.jobs.base._types import Bronzes, Golds, JobConfBronze, JobConfGold, JobConfSilver, Silvers, TStep


def get_step_conf(step: Union[TStep, str]):
    if isinstance(step, str):
        step = cast(TStep, step)

    if step in Bronzes:
        extend = "bronze"
    elif step in Silvers:
        extend = "silver"
    elif step in Golds:
        extend = "gold"
    else:
        raise ValueError(f"{step} - not found")

    job_conf = {
        "bronze": JobConfBronze,
        "silver": JobConfSilver,
        "gold": JobConfGold,
    }.get(extend, None)
    assert job_conf
    return job_conf
