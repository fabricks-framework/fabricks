from fabricks.context import Bronzes, Golds, Silvers
from fabricks.models import JobConfBronze, JobConfGold, JobConfSilver


def get_step_conf(step: str):
    if step in Bronzes:
        expand = "bronze"
    elif step in Silvers:
        expand = "silver"
    elif step in Golds:
        expand = "gold"
    else:
        raise ValueError(f"{step} - not found")

    conf = {
        "bronze": JobConfBronze,
        "silver": JobConfSilver,
        "gold": JobConfGold,
    }.get(expand, None)

    assert conf
    return conf
