from fabricks.context import Bronzes, Golds, Silvers
from fabricks.models import JobConfBronze, JobConfGold, JobConfSilver


def get_step_conf(step: str):
    if step in Bronzes:
        return JobConfBronze
    elif step in Silvers:
        return JobConfSilver
    elif step in Golds:
        return JobConfGold

    raise ValueError(f"{step} - not found")
