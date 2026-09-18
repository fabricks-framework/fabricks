from fabricks.context import Bronzes, Golds, Silvers
from fabricks.models import JobConfBronze, JobConfGold, JobConfSilver


def get_step_conf(step: str) -> type[JobConfBronze] | type[JobConfSilver] | type[JobConfGold]:
    if step in Bronzes:
        return JobConfBronze
    if step in Silvers:
        return JobConfSilver
    if step in Golds:
        return JobConfGold

    raise ValueError(f"{step} - not found")
