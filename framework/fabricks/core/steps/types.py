from dataclasses import dataclass


@dataclass
class Timeouts:
    job: int
    step: int
