"""CDC scenario argument validation (pure; shared by the Apache harness and the plain tier)."""


def validate_scenario(seed_from: int, iters: list[int], compare_to: int | None = None) -> None:
    if not iters:
        raise ValueError("iters must not be empty")
    if iters != list(range(seed_from + 1, iters[-1] + 1)):
        raise ValueError("iters must be the contiguous range right after seed_from")
    if compare_to is not None and iters[-1] != compare_to:
        raise ValueError("compare_to must be iters' own last element")
