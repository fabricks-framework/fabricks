from typing import Optional


class formatter:
    END = "\33[0m"
    BOLD = "\33[1m"
    ITALIC = "\33[3m"
    URL = "\33[4m"
    BLINK = "\33[5m"
    SELECTED = "\33[7m"

    BLINK2 = "\33[6m"


class colors:
    BLACK = "\33[30m"
    RED = "\33[31m"
    GREEN = "\33[32m"
    YELLOW = "\33[33m"
    BLUE = "\33[34m"
    VIOLET = "\33[35m"
    BEIGE = "\33[36m"
    WHITE = "\33[37m"
    GREY = "\33[90m"
    ORANGE = "\33[33m"

    RED2 = "\33[91m"
    GREEN2 = "\33[92m"
    YELLOW2 = "\33[93m"
    BLUE2 = "\33[94m"
    VIOLET2 = "\33[95m"
    BEIGE2 = "\33[96m"
    WHITE2 = "\33[97m"

    RED3 = "\33[1;31m"


def progress_bar(progress: int = 0, width: int = 40, msg: Optional[str] = None):
    if not isinstance(progress, int):
        progress = int(progress)

    left = width * progress // 100
    right = width - left

    tags = "#" * left
    spaces = " " * right
    pct = f" {progress}%"
    if msg:
        pct = f"{pct} ({msg})"

    print("\r[", tags, spaces, "]", pct, sep="", end="", flush=True)
