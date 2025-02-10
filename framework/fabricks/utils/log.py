import hashlib
import json
import logging
from datetime import datetime
from typing import Tuple

from colorama import Back, Fore, Style, init

from fabricks.utils.azure_table import AzureTable

init(autoreset=True)


class LogFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: f"{Fore.CYAN}",
        logging.INFO: f"{Fore.GREEN}",
        logging.WARNING: f"{Fore.YELLOW}",
        logging.ERROR: f"{Fore.RED}",
        logging.CRITICAL: f"{Fore.RED}{Back.WHITE}",
    }

    PADDINGS = {
        "DEBUG": "   ",
        "INFO": "    ",
        "WARNING": " ",
        "ERROR": "   ",
        "CRITICAL": "",
    }

    def __init__(self):
        super().__init__(fmt="%(colored_level)s%(prefix)s%(message)s [%(colored_time)s]%(extra_info)s")

    def formatTime(self, record):
        ct = datetime.fromtimestamp(record.created)
        s = ct.strftime("%d/%m/%y %H:%M:%S")
        return f"{self.COLORS[record.levelno]}{s}{Style.RESET_ALL}"

    def format(self, record):
        padding = self.PADDINGS[record.levelname]
        level_text = f"{record.levelname}:{padding}"
        record.colored_level = f"{self.COLORS[record.levelno]}{level_text}{Style.RESET_ALL}"

        prefix = ""
        if hasattr(record, "job"):
            prefix = f"{record.job} "  # type: ignore
        elif hasattr(record, "step"):
            prefix = f"{Style.BRIGHT}{record.step}{Style.RESET_ALL} "  # type: ignore
        record.prefix = prefix

        record.colored_time = self.formatTime(record)

        extra = ""
        if hasattr(record, "exc_info") and record.exc_info:
            extra += f" !{record.exc_info[0].__name__.lower()}!"  # type: ignore

        if hasattr(record, "sql"):
            extra += f"\n---\n%sql\n{record.sql}\n---"  # type: ignore

        if hasattr(record, "content"):
            extra += f"\n---\n{record.content}\n---"  # type: ignore

        record.extra_info = extra

        return super().format(record)


class AzureTableHandler(logging.Handler):
    def __init__(self, table: AzureTable):
        super().__init__()
        self.buffer = []
        self.table = table

    def emit(self, record):
        if hasattr(record, "target"):
            target = record.__dict__.get("target")

            r = {
                "Created": str(
                    datetime.fromtimestamp(record.created).strftime("%d/%m/%y %H:%M:%S")
                ),  # timestamp not present when querying Azure Table
                "Level": record.levelname,
                "Message": record.message,
            }
            if hasattr(record, "job"):
                j = str(record.__dict__.get("job", ""))
                r["Job"] = j
                r["JobId"] = hashlib.md5(j.encode()).hexdigest()
            if hasattr(record, "table"):
                t = str(record.__dict__.get("table", ""))
                r["Job"] = t
                r["JobId"] = hashlib.md5(t.encode()).hexdigest()

            if hasattr(record, "step"):
                r["Step"] = record.__dict__.get("step", "")

            if hasattr(record, "schedule_id"):
                r["ScheduleId"] = record.__dict__.get("schedule_id", "")

            if hasattr(record, "schedule"):
                r["Schedule"] = record.__dict__.get("schedule", "")

            if hasattr(record, "notebook_id"):
                r["NotebookId"] = record.__dict__.get("notebook_id", "")

            if hasattr(record, "exc_info"):
                e = record.__dict__.get("exc_info", None)
                if e is not None:
                    d = {
                        "type": str(e[0].__name__)[:1000],
                        "message": str(e[1])[:1000],
                        "traceback": str(logging.Formatter.formatException(self, e))[:1000],  # type: ignore
                    }
                    r["Exception"] = json.dumps(d)

            if hasattr(record, "content"):
                r["Content"] = json.dumps(record.__dict__.get("content", ""))[:1000]
            if hasattr(record, "sql"):
                r["Sql"] = record.__dict__.get("sql", "")[:1000]

            r["PartitionKey"] = record.__dict__.get("partition_key", "default")
            if hasattr(record, "row_key"):
                r["RowKey"] = record.__dict__.get("row_key", "")
            else:
                r["RowKey"] = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()

            if target == "table":
                self.table.upsert(r)
            else:
                self.buffer.append(r)

        else:
            pass

    def flush(self):
        self.table.upsert(self.buffer)
        self.buffer.clear()

    def clear_buffer(self):
        self.buffer = []


def get_logger(name: str, level: int, table: AzureTable) -> Tuple[logging.Logger, AzureTableHandler]:
    # Remove any existing handlers for this logger
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Remove handlers from the root logger as well
    root = logging.getLogger()
    if root.hasHandlers():
        root.handlers.clear()

    logger.setLevel(level)
    logger.propagate = False  # Prevent propagation to avoid duplicate logs

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = LogFormatter()
    console_handler.setFormatter(console_format)

    # Azure Table handler
    azure_table_handler = AzureTableHandler(table=table)
    azure_table_handler.setLevel(level)

    logger.addHandler(console_handler)
    logger.addHandler(azure_table_handler)

    return logger, azure_table_handler
