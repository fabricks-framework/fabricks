import hashlib
import json
import logging
from datetime import datetime
from typing import Tuple

from fabricks.utils.azure_table import AzureTable


class LogFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(fmt="%(levelname)s%(prefix)s%(message)s [%(timestamp)s]%(extra)s")

    COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[41;31m",
    }

    RESET = "\033[0m"
    BRIGHT = "\033[1m"

    PADDINGS = {
        "DEBUG": "   ",
        "INFO": "    ",
        "WARNING": " ",
        "ERROR": "   ",
        "CRITICAL": "",
    }

    def formatTime(self, record):
        ct = datetime.fromtimestamp(record.created)
        s = ct.strftime("%d/%m/%y %H:%M:%S")
        return f"{self.COLORS[logging.DEBUG]}{s}{self.RESET}"

    def format(self, record):
        levelname = record.levelname
        padding = self.PADDINGS[levelname]
        levelname = f"{self.COLORS[record.levelno]}{levelname}:{padding}{self.RESET}"

        prefix = ""
        if hasattr(record, "job"):
            prefix = f"{record.__dict__.get('job')} - "
        elif hasattr(record, "step"):
            prefix = f"{self.BRIGHT}{record.__dict__.get('step')}{self.RESET} - "

        extra = ""
        if hasattr(record, "exc_info") and record.exc_info:
            exc_info = record.__dict__.get("exc_info", None)
            extra += f" [{self.COLORS[logging.ERROR]}{exc_info[0].__name__}{self.RESET}]"

        if hasattr(record, "sql"):
            extra += f"\n---\n%sql\n{record.__dict__.get('sql')}\n---"
        if hasattr(record, "content"):
            extra += f"\n---\n{record.__dict__.get('content')}\n---"

        record.levelname = levelname
        record.prefix = prefix
        record.timestamp = self.formatTime(record)
        record.extra = extra

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
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()

    root = logging.getLogger()
    if root.hasHandlers():
        root.handlers.clear()

    logger.setLevel(level)
    logger.propagate = False

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
