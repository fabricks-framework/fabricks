import hashlib
import json
import logging
import sys
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from fabricks.utils.azure_table import AzureTable


class LogFormatter(logging.Formatter):
    def __init__(self, debugmode: Optional[bool] = False):
        super().__init__(fmt="%(levelname)s%(prefix)s%(message)s [%(timestamp)s]%(extra)s")

        if debugmode is None:
            debugmode = False
        self.debugmode = debugmode

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
        levelname_formatted = f"{self.COLORS[record.levelno]}{levelname}:{padding}{self.RESET}"

        prefix = ""
        if hasattr(record, "job"):
            prefix = f"{record.__dict__.get('job')} - "
        elif hasattr(record, "step"):
            prefix = f"{self.BRIGHT}{record.__dict__.get('step')}{self.RESET} - "

        extra = ""
        if hasattr(record, "exc_info") and record.exc_info:
            exc_info = record.__dict__.get("exc_info", None)
            extra += f" [{self.COLORS[logging.ERROR]}{exc_info[0].__name__}{self.RESET}]"

        if self.debugmode:
            if hasattr(record, "sql"):
                extra += f"\n---\n%sql\n{record.__dict__.get('sql')}\n---"

            if hasattr(record, "content"):
                extra += f"\n---\n{record.__dict__.get('content')}\n---"

            if hasattr(record, "df"):
                df = record.__dict__.get("df")
                if isinstance(df, DataFrame):
                    extra += f"\n---\n%df\n{df.toPandas().to_string(index=True)}\n---"

        record.levelname = levelname_formatted
        record.prefix = prefix
        record.timestamp = self.formatTime(record)
        record.extra = extra

        return super().format(record)


class AzureTableLogHandler(logging.Handler):
    def __init__(self, table: AzureTable, debugmode: Optional[bool] = False):
        super().__init__()

        self.buffer = []
        self.table = table

        if debugmode is None:
            debugmode = False
        self.debugmode = debugmode

    def emit(self, record):
        if hasattr(record, "target"):
            target = record.__dict__.get("target")

            level = record.levelname
            if "debug" in level.lower():
                level = "DEBUG"
            elif "info" in level.lower():
                level = "INFO"
            elif "warning" in level.lower():
                level = "WARNING"
            elif "error" in level.lower():
                level = "ERROR"
            elif "critical" in level.lower():
                level = "CRITICAL"
            else:
                level = "INFO"

            r = {
                "Created": str(
                    datetime.fromtimestamp(record.created).strftime("%d/%m/%y %H:%M:%S")
                ),  # timestamp not present when querying Azure Table
                "Level": level,
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

            if self.debugmode:
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


class CustomConsoleHandler(logging.StreamHandler):
    def __init__(self, stream=None, debugmode: Optional[bool] = False):
        super().__init__(stream or sys.stderr)
        self.debugmode = debugmode if debugmode is not None else False

    def emit(self, record):
        if hasattr(record, "sql"):
            if self.debugmode:
                super().emit(record)
        else:
            super().emit(record)


def get_logger(
    name: str,
    level: int,
    table: Optional[AzureTable] = None,
    debugmode: Optional[bool] = False,
) -> Tuple[logging.Logger, Optional[AzureTableLogHandler]]:
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()

    root = logging.getLogger()
    if root.hasHandlers():
        root.handlers.clear()

    logger.setLevel(level)
    logger.propagate = False

    # Console handler
    console_handler = CustomConsoleHandler(debugmode=debugmode)
    console_handler.setLevel(level)
    console_format = LogFormatter(debugmode=debugmode)
    console_handler.setFormatter(console_format)

    if table is not None:
        # Azure Table handler
        azure_table_handler = AzureTableLogHandler(table=table, debugmode=debugmode)
        azure_table_handler.setLevel(level)
    else:
        azure_table_handler = None

    logger.addHandler(console_handler)
    if azure_table_handler is not None:
        logger.addHandler(azure_table_handler)

    return logger, azure_table_handler
