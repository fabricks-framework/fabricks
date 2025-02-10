import hashlib
import json
import logging
from datetime import datetime
from typing import Tuple

from colorama import Back, Fore, Style, init

from fabricks.utils.azure_table import AzureTable

init()


class LogFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Back.WHITE,
    }

    PADDINGS = {
        "DEBUG": "   ",
        "INFO": "    ",
        "WARNING": " ",
        "ERROR": "   ",
        "CRITICAL": "",
    }

    def format(self, record):
        message = super().format(record)  # noqa: F841

        levelname = record.levelname
        padding = self.PADDINGS.get(levelname, "")
        record.levelname = f"{self.COLORS.get(record.levelno)}{levelname}:{padding}{Style.RESET_ALL}"

        out = f"{record.levelname}"

        if hasattr(record, "job"):
            j = f" - {record.__dict__.get('job')}"
            out += str(j)

        elif hasattr(record, "step"):
            s = f" - {record.__dict__.get('step')}"
            out += str(s)

        if hasattr(record, "message"):
            m = record.__dict__.get("message", "")
            if hasattr(record, "job"):
                m = f" => {m}"
            elif hasattr(record, "step"):
                m = f" => {m}"
            else:
                m = " " + m
            out += m

        if hasattr(record, "created"):
            t = datetime.fromtimestamp(record.created).strftime("%d/%m/%y %H:%M:%S")
            t = f"{Fore.CYAN}{t}{Style.RESET_ALL}"
            out += f"[{t}]"

        if hasattr(record, "exc_info"):
            exc_info = record.__dict__.get("exc_info", None)
            if exc_info is not None:
                e = f" !{exc_info[0].__name__.lower()}!"
                out += e

        if hasattr(record, "sql"):
            s = f"\n---\n%sql\n{record.__dict__.get('sql')}\n---"
            out += s
        if hasattr(record, "content"):
            s = f"\n---\n{record.__dict__.get('content')}\n---"
            out += s

        return out


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
    logger.setLevel(level)

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
