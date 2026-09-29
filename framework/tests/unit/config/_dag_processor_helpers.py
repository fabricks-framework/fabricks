"""Shared DagProcessor.receive() test rigging: a fake Azure queue/table pair
wired onto a bare DagProcessor instance, bypassing __init__'s real Azure
client setup. Used by test_dag_receive_status.py and
test_dag_receive_skips_unchanged.py.
"""

from collections.abc import Callable
import json
from unittest.mock import MagicMock


def fake_processor(job_response: dict, query: Callable[[str], list[dict]] | list[dict]) -> tuple[MagicMock, MagicMock]:
    from fabricks.core.dags.processor import DagProcessor

    processor = DagProcessor.__new__(DagProcessor)
    processor.notebook = False
    processor.schedule_id = "sched-1"
    processor.schedule = "daily"
    processor.step = MagicMock(__str__=lambda self: "silver")

    fake_queue = MagicMock()
    fake_queue.sentinel = object()
    fake_queue.receive.side_effect = [json.dumps(job_response), fake_queue.sentinel]

    fake_table = MagicMock()
    if callable(query):
        fake_table.query.side_effect = query
    else:
        fake_table.query.return_value = query

    processor.get_azure_queue = MagicMock()
    processor.get_azure_queue.return_value.__enter__.return_value = fake_queue
    processor.get_azure_table = MagicMock()
    processor.get_azure_table.return_value.__enter__.return_value = fake_table

    return processor, fake_table
