"""Auto-mark everything under tests/integration as `integration`.

Keeps `pytest -m integration` (and the marker declared in pyproject.toml) honest
without a per-file `pytestmark`.
"""


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker("integration")
