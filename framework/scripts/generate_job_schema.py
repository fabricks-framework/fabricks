"""Regenerate job-schema.json from the Pydantic job config models.

tests/spark/databricks/runtime/**/_config.*.yml files reference this file
via a `# yaml-language-server: $schema=...` comment, relative to
framework/job-schema.json.

Usage:
    python scripts/generate_job_schema.py
"""

from pathlib import Path

from fabricks.models.job_schema import get_job_schema

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCHEMA_FILE = _REPO_ROOT / "job-schema.json"


def main() -> None:
    _SCHEMA_FILE.write_text(get_job_schema() + "\n")
    print(f"wrote {_SCHEMA_FILE}")


if __name__ == "__main__":
    main()
