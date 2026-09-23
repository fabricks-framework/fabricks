"""Regenerate job-schema.json (and one per layer) from the Pydantic job
config models.

tests/spark/databricks/runtime/**/_config.*.yml files reference the
combined job-schema.json via a `# yaml-language-server: $schema=...`
comment, relative to framework/job-schema.json. The per-layer files
(bronze-job-schema.json, silver-job-schema.json, gold-job-schema.json)
narrow to that layer's own options for better editor typing -- e.g. a
gold job's yaml can point at gold-job-schema.json instead of the
combined schema, which allows options from every layer.

Usage:
    python scripts/generate_job_schema.py
"""

from pathlib import Path

from fabricks.models.job_schema import get_job_schema

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCHEMAS = {
    "job-schema.json": None,
    "bronze-job-schema.json": "bronze",
    "silver-job-schema.json": "silver",
    "gold-job-schema.json": "gold",
}


def main() -> None:
    for filename, step in _SCHEMAS.items():
        path = _REPO_ROOT / filename
        path.write_text(get_job_schema(step) + "\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
