# Initialize Fabricks

This helper explains how to initialize Fabricks on a Databricks cluster or local environment by:

- Installing the Fabricks library
- Pointing Fabricks to your runtime
- Running the armageddon script to bootstrap metadata and objects

## 1) Install Fabricks

You need the Fabricks package available on your cluster or local environment.

- Databricks cluster (recommended)
  - Libraries → Install new → Python PyPI → fabricks (or install a wheel/artifact you build)
  - Alternatively, attach a workspace library artifact built from this repository

- Local development (optional)
  - pip install fabricks
  - or from source (for development): pip install -e .[dev,test]

Python >=3.9,<4 is recommended; align with your Databricks LTS runtime.

## 2) Point Fabricks to your runtime

Fabricks discovers its runtime via either environment variables or [tool.fabricks] in your pyproject.toml. The core lookup logic is implemented in fabricks/context/runtime.py.

*Option A*: Configure via pyproject.toml (preferred for repo-managed projects):
```toml
[tool.fabricks]
runtime = "path/to/your/runtime"                   # e.g., tests/integration/runtime or examples/runtime
notebooks = "fabricks/api/notebooks"               # optional: helpers shipped with Fabricks
job_config_from_yaml = true                        # optional
loglevel = "info"                                  # optional: DEBUG|INFO|WARNING|ERROR|CRITICAL
debugmode = false                                  # optional
config = "path/to/your/runtime/fabricks/conf.fabricks.yml"  # main runtime YAML
```

*Option B*: Configure via environment variables (useful on clusters):

```python
# FABRICKS_RUNTIME: path to your runtime (jobs, SQL, configs)
# FABRICKS_CONFIG: full path to your main conf.fabricks.yml (if not set, Fabricks tries to infer a conf.uc.<orgId>.yml)
# FABRICKS_NOTEBOOKS: optional helper notebook path
# FABRICKS_IS_JOB_CONFIG_FROM_YAML, FABRICKS_LOGLEVEL, FABRICKS_IS_DEBUGMODE: optional toggles
```

Example on Databricks (Cluster → Configuration → Advanced options → Environment variables):
```
FABRICKS_RUNTIME=/Workspace/Repos/your/repo/examples/runtime
FABRICKS_CONFIG=/Workspace/Repos/your/repo/examples/runtime/fabricks/conf.fabricks.yml
FABRICKS_LOGLEVEL=INFO
```

You can also set env vars in a notebook before importing Fabricks:
```python
import os
os.environ["FABRICKS_RUNTIME"] = "/Workspace/Repos/your/repo/examples/runtime"
os.environ["FABRICKS_CONFIG"] = "/Workspace/Repos/your/repo/examples/runtime/fabricks/conf.fabricks.yml"
# Optional:
# os.environ["FABRICKS_LOGLEVEL"] = "INFO"
```

## 3) Run armageddon

armageddon performs a one-shot setup aligned with your runtime configuration (e.g., preparing databases/metadata, registering views).

Import and call:
```python
# Databricks or local
from fabricks.core.scripts.armageddon import armageddon

# You may pass one or more steps (bronze, silver, gold, semantic, transf, ...)
# Examples:
armageddon(steps="gold")                      # single step
armageddon(steps=["bronze", "silver", "gold"])  # multiple steps
armageddon(steps=None)                        # default behavior, follow runtime config
```

## Example: Databricks Notebook: Initialize

Create a new notebook (Python) named initialize and include:

```python
# (Optional) set env vars if not using pyproject.toml
# import os
# os.environ["FABRICKS_RUNTIME"] = "/Workspace/Repos/your/repo/examples/runtime"
# os.environ["FABRICKS_CONFIG"] = "/Workspace/Repos/your/repo/examples/runtime/fabricks/conf.fabricks.yml"
# os.environ["FABRICKS_LOGLEVEL"] = "INFO"

from fabricks.core.scripts.armageddon import armageddon
# Run for all default steps from your runtime config:
armageddon()
```

Attach the Fabricks library to the cluster before running the notebook.

## Troubleshooting

- Missing env/config:
  - ValueError: Must have at least a pyproject.toml or set FABRICKS_RUNTIME
  - Fix by setting FABRICKS_RUNTIME or adding [tool.fabricks] to pyproject.toml

- Unity Catalog:
  - Ensure options.unity_catalog is true and options.catalog is set in conf.fabricks.yml

- Paths and storage:
  - conf.fabricks.yml must define path_options.storage and per-step runtime/storage paths; Fabricks uses these to resolve PATHS_RUNTIME and PATHS_STORAGE

- Logging:
  - Set FABRICKS_LOGLEVEL or tool.fabricks.loglevel to control verbosity

## Related topics

- Runtime configuration: ../helpers/runtime.md
- Step Helper: ./step.md
- Job Helper: ./job.md
