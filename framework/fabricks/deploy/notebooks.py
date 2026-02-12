import base64
import io
import os
from importlib import resources

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER

def deploy_notebook(notebook: str, overwrite: bool = True):
    from fabricks.api import notebooks

    w = WorkspaceClient()

    target = f"{PATH_NOTEBOOKS}/{notebook}.py"
    src = resources.files(notebooks) / f"{notebook}.py"

    if overwrite:
        if os.path.isfile(target):
            DEFAULT_LOGGER.debug(f"removing {notebook}.py", extra={"label": "fabricks"})
            os.remove(target)

    if not os.path.exists(target):
        DEFAULT_LOGGER.debug(f"deploying {notebook}.py", extra={"label": "fabricks"})

        with io.open(src, "rb") as file:  # type: ignore
            content = file.read()

        encoded = base64.b64encode(content).decode("utf-8")

        w.workspace.import_(
            path=target,
            content=encoded,
            format=workspace.ImportFormat.AUTO,
            language=workspace.Language.PYTHON,
            overwrite=True,
        )


def deploy_notebooks(overwrite: bool = False):
    d = str(PATH_NOTEBOOKS)
    os.makedirs(d, exist_ok=True)

    DEFAULT_LOGGER.info(f"deploying notebooks {'(overwrite)' if overwrite else ''}", extra={"label": "fabricks"})

    for n in [
        "cluster",
        "initialize",
        "process",
        "standalone",
        "run",
        "terminate",
    ]:
        deploy_notebook(notebook=n, overwrite=overwrite)
