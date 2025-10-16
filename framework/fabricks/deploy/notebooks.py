import base64
import io
from importlib import resources

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from fabricks.api import notebooks
from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER


def deploy_notebook(notebook: str):
    DEFAULT_LOGGER.debug(f"overwrite {notebook}")

    w = WorkspaceClient()

    target = f"{PATH_NOTEBOOKS}/{notebook}.py"
    src = resources.files(notebooks) / f"{notebook}.py"

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


def deploy_notebooks():
    DEFAULT_LOGGER.info("overwrite notebooks")

    for n in [
        "cluster",
        "initialize",
        "process",
        "schedule",
        "run",
        "terminate",
    ]:
        deploy_notebook(notebook=n)
