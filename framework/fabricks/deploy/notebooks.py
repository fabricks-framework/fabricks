import base64
from importlib import resources
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER


def deploy_notebook(notebook: str, overwrite: bool = True) -> None:
    from fabricks.api import notebooks

    w = WorkspaceClient()

    target = f"{PATH_NOTEBOOKS}/{notebook}.py"
    target_path = Path(target)
    src = resources.files(notebooks) / f"{notebook}.py"

    if overwrite and target_path.is_file():
        DEFAULT_LOGGER.debug(f"removing {notebook}.py", extra={"label": "fabricks"})
        target_path.unlink()

    if not target_path.exists():
        DEFAULT_LOGGER.debug(f"deploying {notebook}.py", extra={"label": "fabricks"})

        with src.open("rb") as file:
            content = file.read()

        encoded = base64.b64encode(content).decode("utf-8")

        w.workspace.import_(
            path=target,
            content=encoded,
            format=workspace.ImportFormat.AUTO,
            language=workspace.Language.PYTHON,
            overwrite=True,
        )


def deploy_notebooks(overwrite: bool = False) -> None:
    d = Path(str(PATH_NOTEBOOKS))
    d.mkdir(parents=True, exist_ok=True)

    DEFAULT_LOGGER.info(f"deploying notebooks {'(overwrite)' if overwrite else ''}", extra={"label": "fabricks"})

    for n in ["cluster", "initialize", "process", "standalone", "run", "terminate"]:
        deploy_notebook(notebook=n, overwrite=overwrite)
