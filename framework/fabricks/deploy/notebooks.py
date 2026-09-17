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
    # Databricks converts a .py file with notebook-source content + AUTO
    # format into a notebook object whose actual path drops the .py suffix
    # -- checking only the .py-suffixed path (as this used to) always came
    # up empty once converted, silently turning overwrite=False's
    # "skip if exists" into "always re-import" on every call. import_()'s
    # own overwrite=True already handles replacing an existing object
    # safely, so this just needs to know whether to call it at all.
    exists = Path(f"{PATH_NOTEBOOKS}/{notebook}").exists() or Path(target).exists()

    if overwrite or not exists:
        DEFAULT_LOGGER.debug(f"deploying {notebook}.py", extra={"label": "fabricks"})

        src = resources.files(notebooks) / f"{notebook}.py"
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
