import base64
import io
import os
from importlib import resources

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from fabricks.context import PATH_NOTEBOOKS, IS_TESTMODE
from fabricks.context.log import DEFAULT_LOGGER


def deploy_notebook(notebook: str):
    from fabricks.api import notebooks

    DEFAULT_LOGGER.debug(f"overwrite {notebook}", extra={"label": "fabricks"})

    w = WorkspaceClient()

    target = f"{PATH_NOTEBOOKS}/{notebook}.py"
    src = resources.files(notebooks) / f"{notebook}.py"

    with io.open(src, "rb") as file:  # type: ignore
        content = file.read()
    
    if IS_TESTMODE:
        # Prepend Databricks notebook header
        header = b"# Databricks notebook source\n# MAGIC %run ./add_missing_modules\n\n"
        content = header + content

    encoded = base64.b64encode(content).decode("utf-8")

    w.workspace.import_(
        path=target,
        content=encoded,
        format=workspace.ImportFormat.AUTO,
        language=workspace.Language.PYTHON,
        overwrite=True,
    )


def deploy_notebooks(overwrite: bool = False):
    if overwrite:
        DEFAULT_LOGGER.warning("overwrite notebooks", extra={"label": "fabricks"})

        _create_dir_if_not_exists()
        _clean_dir()

        for n in [
            "cluster",
            "initialize",
            "process",
            "standalone",
            "run",
            "terminate",
        ]:
            deploy_notebook(notebook=n)
    else:
        DEFAULT_LOGGER.info("deploy notebooks skipped (overwrite=False)", extra={"label": "fabricks"})


def _create_dir_if_not_exists():
    dir = str(PATH_NOTEBOOKS)
    os.makedirs(dir, exist_ok=True)


def _clean_dir():
    dir = str(PATH_NOTEBOOKS)
    for n in [
        "cluster",
        "initialize",
        "process",
        "schedule",
        "run",
        "terminate",
    ]:
        file_path = os.path.join(dir, f"{n}.py")
        if os.path.isfile(file_path):
            os.remove(file_path)
