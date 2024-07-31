import sys

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import FABRICKS_STORAGE, PATH_LIBRARIES, PATH_REQUIREMENTS
from fabricks.context.log import Logger
from fabricks.utils.pip import pip_requirements, pip_wheel


def collect_site_packages(nofail: bool = False):
    Logger.info(f"collect libraries ({PATH_REQUIREMENTS})")

    dbfs_wheel = "dbfs:/fabricks/wheels"
    mnt_wheel = "dbfs:/mnt/fabricks/wheels"

    dbutils.fs.mkdirs(dbfs_wheel)

    try:
        w = FABRICKS_STORAGE.join("wheels")
        Logger.info(f"pip wheel ({w})")
        pip_wheel(PATH_REQUIREMENTS, w)
    except (Exception, ValueError) as e:
        if nofail:
            Logger.exception("oops (pip wheel)")
        else:
            raise e
    try:
        for f in dbutils.fs.ls(mnt_wheel):
            to = f"{dbfs_wheel}/{f.name}"
            try:
                dbutils.fs.ls(to)
            except Exception:
                Logger.info(f"uploading {f.name} ({to})")
                dbutils.fs.cp(f.path, to)
    except Exception as e:
        if nofail:
            Logger.exception("oops (uploading)")
        else:
            raise e

    try:
        p = FABRICKS_STORAGE.join("site-packages")
        Logger.info(f"pip requirements ({p})")
        pip_requirements(requirements_path=PATH_REQUIREMENTS, tgt_path=p)
    except Exception as e:
        if nofail:
            Logger.exception("oops (pip requirements)")
        else:
            raise e


def add_site_packages_to_path():
    if PATH_LIBRARIES not in sys.path:
        spark._sc._python_includes.append(PATH_LIBRARIES)  # type: ignore
        sys.path.append(PATH_LIBRARIES)
