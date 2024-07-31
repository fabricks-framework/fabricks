import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Set

import requests
from requests.auth import HTTPBasicAuth

organization = "bmeurope"
project = "BMS - Data"
feedId = "BMS"

token = os.environ["DEVOPS_TOKEN"]

known_packages = ["fabricks." + d for d in os.listdir("../fabricks")]


def download_fabricks(do_all=False):
    artifact_url = f"https://feeds.dev.azure.com/{organization}/_apis/packaging/Feeds/{feedId}/packages?api-version=7.0&packageNameQuery=fabricks&includeAllVersions=true"
    res = requests.get(artifact_url, auth=HTTPBasicAuth("a", token))
    res.raise_for_status()
    packages = res.json()["value"]
    dependency_map: Dict[str, Set[str]] = dict()
    for package in packages:
        packageId = package["id"]
        packageName = package["name"]
        print(package["name"])
        versions_relevant = package["versions"][0:5] if not do_all else package["versions"]
        for version in versions_relevant:  # only download the latest 5 versions
            packageVersionId = version["id"]
            publish_date = datetime.fromisoformat(version["publishDate"][0:10])
            if publish_date < (datetime.now() - timedelta(days=30)) and not do_all:
                continue
            versionnr = version["version"]
            vres = requests.get(
                f"https://feeds.dev.azure.com/{organization}/_apis/packaging/Feeds/{feedId}/Packages/{packageId}/versions/{packageVersionId}?includeUrls=true&api-version=7.0",
                auth=HTTPBasicAuth("a", token),
            )
            vres.raise_for_status()
            vresjs = vres.json()
            is_fabricks_pkg = (
                packageName in ["fabricks"]
                or packageName.replace("-", ".") in known_packages
                or any((d["packageName"] in known_packages for d in vresjs["dependencies"]))
            )
            if not is_fabricks_pkg:
                print(f"skipping {packageName} {versionnr} because it is not a fabricks package")
                continue
            fls = vresjs["files"]
            for file in fls:
                fileName = file["name"]
                if not fileName.endswith(".whl"):
                    continue
                target_dir = f"_out/v{versionnr}/wheels"
                fabricks_target_dir = f"_out/v{versionnr}/fabricks_wheels"
                target_filename = target_dir + "/" + fileName
                if not os.path.exists(target_filename):
                    url = f"https://pkgs.dev.azure.com/{organization}/_apis/packaging/feeds/{feedId}/pypi/packages/{packageName}/versions/{versionnr}/{fileName}/content?api-version=7.0-preview.1"
                    fres = requests.get(url, auth=HTTPBasicAuth("a", token))
                    if fres.status_code == 503:
                        print("sleeping because of request limit")
                        sleep(60)  # happens if you overshoot the request limit :)
                        fres = requests.get(url, auth=HTTPBasicAuth("a", token))

                    fres.raise_for_status()
                    sleep(0.001)
                    os.makedirs(target_dir, exist_ok=True)
                    os.makedirs(fabricks_target_dir, exist_ok=True)
                    with open(target_filename, "wb") as f:
                        f.write(fres.content)

                    shutil.copy(target_filename, fabricks_target_dir)
            deps = vres.json()["dependencies"]
            for dep in deps:
                depstr = dep["packageName"] + dep["versionRange"]
                if versionnr not in dependency_map:
                    dependency_map[versionnr] = set()
                dependency_map[versionnr].add(depstr)

    for version, dependencies in dependency_map.items():
        with open(f"_out/v{version}/requirements.txt", "w") as r:
            r.write("\n".join(dependencies))
        with open(f"_out/v{version}/requirements4downl.txt", "w") as r:
            r.write("\n".join([d for d in dependencies if not d.startswith("")]))
        subprocess.check_call(
            "pip download --prefer-binary -r requirements4downl.txt -d wheels",
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            cwd=f"_out/v{version}",
        )
        for pyv in ["3.10", "3.11", "3.12"]:
            subprocess.check_call(
                f"pip download --only-binary=:all: -r requirements4downl.txt -d wheels --python-version {pyv}",
                shell=True,
                stdout=sys.stdout,
                stderr=sys.stderr,
                cwd=f"_out/v{version}",
            )
        os.remove(f"_out/v{version}/requirements4downl.txt")
        with zipfile.ZipFile(f"_out/v{version}/fabricks_and_dependencies.wheelhouse.zip", "w") as zp:
            for root, _, files in os.walk(f"_out/v{version}/wheels"):
                for file in files:
                    zp.write(os.path.join(root, file), arcname=file)
        with zipfile.ZipFile(f"_out/v{version}/fabricks.wheelhouse.zip", "w") as zp:
            for root, _, files in os.walk(f"_out/v{version}/fabricks_wheels"):
                for file in files:
                    if file.endswith(".whl"):
                        zp.write(os.path.join(root, file), arcname=file)

    print(dependency_map)


if __name__ == "__main__":
    download_fabricks()
