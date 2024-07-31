import json
import os

import requests
from dotenv import load_dotenv


def remote_pull(path: str = "/Repos/bmeurope/fabricks"):
    load_dotenv()

    workspace = os.getenv("DATABRICKS_WORKSPACE")
    assert workspace, "DATABRICKS_WORKSPACE environment variable is not set"
    token = os.getenv("DATABRICKS_TOKEN")
    assert token, "DATABRICKS_TOKEN environment variable is not set"

    uri = f"https://{workspace}.azuredatabricks.net/api/2.0/repos"

    req = requests.get(
        uri,
        headers={"Authorization": f"Bearer {token}"},
    )
    req.raise_for_status()
    jsd = req.json()
    for item in jsd["repos"]:
        if item["path"] == path:
            print(json.dumps(item, indent=4))
            jsd = requests.patch(
                f"{uri}/{item['id']}",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "branch": item["branch"],
                },
            )
            jsd.raise_for_status()
            print(json.dumps(jsd.json(), indent=4))


def cli():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="/Repos/bmeurope/fabricks")
    args = parser.parse_args()
    remote_pull(args.path)


if __name__ == "__main__":
    cli()
