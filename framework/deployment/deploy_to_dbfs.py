import base64
import os

import requests

## TODO: Move out of this repo


def upload_file(headers: dict, instance_url: str, src_path: str, trg_path: str, overwrite=False):
    res = requests.post(
        f"{instance_url}/api/2.0/dbfs/create",
        json={"path": trg_path, "overwrite": overwrite},
        headers=headers,
    )
    if res.status_code >= 300 and res.json()["error_code"] == "RESOURCE_ALREADY_EXISTS":
        return False
    res.raise_for_status()
    handle = res.json()["handle"]
    with open(src_path, "rb") as f:
        dt = f.read(1000000)
        if dt:
            requests.post(
                f"{instance_url}/api/2.0/dbfs/add-block",
                json={"data": base64.b64encode(dt).decode("ascii"), "handle": handle},
                headers=headers,
            ).raise_for_status()
        while dt:
            dt = f.read(1000000)
            requests.post(
                f"{instance_url}/api/2.0/dbfs/add-block",
                json={"data": base64.b64encode(dt).decode("ascii"), "handle": handle},
                headers=headers,
            ).raise_for_status()
    requests.post(
        f"{instance_url}/api/2.0/dbfs/close",
        json={"handle": handle},
        headers=headers,
    ).raise_for_status()
    return True


def upload_directory_to_databricks(srcdir: str, trgdir: str, accesstoken: str, instance_url: str, overwrite: bool):
    headers = {"Authorization": "Bearer " + accesstoken}
    res = requests.post(f"{instance_url}/api/2.0/dbfs/mkdirs", headers=headers, json={"path": trgdir})
    res.raise_for_status()

    for root, _, files in os.listdir("_build"):
        for file in files:
            trg_path = trgdir + "/" + file
            res = upload_file(
                headers=headers,
                instance_url=instance_url,
                src_path=os.path.join(root, file),
                trg_path=trg_path,
                overwrite=overwrite,
            )
            if not res:
                print(f"Done already dbfs://{trg_path}")
            else:
                print(f"Done dbfs://{trg_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--databricks-dir", required=True)
    parser.add_argument("--instance-url", required=True)
    parser.add_argument("--access-token", required=True)
    parser.add_argument("--overwrite", action="store_true", required=False, default=False)
    args = parser.parse_args()
    upload_directory_to_databricks(
        args.src_dir,
        args.databricks_dir,
        args.access_token,
        args.instance_url,
        args.overwrite,
    )
