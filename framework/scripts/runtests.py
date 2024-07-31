import os

import requests
from dotenv import load_dotenv


def runtests(
    armageddon: bool = True,
    initialize: bool = True,
    reset: bool = False,
    last_test: int = 7,
):
    load_dotenv()

    workspace = os.getenv("DATABRICKS_WORKSPACE")
    assert workspace
    token = os.getenv("DATABRICKS_TOKEN")
    assert token
    job_id = os.getenv("DATABRICKS_RUNTESTS_JOB_ID")
    assert job_id

    tests = range(1, last_test + 1) if last_test > 1 else [1]
    uri = f"https://{workspace}.azuredatabricks.net/api/2.1/jobs"

    req = requests.post(
        f"{uri}/run-now",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "job_id": job_id,
            "job_parameters": {
                "armageddon": armageddon,
                "initialize": initialize,
                "reset": reset,
                "tests": ",".join([f"job{t}" for t in tests]),
            },
        },
    )
    req.raise_for_status()
    jsd = req.json()
    run_id = jsd["run_id"]
    status = "PENDING"

    while status not in ["TERMINATED", "INTERNAL_ERROR"]:
        res = requests.get(
            f"{uri}/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        res.raise_for_status()
        jsd = res.json()
        status = jsd.get("state").get("life_cycle_state")
        print(
            "\r",
            "status: ",
            status,
            " (",
            jsd.get("run_page_url"),
            ")",
            sep="",
            end="",
            flush=True,
        )

    if status == "INTERNAL_ERROR":
        print("\n")
        print(jsd.get("state").get("state_message"), sep="")


def cli():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--armageddon", type=bool, default=True)
    parser.add_argument("--initialize", type=bool, default=True)
    parser.add_argument("--reset", type=bool, default=False)
    parser.add_argument("--tests", type=int, default=7)

    args = parser.parse_args()
    runtests(args.armageddon, args.initialize, args.reset, args.tests)


if __name__ == "__main__":
    cli()
