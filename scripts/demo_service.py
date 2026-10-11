"""Submit, replay and inspect a Run through the API; default demonstration is read-only."""

import argparse
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


def request(method, path, payload=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    body = json.dumps(payload).encode() if payload is not None else None
    try:
        with urlopen(Request(BASE + path, data=body, headers=headers, method=method), timeout=10) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        raise RuntimeError(f"API returned HTTP {error.code}: {error.read().decode()}") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=["check_inventory", "reserve_inventory"], default="check_inventory")
    parser.add_argument("--sku", default="BOOK-003")
    parser.add_argument("--quantity", type=int, default=2)
    args = parser.parse_args()
    if args.quantity < 1:
        parser.error("quantity must be positive")
    payload = {"task": args.task, "input": {"sku": args.sku}}
    if args.task == "reserve_inventory":
        payload["input"]["quantity"] = args.quantity
    key = "demo-" + uuid4().hex
    _, created = request("POST", "/runs", payload, key)
    _, replay = request("POST", "/runs", payload, key)
    if replay["id"] != created["id"]:
        raise RuntimeError("Idempotent replay returned a different Run")
    print("Run:", created["id"])
    print("Same-key replay reused the Run")
    path = "/runs/" + created["id"]
    for _ in range(120):
        _, run = request("GET", path)
        if run["status"] in {"COMPLETED", "FAILED", "CANCELLED"}:
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("Run did not finish in 30s; check Worker and dispatch_status")
    _, trace = request("GET", path + "/steps")
    print("Status:", run["status"], "Rounds:", run["iteration_count"])
    print("Result:", json.dumps(run["result_payload"], ensure_ascii=False))
    if run["error_payload"]:
        print("Error:", json.dumps(run["error_payload"], ensure_ascii=False))
    for step in trace["items"]:
        print(f"  Round {step['iteration_index']}: {step['step_type']}")
    if run["status"] != "COMPLETED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
