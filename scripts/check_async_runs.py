"""Run prepare with Worker stopped, then start Worker and run verify."""

import asyncio
import base64
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

import aio_pika
from sqlalchemy import func, select

from app.broker import Broker
from app.config import get_settings
from app.db.models import Step
from app.db.session import close_db, session_scope


BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
FIXTURE = Path(__file__).resolve().parents[1] / ".venv" / "async-run-check.json"
PAYLOAD = {"task": "check_inventory", "input": {"sku": "BOOK-001"}}


def request(method, path, payload=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    body = None if payload is None else json.dumps(payload).encode()
    req = Request(BASE + path, data=body, method=method, headers=headers)
    try:
        with urlopen(req, timeout=15) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


def wait_ready():
    for _ in range(30):
        try:
            code, spec = request("GET", "/openapi.json")
            if code == 200 and "/runs/{run_id}/dispatch" in spec["paths"]:
                return
        except URLError:
            pass
        time.sleep(0.5)
    raise RuntimeError("Updated API did not become ready")


async def publish_bad_message():
    broker = Broker()
    try:
        await broker.connect()
        await broker.channel.default_exchange.publish(
            aio_pika.Message(body=b'{"run_id":123}', delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=broker.queue.name, mandatory=True,
        )
    finally:
        await broker.close()


def prepare():
    wait_ready()
    key = "async-check-" + uuid4().hex
    code, normal = request("POST", "/runs", PAYLOAD, key)
    assert code == 201 and normal["dispatch_status"] == "CONFIRMED", (code, normal)
    assert normal["status"] == "PENDING"
    code, replay = request("POST", "/runs", PAYLOAD, key)
    assert code == 200 and replay["id"] == normal["id"]
    for _ in range(2):
        assert request("POST", "/runs/" + normal["id"] + "/dispatch")[0] == 202
    code, cancel = request("POST", "/runs", PAYLOAD)
    assert code == 201 and cancel["dispatch_status"] == "CONFIRMED"
    assert request("POST", "/runs/" + cancel["id"] + "/cancel")[1]["cancel_requested"] is True
    assert request("GET", "/runs/" + normal["id"])[1]["status"] == "PENDING"
    asyncio.run(publish_bad_message())
    FIXTURE.write_text(json.dumps({"normal": normal["id"], "cancel": cancel["id"], "key": key}), encoding="utf-8")
    print("Worker stopped: submissions confirmed, remain PENDING; duplicate dispatches queued")
    print("Normal run:", normal["id"])


async def verify_steps(fixture):
    try:
        async with session_scope() as session:
            for name, expected in (("normal", 3), ("cancel", 0)):
                count = await session.scalar(select(func.count()).select_from(Step).where(Step.run_id == UUID(fixture[name])))
                assert count == expected, (name, count)
    finally:
        await close_db()


def queue_info(name):
    url = urlsplit(str(get_settings().rabbitmq_url))
    credentials = (unquote(url.username) + ":" + unquote(url.password)).encode()
    management_base = os.environ.get("RABBITMQ_MANAGEMENT_URL", "http://127.0.0.1:15672").rstrip("/")
    req = Request(management_base + "/api/queues/%2F/" + name)
    req.add_header("Authorization", "Basic " + base64.b64encode(credentials).decode())
    with urlopen(req, timeout=5) as response:
        return json.loads(response.read())


def verify():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for _ in range(60):
        normal = request("GET", "/runs/" + fixture["normal"])[1]
        cancel = request("GET", "/runs/" + fixture["cancel"])[1]
        if normal["status"] == "COMPLETED" and cancel["status"] == "CANCELLED":
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Worker did not finish queued tasks")
    assert normal["iteration_count"] == 2 and normal["result_payload"]["source"] == "postgresql"
    asyncio.run(verify_steps(fixture))
    code, replay = request("POST", "/runs", PAYLOAD, fixture["key"])
    assert code == 200 and replay["id"] == fixture["normal"] and replay["dispatch_status"] == "SKIPPED"
    assert request("POST", "/runs/" + fixture["normal"] + "/dispatch")[0] == 409
    assert request("POST", "/runs/" + str(uuid4()) + "/dispatch")[0] == 404
    for _ in range(30):
        main_queue = queue_info(get_settings().rabbitmq_queue)
        dead_queue = queue_info(get_settings().rabbitmq_queue + ".dead")
        if main_queue.get("messages", 0) == 0 and dead_queue.get("messages_ready", 0) >= 1:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Queue acknowledgements or dead-lettering did not settle")
    print("API -> RabbitMQ -> independent Worker -> Engine -> PostgreSQL: verified")
    print("Duplicate delivery: 2 iterations / 3 steps; cancellation before execution: 0 steps")
    print("Main queue empty; malformed message dead-lettered; terminal replay skipped")
    print("Completed run:", fixture["normal"])


if __name__ == "__main__":
    {"prepare": prepare, "verify": verify}[sys.argv[1]]()
