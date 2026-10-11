"""Use real services, but isolate all test state from the local demo."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]


class Harness:
    def __init__(self, env, log_dir):
        self.env = env
        self.log_dir = log_dir

    def run(self, module, *args, overrides=None, timeout=90):
        result = subprocess.run(
            [sys.executable, "-m", module, *args], cwd=ROOT,
            env={**self.env, **(overrides or {})}, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=timeout,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"{module} exited {result.returncode}\n{output}"
        return output

    @contextmanager
    def process(self, name, module, *args):
        log = self.log_dir / f"{name}.log"
        with log.open("w", encoding="utf-8") as handle:
            process = subprocess.Popen(
                [sys.executable, "-m", module, *args], cwd=ROOT, env=self.env,
                stdout=handle, stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                yield process
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

    def wait_api(self, process):
        for _ in range(100):
            if process.poll() is not None:
                pytest.fail((self.log_dir / "api.log").read_text(encoding="utf-8"))
            try:
                with urlopen(self.env["API_BASE_URL"] + "/openapi.json", timeout=1) as response:
                    paths = json.loads(response.read())["paths"]
                    if "/runs/{run_id}/steps" in paths:
                        return
            except (URLError, TimeoutError):
                pass
            time.sleep(0.1)
        pytest.fail("Isolated test API did not become ready")


@pytest.fixture(scope="session")
def harness(tmp_path_factory):
    target = os.environ.get("TEST_DATABASE_URL")
    if not target:
        pytest.fail("Set TEST_DATABASE_URL; tests never fall back to the demo database")
    url = make_url(target)
    if not url.database or not re.fullmatch(r"ai_agent_test(?:_[a-z0-9_]+)?", url.database):
        pytest.fail("TEST_DATABASE_URL must use ai_agent_test or ai_agent_test_*")
    # Reserve a free local port rather than using the user's API on port 8000.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = {
        **os.environ, "DATABASE_URL": target, "API_BASE_URL": f"http://127.0.0.1:{port}",
        "RABBITMQ_QUEUE": "agent.tests." + uuid4().hex,
        "WORKER_PREFETCH": "1", "WORKER_CONCURRENCY": "1",
        "LEASE_TTL_SECONDS": "6", "HEARTBEAT_INTERVAL_SECONDS": "1",
        "DECISION_TIMEOUT_SECONDS": "10", "DECISION_MAX_ATTEMPTS": "3",
        "DECISION_RETRY_BASE_SECONDS": "1", "DECISION_RETRY_JITTER_SECONDS": "0.2",
        "RUN_TIMEOUT_SECONDS": "300", "PYTHONUTF8": "1",
    }
    instance = Harness(env, tmp_path_factory.mktemp("services"))
    instance.run("alembic", "upgrade", "head")
    instance.run("scripts.seed_inventory")
    yield instance
    instance.run("scripts.cleanup_test_queue")


@pytest.fixture(scope="session")
def api(harness):
    port = harness.env["API_BASE_URL"].rsplit(":", 1)[1]
    with harness.process("api", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", port) as process:
        harness.wait_api(process)
        yield harness
