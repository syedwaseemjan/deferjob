from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator

import psycopg
import pytest

from deferjob import Defer

IMAGE = "postgres:16-alpine"
PASSWORD = "deferjob"
USER = "deferjob"
DB = "deferjob"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_dsn(dsn: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("SELECT 1")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"Postgres did not become ready: {last}")


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
    env = os.environ.get("DATABASE_URL")
    if env:
        _wait_for_dsn(env)
        yield env
        return

    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("set DATABASE_URL or install docker to run tests")

    port = _free_port()
    name = f"deferjob-test-{uuid.uuid4().hex[:8]}"
    cmd = [
        docker,
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "-e",
        f"POSTGRES_USER={USER}",
        "-e",
        f"POSTGRES_PASSWORD={PASSWORD}",
        "-e",
        f"POSTGRES_DB={DB}",
        "-p",
        f"127.0.0.1:{port}:5432",
        IMAGE,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        pytest.skip(f"could not start postgres: {exc.stderr}")

    url = f"postgresql://{USER}:{PASSWORD}@127.0.0.1:{port}/{DB}"
    try:
        _wait_for_dsn(url)
        yield url
    finally:
        subprocess.run(
            [docker, "stop", "-t", "1", name],
            check=False,
            capture_output=True,
        )


@pytest.fixture
def jobs(dsn: str) -> Iterator[Defer]:
    table = f"defer_jobs_{uuid.uuid4().hex[:10]}"
    client = Defer(dsn, table=table)
    client.install()
    try:
        yield client
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
