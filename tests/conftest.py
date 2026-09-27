"""Shared fixtures and logging setup.

Logging is captured at INFO so test output shows the key inputs,
scheduling decisions and reject reasons (use ``--log-cli-level=INFO``).
"""
from __future__ import annotations

import logging
import os
import tempfile
import time

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

DEFAULT_NODES = [("node-1", 4, 8192, 0), ("node-2", 8, 16384, 1)]


@pytest.fixture(autouse=True)
def _enable_logs(caplog):
    caplog.set_level(logging.INFO, logger="scheduler")
    caplog.set_level(logging.INFO, logger="service")
    caplog.set_level(logging.INFO, logger="executor")
    caplog.set_level(logging.INFO, logger="http")
    yield


@pytest.fixture
def state_path(tmp_path):
    return str(tmp_path / "scheduler_state.json")


@pytest.fixture
def make_app(state_path):
    def _make(path=None, nodes=None):
        # nodes are only used on a fresh state file
        if nodes is not None:
            os.environ["HPC_NODES"] = ",".join(
                f"{nid}:{c}:{m}:{g}" for nid, c, m, g in nodes
            )
        else:
            os.environ["HPC_NODES"] = ",".join(
                f"{nid}:{c}:{m}:{g}" for nid, c, m, g in DEFAULT_NODES
            )
        return create_app(path or state_path)

    yield _make
    os.environ.pop("HPC_NODES", None)


@pytest.fixture
def client(make_app):
    app = make_app()
    with TestClient(app) as c:
        c.app.state.service._loop_interval = 0.02
        yield c


def wait_for(client, job_id, states, timeout=5.0):
    """Poll a job until it reaches one of ``states``."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.get(f"/api/v1/jobs/{job_id}").json()
        if last["state"] in states:
            return last
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {states}; last={last}")
