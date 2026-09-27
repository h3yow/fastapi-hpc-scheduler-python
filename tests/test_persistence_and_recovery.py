"""Durability across process restarts and node drain behaviour."""
from __future__ import annotations

import logging
import time

from fastapi.testclient import TestClient

from tests.conftest import wait_for

logger = logging.getLogger(__name__)


def test_restart_preserves_jobs_and_requeues_running(make_app, state_path):
    app = make_app()
    with TestClient(app) as c1:
        c1.post("/api/v1/jobs", json={
            "name": "quick", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
            "idempotency_key": "persist-1",
        })
        # a long-ish job will almost certainly be RUNNING at shutdown
        running = c1.post("/api/v1/jobs", json={
            "name": "long", "cpus": 2, "memory_mb": 256, "run_time_ms": 2000,
        }).json()["job"]["id"]
        time.sleep(0.15)
        assert c1.get(f"/api/v1/jobs/{running}").json()["state"] == "running"

    app2 = make_app()
    with TestClient(app2) as c2:
        jobs = {j["name"]: j for j in c2.get("/api/v1/jobs").json()}
        assert "quick" in jobs and "long" in jobs
        # interrupted execution is requeued, not stuck running
        assert jobs["long"]["state"] == "pending"
        # and its allocation was dropped
        assert all(n["used"]["cpus"] == 0 for n in c2.get("/api/v1/nodes").json())

        # idempotency records survive: replay returns the same job
        replay = c2.post("/api/v1/jobs", json={
            "name": "quick", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
            "idempotency_key": "persist-1",
        })
        assert replay.json()["replayed"] is True
        assert replay.json()["job"]["name"] == "quick"
        logger.info("restart recovery: long requeued, usage zero, idempotency intact")

        # the requeued job can now run to completion after restart
        wait_for(c2, jobs["long"]["id"], {"succeeded"}, timeout=5)


def test_drained_node_does_not_accept_placements(client):
    # drain node-1; a job that fits both nodes must land on node-2
    r = client.post("/api/v1/nodes/node-1/state", json={"drained": True})
    assert r.status_code == 200 and r.json()["state"] == "drained"

    jid = client.post("/api/v1/jobs", json={
        "name": "anywhere", "cpus": 1, "memory_mb": 128, "run_time_ms": 20,
    }).json()["job"]["id"]
    running = wait_for(client, jid, {"running", "succeeded"})
    # when already finished, inspect nodes' history is impossible; verify
    # via a long-running job instead
    holder = client.post("/api/v1/jobs", json={
        "name": "holder", "cpus": 1, "memory_mb": 128, "run_time_ms": 2000,
    }).json()["job"]["id"]
    time.sleep(0.15)
    assert client.get(f"/api/v1/jobs/{holder}").json()["node_id"] == "node-2"

    # GPU job also avoids drained node-1 (which has no GPU anyway) - drain
    # the healthy node too -> node_unavailable style reject via scheduling
    client.post("/api/v1/nodes/node-2/state", json={"drained": True})
    queued = client.post("/api/v1/jobs", json={
        "name": "queued", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
    }).json()["job"]["id"]
    time.sleep(0.25)
    assert client.get(f"/api/v1/jobs/{queued}").json()["state"] == "pending"
    logger.info("all nodes drained: job=%s held pending (node_unavailable)", queued)

    # undrain -> it schedules and finishes
    client.post("/api/v1/nodes/node-1/state", json={"drained": False})
    wait_for(client, queued, {"succeeded"}, timeout=5)
    assert running["state"] in ("running", "succeeded")
