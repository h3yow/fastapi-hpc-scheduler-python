"""Illegal transitions, stable reject codes, and resource release on cancel."""
from __future__ import annotations

import logging
import time

from tests.conftest import wait_for

logger = logging.getLogger(__name__)


def test_cancel_pending_job_does_not_consume_resources(client):
    # fill node-2 (the only node with a GPU) with one long job
    holder = client.post("/api/v1/jobs", json={
        "name": "holder", "cpus": 8, "memory_mb": 16384, "gpus": 1,
        "run_time_ms": 3000,
    }).json()["job"]["id"]
    time.sleep(0.15)

    # this job also needs the GPU and therefore stays pending
    queued = client.post("/api/v1/jobs", json={
        "name": "queued", "cpus": 1, "memory_mb": 128, "gpus": 1,
        "run_time_ms": 100,
    }).json()["job"]["id"]
    time.sleep(0.1)
    assert client.get(f"/api/v1/jobs/{queued}").json()["state"] == "pending"

    r = client.post(f"/api/v1/jobs/{queued}/cancel")
    assert r.status_code == 200
    assert r.json()["state"] == "cancelled"

    nodes = client.get("/api/v1/nodes").json()
    node2 = next(n for n in nodes if n["id"] == "node-2")
    logger.info("after pending-cancel node-2 used=%s", node2["used"]["cpus"])
    assert node2["used"]["cpus"] == 8  # holder untouched
    assert node2["used"]["gpus"] == 1
    assert queued not in node2["running_jobs"]


def test_cancel_running_job_frees_resources_for_next_job(client):
    blocker = client.post("/api/v1/jobs", json={
        "name": "blocker", "cpus": 8, "memory_mb": 16384, "gpus": 1,
        "run_time_ms": 5000,
    }).json()["job"]["id"]
    time.sleep(0.15)

    waiter = client.post("/api/v1/jobs", json={
        "name": "waiter", "cpus": 8, "memory_mb": 16384, "gpus": 1,
        "run_time_ms": 30,
    }).json()["job"]["id"]
    time.sleep(0.1)
    assert client.get(f"/api/v1/jobs/{waiter}").json()["state"] == "pending"

    r = client.post(f"/api/v1/jobs/{blocker}/cancel")
    assert r.status_code == 200 and r.json()["state"] == "cancelled"

    wait_for(client, waiter, {"succeeded"}, timeout=5)
    nodes = client.get("/api/v1/nodes").json()
    assert all(n["used"]["cpus"] == 0 for n in nodes)
    logger.info("running-cancel freed node, waiter succeeded")


def test_double_cancel_is_rejected_with_stable_code(client):
    jid = client.post("/api/v1/jobs", json={
        "name": "x", "cpus": 1, "memory_mb": 128, "run_time_ms": 3000,
    }).json()["job"]["id"]
    time.sleep(0.1)
    first = client.post(f"/api/v1/jobs/{jid}/cancel")
    assert first.status_code == 200
    second = client.post(f"/api/v1/jobs/{jid}/cancel")
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "job_already_cancelled"
    logger.info("REJECT double-cancel job=%s code=job_already_cancelled", jid)


def test_cancel_terminal_succeeded_is_rejected(client):
    jid = client.post("/api/v1/jobs", json={
        "name": "quick", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
    }).json()["job"]["id"]
    wait_for(client, jid, {"succeeded"})
    r = client.post(f"/api/v1/jobs/{jid}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "job_not_cancellable"
    logger.info("REJECT cancel-succeeded job=%s code=job_not_cancellable", jid)


def test_reject_codes_are_distinguishable(client):
    missing = client.post("/api/v1/jobs/does-not-exist/cancel")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "job_not_found"

    bad = client.post("/api/v1/jobs", json={"name": "no", "cpus": -1, "memory_mb": 1})
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "validation_error"

    huge = client.post("/api/v1/jobs", json={"name": "huge", "cpus": 10000, "memory_mb": 1})
    assert huge.status_code == 422
    assert huge.json()["error"]["code"] == "invalid_resources"

    unknown_node = client.post("/api/v1/nodes/nope/state", json={"drained": True})
    assert unknown_node.status_code == 404
    assert unknown_node.json()["error"]["code"] == "node_not_found"


def test_state_transition_table():
    from app.models import JobState
    from app.state_machine import ALLOWED

    assert JobState.RUNNING in ALLOWED[JobState.PENDING]
    assert JobState.CANCELLED in ALLOWED[JobState.RUNNING]
    assert JobState.PENDING in ALLOWED[JobState.FAILED]
    assert ALLOWED[JobState.SUCCEEDED] == frozenset()
    assert ALLOWED[JobState.CANCELLED] == frozenset()
