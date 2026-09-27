"""Failure normalisation, retry limits, and timeout handling."""
from __future__ import annotations

import logging

from tests.conftest import wait_for

logger = logging.getLogger(__name__)


def test_failed_job_can_be_retried_and_succeeds(client):
    jid = client.post("/api/v1/jobs", json={
        "name": "flaky", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
        "fail": True, "max_attempts": 2,
    }).json()["job"]["id"]

    failed = wait_for(client, jid, {"failed"})
    assert failed["attempts"] == 1
    logger.info("job=%s failed first attempt, retrying", jid)

    # resources must have been released after the failure
    assert all(n["used"]["cpus"] == 0 for n in client.get("/api/v1/nodes").json())

    r = client.post(f"/api/v1/jobs/{jid}/retry")
    assert r.status_code == 200
    assert r.json()["state"] == "pending"

    done = wait_for(client, jid, {"succeeded"})
    assert done["attempts"] == 2


def test_retry_rejected_when_attempts_exhausted(client):
    jid = client.post("/api/v1/jobs", json={
        "name": "doomed", "cpus": 1, "memory_mb": 128, "run_time_ms": 10,
        "fail": True, "max_attempts": 1,
    }).json()["job"]["id"]
    wait_for(client, jid, {"failed"})

    r = client.post(f"/api/v1/jobs/{jid}/retry")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "job_not_retryable"
    logger.info("REJECT retry job=%s code=job_not_retryable detail=attempts exhausted", jid)


def test_retry_only_allowed_from_failed_state(client):
    # pending job cannot be retried
    jid = client.post("/api/v1/jobs", json={
        "name": "held", "cpus": 1, "memory_mb": 128, "gpus": 1,
        "run_time_ms": 3000,
    }).json()["job"]["id"]
    # take the GPU with a blocker so it stays pending
    client.post("/api/v1/jobs", json={
        "name": "gpu-hold", "cpus": 8, "memory_mb": 16384, "gpus": 1,
        "run_time_ms": 3000,
    })
    import time
    time.sleep(0.15)
    r = client.post(f"/api/v1/jobs/{jid}/retry")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "job_not_retryable"
    logger.info("REJECT retry-pending job=%s code=job_not_retryable", jid)


def test_timeout_is_normalised_to_failure_and_releases_resources(client):
    jid = client.post("/api/v1/jobs", json={
        "name": "slow", "cpus": 2, "memory_mb": 256,
        "run_time_ms": 200, "timeout_ms": 50, "max_attempts": 1,
    }).json()["job"]["id"]
    wait_for(client, jid, {"failed"}, timeout=5)
    nodes = client.get("/api/v1/nodes").json()
    logger.info("timeout job=%s failed; node usage=%s",
                jid, [(n["id"], n["used"]["cpus"]) for n in nodes])
    assert all(n["used"]["cpus"] == 0 for n in nodes)
