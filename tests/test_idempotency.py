"""Idempotent submission semantics."""
from __future__ import annotations

import logging
import threading

from tests.conftest import wait_for

logger = logging.getLogger(__name__)


def test_same_key_returns_same_job_and_uses_resources_once(client):
    body = {
        "name": "once", "cpus": 4, "memory_mb": 8192,
        "run_time_ms": 20, "idempotency_key": "key-1",
    }
    first = client.post("/api/v1/jobs", json=body)
    second = client.post("/api/v1/jobs", json=body)
    third = client.post("/api/v1/jobs", json=body)

    assert first.status_code == 201
    jid = first.json()["job"]["id"]
    assert second.json()["job"]["id"] == jid
    assert third.json()["job"]["id"] == jid
    assert first.json()["replayed"] is False
    assert second.json()["replayed"] is True

    jobs = client.get("/api/v1/jobs").json()
    matching = [j for j in jobs if j["id"] == jid]
    assert len(matching) == 1

    wait_for(client, jid, {"succeeded"})
    nodes = client.get("/api/v1/nodes").json()
    # exactly one job's worth of resources was ever used; now all released
    assert all(n["used"]["cpus"] == 0 for n in nodes)
    logger.info("idempotent key=key-1 produced one job=%s", jid)


def test_same_key_different_payload_is_conflict(client):
    body = {"name": "a", "cpus": 1, "memory_mb": 128, "idempotency_key": "k"}
    client.post("/api/v1/jobs", json=body)
    r = client.post("/api/v1/jobs", json={**body, "cpus": 2})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"
    logger.info("REJECT key=k code=idempotency_conflict")


def test_concurrent_duplicate_submits_create_one_job(client):
    body = {
        "name": "race", "cpus": 2, "memory_mb": 256,
        "run_time_ms": 30, "idempotency_key": "race-key",
    }
    results: list[str] = []
    barrier = threading.Barrier(6)

    def submit():
        barrier.wait()
        r = client.post("/api/v1/jobs", json=body)
        results.append(r.json()["job"]["id"])

    threads = [threading.Thread(target=submit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("concurrent idempotent submissions -> %s", results)
    assert len(set(results)) == 1
    jobs = client.get("/api/v1/jobs").json()
    assert sum(1 for j in jobs if j["name"] == "race") == 1
    wait_for(client, results[0], {"succeeded"})
