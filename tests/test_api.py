"""HTTP 层：错误码映射、幂等响应、响应体可区分拒绝原因。"""
import logging
import time

import pytest
from fastapi.testclient import TestClient

from main import create_app

log = logging.getLogger("test")


@pytest.fixture()
def client():
    app = create_app(":memory:")
    with TestClient(app) as c:
        yield c


def submit(client, key, **kw):
    body = dict(idempotency_key=key, name=f"job-{key}", priority=0,
                cpu=1, memory_mb=256, duration_seconds=0.02)
    body.update(kw)
    log.info("POST /jobs %s", body)
    return client.post("/jobs", json=body)


def test_submit_and_get(client):
    r = submit(client, "api-1")
    assert r.status_code == 201, r.text
    job = r.json()
    r2 = client.get(f"/jobs/{job['job_id']}")
    assert r2.status_code == 200
    assert r2.json()["idempotency_key"] == "api-1"


def test_idempotent_replay_returns_same_job(client):
    a = submit(client, "api-idem").json()
    b = submit(client, "api-idem").json()
    assert a["job_id"] == b["job_id"]
    assert len(client.get("/jobs").json()) == 1


def test_idempotency_conflict(client):
    submit(client, "api-conf", cpu=1)
    r = submit(client, "api-conf", cpu=2)
    log.info("conflict response: %s", r.json())
    assert r.status_code == 409
    assert r.json()["code"] == "idempotency_conflict"


def test_invalid_resource_request(client):
    r = submit(client, "api-big", cpu=10_000)
    assert r.status_code == 422
    assert r.json()["code"] == "invalid_resource_request"


def test_not_found(client):
    r = client.get("/jobs/nope")
    assert r.status_code == 404
    assert r.json()["code"] == "job_not_found"


def test_double_cancel_distinct_error(client):
    job = submit(client, "api-cancel", duration_seconds=5.0).json()
    r1 = client.post(f"/jobs/{job['job_id']}/cancel")
    assert r1.status_code == 200
    r2 = client.post(f"/jobs/{job['job_id']}/cancel")
    log.info("double cancel response: %s", r2.json())
    assert r2.status_code == 409
    assert r2.json()["code"] == "already_cancelled"


def test_retry_flow(client):
    job = submit(client, "api-fail", should_fail=True).json()
    for _ in range(100):
        if client.get(f"/jobs/{job['job_id']}").json()["status"] == "failed":
            break
        time.sleep(0.02)
    r = client.post(f"/jobs/{job['job_id']}/retry")
    assert r.status_code == 200
    assert r.json()["status"] in ("pending", "running")


def test_nodes_endpoint(client):
    nodes = client.get("/nodes").json()
    log.info("nodes: %s", nodes)
    assert len(nodes) == 3
    assert all(n["avail_cpu"] == n["total_cpu"] for n in nodes)
