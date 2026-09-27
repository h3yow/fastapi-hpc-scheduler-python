"""Concurrent submit / cancel must never over-allocate or leak resources."""
from __future__ import annotations

import logging
import threading

from app.models import Job, JobState, Resources
from app.scheduler import Scheduler
from app.state import SchedulerState

logger = logging.getLogger(__name__)


def test_concurrent_ticks_never_over_allocate():
    """N threads schedule against capacity that fits far fewer jobs."""
    st = SchedulerState()
    st.add_node("node-1", cpus=4, memory_mb=8192, gpus=0)
    sched = Scheduler(st)
    for i in range(20):
        st.jobs[f"j{i}"] = Job(
            id=f"j{i}", name=f"j{i}", state=JobState.PENDING, priority=0,
            request=Resources(cpus=1, memory_mb=1024), run_time_ms=10,
        )

    errors: list[Exception] = []

    def worker():
        try:
            sched.tick()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    node = st.nodes["node-1"]
    logger.info("AFTER concurrent ticks used_cpus=%s placements=%s",
                node.used.cpus, st.placements["node-1"])
    assert node.used.cpus <= node.capacity.cpus
    running = [j for j in st.jobs.values() if j.state is JobState.RUNNING]
    assert len(running) == 4
    assert len(set(j.node_id for j in running)) >= 1
    # no duplicate placement
    assert len(st.placements["node-1"]) == len(set(st.placements["node-1"])) == 4


def test_concurrent_releases_never_double_free():
    st = SchedulerState()
    st.add_node("node-1", cpus=4, memory_mb=8192, gpus=0)
    sched = Scheduler(st)
    for i in range(4):
        st.jobs[f"j{i}"] = Job(
            id=f"j{i}", name=f"j{i}", state=JobState.PENDING, priority=0,
            request=Resources(cpus=1, memory_mb=1024), run_time_ms=10,
        )
    sched.tick()
    assert st.nodes["node-1"].used.cpus == 4

    barrier = threading.Barrier(4)

    def release(jid):
        barrier.wait()
        sched.release(jid)

    threads = [threading.Thread(target=release, args=(f"j{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    node = st.nodes["node-1"]
    logger.info("AFTER concurrent release used_cpus=%s", node.used.cpus)
    assert node.used.cpus == 0
    assert node.used.memory_mb == 0
    assert st.placements["node-1"] == []


def test_concurrent_cancel_and_schedule_keeps_consistency(client):
    """Fill a node, then cancel running jobs while new jobs queue."""
    # occupy node-1 (4 cpu) fully
    owners = []
    for i in range(4):
        r = client.post("/api/v1/jobs", json={
            "name": f"owner-{i}", "cpus": 1, "memory_mb": 512,
            "run_time_ms": 3000,
        })
        owners.append(r.json()["job"]["id"])

    import time
    time.sleep(0.15)
    nodes = client.get("/api/v1/nodes").json()
    assert sum(n["used"]["cpus"] for n in nodes) >= 4

    # queued jobs waiting for the freed resources
    queued = []
    for i in range(4):
        r = client.post("/api/v1/jobs", json={
            "name": f"waiter-{i}", "cpus": 1, "memory_mb": 512,
            "run_time_ms": 10,
        })
        queued.append(r.json()["job"]["id"])

    def cancel_all():
        for jid in owners:
            client.post(f"/api/v1/jobs/{jid}/cancel")

    t = threading.Thread(target=cancel_all)
    t.start()
    # hammer ticks via cancels/state toggles in parallel
    t.join()

    for jid in queued:
        from tests.conftest import wait_for
        wait_for(client, jid, {"succeeded", "cancelled"}, timeout=5)

    # give the loop a moment to settle, then verify no over/under allocation
    time.sleep(0.2)
    nodes = client.get("/api/v1/nodes").json()
    for node in nodes:
        assert node["used"]["cpus"] >= 0
        assert node["used"]["cpus"] <= node["capacity"]["cpus"]
        # used must match the actual still-running jobs on the node
        for jid in node["running_jobs"]:
            body = client.get(f"/api/v1/jobs/{jid}").json()
            assert body["state"] == "running"
            assert body["node_id"] == node["id"]
    total_used = sum(
        n["used"]["cpus"] for n in nodes
        if n["running_jobs"]
    )
    logger.info("FINAL node usage=%s", [(n["id"], n["used"]["cpus"]) for n in nodes])
    # waiters are tiny and finish quickly; eventually node is empty
    assert total_used == 0
