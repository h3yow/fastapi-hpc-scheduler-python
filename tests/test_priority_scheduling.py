"""Priority + FIFO scheduling and reject-reason differentiation."""
from __future__ import annotations

import logging

from app.errors import ErrorCode
from app.models import Job, JobState, NodeState, Resources
from app.scheduler import Scheduler
from app.state import SchedulerState

logger = logging.getLogger(__name__)


def _state():
    st = SchedulerState()
    st.add_node("node-1", cpus=4, memory_mb=8192, gpus=0)
    st.add_node("node-2", cpus=2, memory_mb=4096, gpus=1)
    return st


def _job(jid, priority, cpus, mem, gpus=0, created_at=0.0):
    return Job(
        id=jid, name=jid, state=JobState.PENDING, priority=priority,
        request=Resources(cpus=cpus, memory_mb=mem, gpus=gpus),
        run_time_ms=10, created_at=created_at,
    )


def test_higher_priority_runs_first_even_when_submitted_later():
    st = _state()
    sched = Scheduler(st)
    # low-priority arrives first and would consume node-1 fully;
    # high-priority arrives second but must win the only fitting node.
    st.jobs["low"] = _job("low", 0, 4, 8192, created_at=1.0)
    st.jobs["high"] = _job("high", 9, 4, 8192, created_at=2.0)

    logger.info("INPUT jobs=low(pri=0,arrive=1),high(pri=9,arrive=2) capacity node-1=4cpu")
    report = sched.tick()

    placed = [(p.job_id, p.node_id) for p in report.placements]
    logger.info("DECISION placements=%s rejects=%s",
                placed, [(r.job_id, r.reason.value) for r in report.rejects])
    assert placed[0] == ("high", "node-1")
    assert report.rejects[0].job_id == "low"
    assert report.rejects[0].reason is ErrorCode.INSUFFICIENT_RESOURCES
    assert st.jobs["high"].state is JobState.RUNNING
    assert st.jobs["low"].state is JobState.PENDING


def test_fifo_tie_break_within_same_priority():
    st = _state()
    sched = Scheduler(st)
    st.jobs["a"] = _job("a", 5, 2, 4096, created_at=10.0)
    st.jobs["b"] = _job("b", 5, 2, 4096, created_at=1.0)

    report = sched.tick()
    placed = [p.job_id for p in report.placements]
    logger.info("DECISION same-priority order=%s", placed)
    assert placed == ["b", "a"]


def test_gpu_job_goes_to_gpu_node():
    st = _state()
    sched = Scheduler(st)
    st.jobs["gpu-job"] = _job("gpu-job", 1, 1, 1024, gpus=1)
    report = sched.tick()
    assert report.placements[0].node_id == "node-2"
    assert st.nodes["node-2"].used.gpus == 1


def test_reject_reasons_are_distinguishable():
    st = _state()
    sched = Scheduler(st)

    # 1) drained nodes -> node_unavailable
    for node in st.nodes.values():
        node.state = NodeState.DRAINED
    st.jobs["drained-req"] = _job("drained-req", 0, 1, 1)
    report = sched.tick()
    assert report.rejects[0].reason is ErrorCode.NODE_UNAVAILABLE
    logger.info("REJECT job=drained-req reason=%s", report.rejects[0].reason.value)

    # 2) no nodes at all -> node_unavailable
    empty = SchedulerState()
    empty.jobs["lonely"] = _job("lonely", 0, 1, 1)
    report = Scheduler(empty).tick()
    assert report.rejects[0].reason is ErrorCode.NODE_UNAVAILABLE

    # 3) exceeds every capacity -> invalid_resources
    st2 = _state()
    st2.jobs["huge"] = _job("huge", 0, 999, 999999)
    report = Scheduler(st2).tick()
    assert report.rejects[0].reason is ErrorCode.INVALID_RESOURCES
    logger.info("REJECT job=huge reason=%s", report.rejects[0].reason.value)

    # 4) fits capacity but nodes full -> insufficient_resources
    st3 = _state()
    st3.nodes["node-1"].allocate(Resources(cpus=4, memory_mb=8192))
    st3.nodes["node-2"].state = NodeState.DRAINED
    st3.jobs["waiter"] = _job("waiter", 0, 3, 1024)
    report = Scheduler(st3).tick()
    assert report.rejects[0].reason is ErrorCode.INSUFFICIENT_RESOURCES
    logger.info("REJECT job=waiter reason=%s", report.rejects[0].reason.value)


def test_freed_capacity_allows_next_tick_placement():
    st = _state()
    sched = Scheduler(st)
    st.jobs["a"] = _job("a", 0, 4, 8192)
    sched.tick()
    st.jobs["b"] = _job("b", 0, 4, 8192)
    assert sched.tick().rejects[0].reason is ErrorCode.INSUFFICIENT_RESOURCES
    assert sched.release("a")
    report = sched.tick()
    assert report.placements[0].job_id == "b"
