"""The executor must normalise exceptions and ignore out-of-order results."""
from __future__ import annotations

import asyncio
import logging

import pytest

from app.executor import Executor
from app.models import Job, JobState, Resources
from app.scheduler import Scheduler
from app.state import SchedulerState

logger = logging.getLogger(__name__)


def _running_job(state, jid="j1", fail=False, run_time_ms=10, timeout_ms=None):
    state.add_node("node-1", 4, 8192, gpus=0)
    state.nodes["node-1"].allocate(Resources(cpus=2, memory_mb=1024))
    state.placements["node-1"].append(jid)
    job = Job(
        id=jid, name=jid, state=JobState.RUNNING, priority=0,
        request=Resources(cpus=2, memory_mb=1024),
        run_time_ms=run_time_ms, fail=fail, timeout_ms=timeout_ms,
        node_id="node-1",
    )
    state.jobs[jid] = job
    return job


@pytest.mark.asyncio
async def test_execution_exception_becomes_failed_and_releases():
    state = SchedulerState()
    sched = Scheduler(state)

    async def boom(_):
        raise RuntimeError("simulated driver explosion")

    ex = Executor(state, sched, sleep=boom)
    _running_job(state)

    # must not raise out of the boundary
    await ex.launch("j1")

    assert state.jobs["j1"].state is JobState.FAILED
    assert state.nodes["node-1"].used.cpus == 0
    assert state.placements["node-1"] == []
    logger.info("underlying RuntimeError normalised to failed, resources released")


@pytest.mark.asyncio
async def test_late_completion_after_cancel_is_ignored_without_overrelease():
    state = SchedulerState()
    sched = Scheduler(state)

    async def slow(_):
        await asyncio.sleep(0.05)

    ex = Executor(state, sched, sleep=slow)
    _running_job(state)

    async def cancel_concurrently():
        await asyncio.sleep(0.01)
        assert sched.release("j1")
        state.jobs["j1"].state = JobState.CANCELLED

    await asyncio.gather(ex.launch("j1"), cancel_concurrently())

    assert state.jobs["j1"].state is JobState.CANCELLED
    # no negative / double release
    assert state.nodes["node-1"].used.cpus == 0
    logger.info("out-of-order completion dropped, allocation stays consistent")


@pytest.mark.asyncio
async def test_timeout_marks_failed():
    state = SchedulerState()
    sched = Scheduler(state)
    ex = Executor(state, sched)
    _running_job(state, run_time_ms=10, timeout_ms=1)
    await ex.launch("j1")
    assert state.jobs["j1"].state is JobState.FAILED
    assert state.nodes["node-1"].used.cpus == 0
