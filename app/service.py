"""Application service: the single coordinator between HTTP, scheduler,
executor and storage.

Concurrency model
-----------------
* Every state mutation happens under ``state.lock`` and is immediately
  persisted before the lock is released, so accepted jobs/allocations
  survive process restarts.
* A background asyncio loop calls :meth:`SchedulerService.tick` whenever
  something might have changed (wake event), and submissions/finishes
  also trigger a tick. Overlapping ticks are collapsed via
  ``_tick_guard`` so two concurrent requests never schedule twice.
"""
from __future__ import annotations

import asyncio
import logging
import os

from app.errors import ErrorCode, SchedulerError
from app.executor import Executor
from app.idempotency import fingerprint
from app.models import (
    Job,
    JobState,
    JobSubmitRequest,
    NodeState,
)
from app.scheduler import Scheduler, TickReport
from app.state import SchedulerState
from app.state_machine import ensure_transition
from app.storage import Store

logger = logging.getLogger("service")


def default_nodes() -> list[tuple[str, int, int, int]]:
    raw = os.environ.get("HPC_NODES", "node-1:4:8192:0,node-2:8:16384:1")
    nodes: list[tuple[str, int, int, int]] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":")
        if len(parts) != 4:
            raise ValueError(f"invalid HPC_NODES entry: {chunk!r}")
        nid, cpus, mem, gpus = parts
        nodes.append((nid, int(cpus), int(mem), int(gpus)))
    return nodes


class SchedulerService:
    def __init__(
        self,
        state_path: str = "scheduler_state.json",
        nodes: list[tuple[str, int, int, int]] | None = None,
        loop_interval: float = 0.2,
    ) -> None:
        self.state = SchedulerState()
        self.store = Store(state_path)
        self.scheduler = Scheduler(self.state)
        self.executor = Executor(self.state, self.scheduler)
        self._nodes_config = nodes
        self._loop_interval = loop_interval
        self._wake = asyncio.Event()
        self._tick_guard = asyncio.Lock()
        self._bg_task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self.state.restore(self.store.load())
        if not self.state.nodes:
            for nid, cpus, mem, gpus in self._nodes_config or default_nodes():
                self.state.add_node(nid, cpus, mem, gpus)
        self._persist()
        logger.info(
            "service started nodes=%s jobs=%s",
            len(self.state.nodes), len(self.state.jobs),
        )
        self._bg_task = asyncio.create_task(self._run_loop(), name="scheduler-loop")

    async def stop(self) -> None:
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._bg_task = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        # Final flush: an executor task may have committed a state change
        # just before being cancelled; the next start() requeues survivors.
        self._persist()

    async def _run_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._loop_interval)
                self._wake.clear()
            except asyncio.TimeoutError:
                pass
            await self.tick()

    # ------------------------------------------------------------ operations
    def submit_job(self, request: JobSubmitRequest) -> tuple[Job, bool]:
        """Return ``(job, created)``. Replays return the original job."""
        fp = fingerprint(request)
        with self.state.lock:
            if request.idempotency_key is not None:
                existing = self.state.idempotency.get(request.idempotency_key)
                if existing is not None:
                    job = self.state.jobs.get(existing["job_id"])
                    if existing["fingerprint"] != fp:
                        raise SchedulerError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "idempotency key was already used with a different request",
                            status_code=409,
                        )
                    if job is not None:
                        logger.info(
                            "submit replay key=%s job=%s state=%s",
                            request.idempotency_key, job.id, job.state.value,
                        )
                        return job, False
            self._validate_request(request)
            job = Job(
                id=Job.new_id(),
                name=request.name,
                state=JobState.PENDING,
                priority=request.priority,
                request=request.model_dump(include={"cpus", "memory_mb", "gpus"}),
                run_time_ms=request.run_time_ms,
                fail=request.fail,
                timeout_ms=request.timeout_ms,
                idempotency_key=request.idempotency_key,
                max_attempts=request.max_attempts,
            )
            self.state.jobs[job.id] = job
            if request.idempotency_key is not None:
                self.state.idempotency[request.idempotency_key] = {
                    "job_id": job.id,
                    "fingerprint": fp,
                }
            self._persist()
            logger.info(
                "submit accept job=%s name=%s priority=%s request=(cpus=%s,mem=%s,gpus=%s) key=%s",
                job.id, job.name, job.priority,
                request.cpus, request.memory_mb, request.gpus,
                request.idempotency_key,
            )
        self.request_tick()
        return job, True

    def get_job(self, job_id: str) -> Job:
        with self.state.lock:
            job = self.state.jobs.get(job_id)
            if job is None:
                raise SchedulerError(
                    ErrorCode.JOB_NOT_FOUND, f"job {job_id!r} not found", 404
                )
            return job.model_copy()

    def list_jobs(self) -> list[Job]:
        with self.state.lock:
            return [j.model_copy() for j in self.state.jobs.values()]

    def cancel_job(self, job_id: str) -> Job:
        with self.state.lock:
            job = self.state.jobs.get(job_id)
            if job is None:
                raise SchedulerError(
                    ErrorCode.JOB_NOT_FOUND, f"job {job_id!r} not found", 404
                )
            if job.state is JobState.CANCELLED:
                # Repeated cancellation must be distinguishable.
                raise SchedulerError(
                    ErrorCode.JOB_ALREADY_CANCELLED,
                    f"job {job_id!r} is already cancelled",
                    status_code=409,
                )
            if job.state in (JobState.SUCCEEDED, JobState.FAILED):
                raise SchedulerError(
                    ErrorCode.JOB_NOT_CANCELLABLE,
                    f"job {job_id!r} in state {job.state.value} cannot be cancelled",
                    status_code=409,
                )
            ensure_transition(job.state, JobState.CANCELLED)
            job.state = JobState.CANCELLED
            job.touch()
            was_running = job.node_id is not None
            if was_running:
                # State change + resource release + persistence are one
                # atomic step under the lock, so no crash window can leak
                # an allocation.
                self.scheduler.release(job_id)
            self._persist()
            logger.info(
                "cancel accept job=%s previous=%s running=%s",
                job_id, "running" if was_running else "pending", was_running,
            )
        if was_running:
            self.request_tick()
        return self.get_job(job_id)

    def retry_job(self, job_id: str) -> Job:
        with self.state.lock:
            job = self.state.jobs.get(job_id)
            if job is None:
                raise SchedulerError(
                    ErrorCode.JOB_NOT_FOUND, f"job {job_id!r} not found", 404
                )
            if job.state is not JobState.FAILED:
                raise SchedulerError(
                    ErrorCode.JOB_NOT_RETRYABLE,
                    f"job {job_id!r} in state {job.state.value} cannot be retried",
                    status_code=409,
                )
            if job.attempts >= job.max_attempts:
                raise SchedulerError(
                    ErrorCode.JOB_NOT_RETRYABLE,
                    f"job {job_id!r} exhausted {job.max_attempts} attempt(s)",
                    status_code=409,
                )
            ensure_transition(JobState.FAILED, JobState.PENDING)
            job.state = JobState.PENDING
            job.node_id = None
            job.fail = False  # the new attempt runs normally
            job.touch()
            self._persist()
            logger.info(
                "retry accept job=%s attempts=%s/%s",
                job_id, job.attempts, job.max_attempts,
            )
        self.request_tick()
        return self.get_job(job_id)

    # --------------------------------------------------------------- nodes
    def list_nodes(self):
        from app.models import NodeView

        with self.state.lock:
            return [
                NodeView(
                    id=node.id,
                    state=node.state,
                    capacity=node.capacity,
                    used=node.used,
                    available=node.available,
                    running_jobs=self.state.running_jobs_on(node.id),
                )
                for node in self.state.nodes.values()
            ]

    def set_node_state(self, node_id: str, drained: bool) -> None:
        with self.state.lock:
            node = self.state.nodes.get(node_id)
            if node is None:
                raise SchedulerError(
                    ErrorCode.NODE_NOT_FOUND, f"node {node_id!r} not found", 404
                )
            node.state = NodeState.DRAINED if drained else NodeState.HEALTHY
            self._persist()
            logger.info(
                "node state node=%s drained=%s", node_id, drained,
            )
        self.request_tick()

    # ------------------------------------------------------------- scheduling
    async def tick(self) -> TickReport:
        if self._tick_guard.locked():
            return TickReport()
        async with self._tick_guard:
            report = await asyncio.to_thread(self.scheduler.tick)
            if report.placements:
                self._persist()
                for placement in report.placements:
                    self._spawn_execution(placement.job_id)
            return report

    def request_tick(self) -> None:
        self._wake.set()

    def _spawn_execution(self, job_id: str) -> None:
        task = asyncio.create_task(
            self.executor.launch(job_id, on_finish=self._on_job_finished),
            name=f"exec-{job_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _on_job_finished(self) -> None:
        self._persist()
        self.request_tick()

    # --------------------------------------------------------------- helpers
    def _validate_request(self, request: JobSubmitRequest) -> None:
        with self.state.lock:
            if not self.state.nodes:
                raise SchedulerError(
                    ErrorCode.NODE_UNAVAILABLE, "no nodes registered", 503
                )
            too_big = all(
                request.cpus > node.capacity.cpus
                or request.memory_mb > node.capacity.memory_mb
                or request.gpus > node.capacity.gpus
                for node in self.state.nodes.values()
            )
            if too_big:
                raise SchedulerError(
                    ErrorCode.INVALID_RESOURCES,
                    "resource request exceeds every node capacity",
                    status_code=422,
                )

    def _persist(self) -> None:
        # Called while holding state.lock (or from lifecycle code).
        self.store.save(self.state.snapshot())
