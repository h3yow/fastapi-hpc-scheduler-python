"""Scheduling policy.

Jobs are picked in *priority desc, arrival asc* order and placed onto
healthy nodes using a stable first-fit order (node id). A single
:meth:`Scheduler.tick` runs under the state lock so concurrent requests
can never over-allocate or double-place a job.

The scheduler never raises for "nothing fits right now": per-job reject
reasons are returned in the tick report instead, so the caller can log
them and the HTTP layer can distinguish *node unavailable* from
*insufficient resources*.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from app.errors import ErrorCode
from app.models import Job, JobState, NodeState, Resources
from app.state import SchedulerState

logger = logging.getLogger("scheduler")


@dataclass
class Placement:
    job_id: str
    node_id: str


@dataclass
class Reject:
    job_id: str
    reason: ErrorCode
    detail: str


@dataclass
class TickReport:
    placements: list[Placement] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)


class Scheduler:
    def __init__(self, state: SchedulerState) -> None:
        self.state = state

    # ----------------------------------------------------------- tick
    def tick(self) -> TickReport:
        report = TickReport()
        with self.state.lock:
            pending = [
                job
                for job in self.state.jobs.values()
                if job.state is JobState.PENDING
            ]
            # Higher priority first; ties broken by arrival time, then by
            # job id for a total deterministic order.
            pending.sort(key=lambda j: (-j.priority, j.created_at, j.id))

            # Snapshot per-node available resources for this tick so that a
            # single large request cannot be satisfied twice.
            available = {
                nid: node.available.model_copy()
                for nid, node in self.state.nodes.items()
            }
            healthy = {
                nid: node.state is NodeState.HEALTHY
                for nid, node in self.state.nodes.items()
            }

            for job in pending:
                if not self._request_positive(job):
                    report.rejects.append(
                        Reject(job.id, ErrorCode.INVALID_RESOURCES,
                               "resource request must be positive")
                    )
                    logger.warning(
                        "schedule reject job=%s reason=%s detail=zero-or-negative",
                        job.id, ErrorCode.INVALID_RESOURCES.value,
                    )
                    continue

                exceeds_capacity = not self._fits_any_when_empty(job)
                if not self.state.nodes:
                    report.rejects.append(
                        Reject(job.id, ErrorCode.NODE_UNAVAILABLE,
                               "no nodes registered")
                    )
                    logger.warning(
                        "schedule reject job=%s reason=%s",
                        job.id, ErrorCode.NODE_UNAVAILABLE.value,
                    )
                    continue

                target = self._find_node(job, available, healthy)
                if target is None:
                    if not any(healthy.values()):
                        reason = ErrorCode.NODE_UNAVAILABLE
                        detail = "all nodes are drained"
                    elif exceeds_capacity or not self._fits_any_when_empty(job):
                        reason = ErrorCode.INVALID_RESOURCES
                        detail = "request exceeds every node capacity"
                    else:
                        reason = ErrorCode.INSUFFICIENT_RESOURCES
                        detail = "no healthy node has enough free resources"
                    report.rejects.append(Reject(job.id, reason, detail))
                    logger.warning(
                        "schedule reject job=%s priority=%s request=(cpus=%s,mem=%s,gpus=%s) reason=%s detail=%s",
                        job.id, job.priority, job.request.cpus,
                        job.request.memory_mb, job.request.gpus,
                        reason.value, detail,
                    )
                    continue

                self._place(job, target, available)
                report.placements.append(Placement(job.id, target.id))
                logger.info(
                    "schedule place job=%s priority=%s node=%s request=(cpus=%s,mem=%s,gpus=%s)",
                    job.id, job.priority, target.id,
                    job.request.cpus, job.request.memory_mb, job.request.gpus,
                )
        return report

    # -------------------------------------------------------- release
    def release(self, job_id: str) -> bool:
        """Release whatever the job currently holds. Idempotent.

        Returns True when an allocation was actually freed.
        """
        with self.state.lock:
            job = self.state.jobs.get(job_id)
            if job is None or job.node_id is None:
                return False
            node = self.state.nodes.get(job.node_id)
            if node is None:
                job.node_id = None
                return False
            node.release(job.request)
            if job_id in self.state.placements.get(node.id, ()):
                self.state.placements[node.id].remove(job_id)
            logger.info(
                "release job=%s node=%s freed=(cpus=%s,mem=%s,gpus=%s) available=(cpus=%s,mem=%s,gpus=%s)",
                job_id, node.id,
                job.request.cpus, job.request.memory_mb, job.request.gpus,
                node.available.cpus, node.available.memory_mb, node.available.gpus,
            )
            job.node_id = None
            return True

    # --------------------------------------------------------- helpers
    @staticmethod
    def _request_positive(job: Job) -> bool:
        return not job.request.is_zero()

    def _fits_any_when_empty(self, job: Job) -> bool:
        return any(
            job.request.fits_in(node.capacity) for node in self.state.nodes.values()
        )

    def _find_node(
        self,
        job: Job,
        available: dict[str, Resources],
        healthy: dict[str, bool],
    ):
        for nid in sorted(self.state.nodes):
            node = self.state.nodes[nid]
            if not healthy[nid]:
                continue
            if job.request.fits_in(available[nid]):
                return node
        return None

    def _place(
        self,
        job: Job,
        node,
        available: dict[str, Resources],
    ) -> None:
        node.allocate(job.request)
        available[node.id] = node.available.model_copy()
        job.state = JobState.RUNNING
        job.node_id = node.id
        job.attempts += 1
        job.touch()
        self.state.placements.setdefault(node.id, []).append(job.id)
