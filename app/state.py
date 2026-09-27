"""In-memory cluster state guarded by a single re-entrant lock.

All mutations of jobs/nodes/idempotency keys happen while holding
:attr:`SchedulerState.lock`. The state knows how to snapshot itself to
plain dicts (for the :mod:`app.storage` layer) and how to recover from a
snapshot - on recovery jobs that were ``running`` when the process died
are requeued to ``pending`` and their node allocations are dropped.
"""
from __future__ import annotations

import threading
from typing import Iterable

from app.models import Job, JobState, Node, NodeState, Resources

SNAPSHOT_VERSION = 1


class SchedulerState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.jobs: dict[str, Job] = {}
        self.nodes: dict[str, Node] = {}
        # node_id -> list of job ids currently placed there
        self.placements: dict[str, list[str]] = {}
        # idempotency_key -> {"job_id": ..., "fingerprint": ...}
        self.idempotency: dict[str, dict] = {}

    # ------------------------------------------------------------- nodes
    def add_node(
        self,
        node_id: str,
        cpus: int,
        memory_mb: int,
        gpus: int = 0,
    ) -> Node:
        with self.lock:
            node = Node(
                id=node_id,
                capacity=Resources(cpus=cpus, memory_mb=memory_mb, gpus=gpus),
            )
            self.nodes[node_id] = node
            self.placements.setdefault(node_id, [])
            return node

    def running_jobs_on(self, node_id: str) -> list[str]:
        return list(self.placements.get(node_id, ()))

    # --------------------------------------------------------- snapshot
    def snapshot(self) -> dict:
        with self.lock:
            return {
                "version": SNAPSHOT_VERSION,
                "jobs": {jid: job.model_dump() for jid, job in self.jobs.items()},
                "nodes": {nid: node.model_dump() for nid, node in self.nodes.items()},
                "placements": {
                    nid: list(jids) for nid, jids in self.placements.items()
                },
                "idempotency": dict(self.idempotency),
            }

    def restore(self, data: dict | None) -> None:
        with self.lock:
            if not data:
                return
            self.jobs = {
                jid: Job(**payload) for jid, payload in data.get("jobs", {}).items()
            }
            self.nodes = {
                nid: Node(**payload)
                for nid, payload in data.get("nodes", {}).items()
            }
            self.placements = {
                nid: list(jids) for nid, jids in data.get("placements", {}).items()
            }
            self.idempotency = dict(data.get("idempotency", {}))
            # Crash recovery: nothing was truly executing after a restart.
            # Free every allocation and requeue interrupted jobs.
            for node in self.nodes.values():
                node.used = Resources(cpus=0, memory_mb=0, gpus=0)
                node.state = NodeState(node.state)
            for job in self.jobs.values():
                if job.state is JobState.RUNNING:
                    job.state = JobState.PENDING
                    job.node_id = None
                    job.touch()
            for nid in self.placements:
                self.placements[nid] = [
                    jid
                    for jid in self.placements[nid]
                    if self.jobs.get(jid) is not None
                    and self.jobs[jid].state is JobState.RUNNING
                ]
