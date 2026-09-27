"""调度核心：纯内存资源账本 + 选作业/选节点策略，不做 I/O。

线程安全由调用方（Service 的锁）保证；本模块内部不加锁，保持纯粹可测。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass

from app.models import Job, Node, Resources


class ScheduleOutcome(str, enum.Enum):
    SCHEDULED = "scheduled"                        # 已分配节点
    QUEUED_NO_RESOURCES = "queued_no_resources"    # 资源不足，保持排队
    QUEUED_NODES_UNAVAILABLE = "queued_nodes_unavailable"  # 无健康节点


@dataclass
class ScheduleDecision:
    outcome: ScheduleOutcome
    job_id: str
    node_id: str | None = None


class Scheduler:
    """维护节点可用资源与健康状态。

    选节点策略：在健康且资源足够的节点中，选剩余资源最紧（best-fit）的，
    节点 id 字典序兜底，保证结果确定性。
    """

    def __init__(self, nodes: list[Node]) -> None:
        self._nodes: dict[str, Node] = {n.node_id: n for n in nodes}
        self._avail: dict[str, Resources] = {
            n.node_id: n.total for n in nodes
        }
        # 节点 -> 占用它的作业集合，用于审计与一致性恢复
        self._allocations: dict[str, set[str]] = {
            n.node_id: set() for n in nodes
        }

    def try_schedule(self, job: Job) -> ScheduleDecision:
        healthy = [nid for nid, n in self._nodes.items() if n.healthy]
        if not healthy:
            return ScheduleDecision(ScheduleOutcome.QUEUED_NODES_UNAVAILABLE,
                                    job.job_id)
        candidates = [
            nid for nid in healthy if job.resources.fits_in(self._avail[nid])
        ]
        if not candidates:
            return ScheduleDecision(ScheduleOutcome.QUEUED_NO_RESOURCES,
                                    job.job_id)
        # best-fit：剩余 cpu 最小者优先，其次内存，再按 id 排序保证确定性
        best = min(candidates,
                   key=lambda nid: (self._avail[nid].cpu,
                                    self._avail[nid].memory_mb, nid))
        self._avail[best] = self._avail[best] - job.resources
        self._allocations[best].add(job.job_id)
        return ScheduleDecision(ScheduleOutcome.SCHEDULED, job.job_id, best)

    def release(self, job: Job) -> None:
        """释放作业占用的资源；重复释放是安全的（幂等）。"""
        nid = job.node_id
        if nid is None or nid not in self._nodes:
            return
        if job.job_id not in self._allocations[nid]:
            return  # 已释放过，避免重复加回资源
        self._allocations[nid].discard(job.job_id)
        self._avail[nid] = self._avail[nid] + job.resources

    def set_node_health(self, node_id: str, healthy: bool) -> None:
        if node_id in self._nodes:
            self._nodes[node_id].healthy = healthy

    def available(self, node_id: str) -> Resources:
        return self._avail[node_id]

    def is_healthy(self, node_id: str) -> bool:
        return self._nodes[node_id].healthy

    def node_ids(self) -> list[str]:
        return sorted(self._nodes)
