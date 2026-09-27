"""领域模型：作业、节点、资源与状态机定义（不含任何 I/O 逻辑）。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field

from pydantic import BaseModel, Field


class JobStatus(str, enum.Enum):
    PENDING = "pending"      # 待调度
    RUNNING = "running"      # 运行中
    SUCCEEDED = "succeeded"  # 成功
    FAILED = "failed"        # 失败
    CANCELLED = "cancelled"  # 已取消


# 合法状态流转表：状态机唯一定义处
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}),
    JobStatus.FAILED: frozenset({JobStatus.PENDING}),  # 失败重试
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})


def can_transition(src: JobStatus, dst: JobStatus) -> bool:
    return dst in ALLOWED_TRANSITIONS.get(src, frozenset())


@dataclass(frozen=True)
class Resources:
    cpu: int
    memory_mb: int

    def fits_in(self, other: "Resources") -> bool:
        return self.cpu <= other.cpu and self.memory_mb <= other.memory_mb

    def __sub__(self, other: "Resources") -> "Resources":
        return Resources(self.cpu - other.cpu, self.memory_mb - other.memory_mb)

    def __add__(self, other: "Resources") -> "Resources":
        return Resources(self.cpu + other.cpu, self.memory_mb + other.memory_mb)


@dataclass
class Node:
    node_id: str
    total: Resources
    healthy: bool = True


@dataclass
class Job:
    job_id: str
    idempotency_key: str
    name: str
    priority: int
    resources: Resources
    status: JobStatus = JobStatus.PENDING
    node_id: str | None = None
    duration_seconds: float = 0.1   # 模拟执行时长（本地可运行）
    should_fail: bool = False       # 模拟失败（便于测试重试）
    attempt: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: str | None = None

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


# ---- HTTP 层 schema ----

class JobSubmitRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    priority: int = Field(default=0, ge=0, le=1000)
    cpu: int = Field(ge=1)
    memory_mb: int = Field(ge=1)
    duration_seconds: float = Field(default=0.1, ge=0.0, le=3600.0)
    should_fail: bool = False


class JobView(BaseModel):
    job_id: str
    idempotency_key: str
    name: str
    priority: int
    cpu: int
    memory_mb: int
    status: JobStatus
    node_id: str | None
    attempt: int
    created_at: float
    updated_at: float
    error: str | None


class NodeView(BaseModel):
    node_id: str
    healthy: bool
    total_cpu: int
    total_memory_mb: int
    avail_cpu: int
    avail_memory_mb: int
