"""Domain models: jobs, nodes, API request/response schemas.

Pure data, no I/O and no scheduling logic live here so the state layer,
the scheduler and the HTTP layer can evolve independently.
"""
from __future__ import annotations

import time
import uuid
from enum import Enum

from pydantic import BaseModel, Field


class JobState(str, Enum):
    PENDING = "pending"      # accepted, waiting to be scheduled
    RUNNING = "running"      # placed on a node and executing
    SUCCEEDED = "succeeded"  # terminal: execution finished ok
    FAILED = "failed"        # terminal for this attempt; retryable
    CANCELLED = "cancelled"  # terminal: cancelled by user


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})


class NodeState(str, Enum):
    HEALTHY = "healthy"    # accepts new placements
    DRAINED = "drained"    # alive but does not accept new placements


class Resources(BaseModel):
    """Resource request / capacity. All values are non-negative integers."""

    cpus: int = Field(default=0, ge=0)
    memory_mb: int = Field(default=0, ge=0)
    gpus: int = Field(default=0, ge=0)

    def fits_in(self, capacity: "Resources") -> bool:
        return (
            self.cpus <= capacity.cpus
            and self.memory_mb <= capacity.memory_mb
            and self.gpus <= capacity.gpus
        )

    def is_zero(self) -> bool:
        return self.cpus == 0 and self.memory_mb == 0 and self.gpus == 0


class Job(BaseModel):
    """The durable job aggregate."""

    id: str
    name: str
    state: JobState
    priority: int                       # larger value = higher priority
    request: Resources
    run_time_ms: int = Field(ge=0)     # simulated execution duration
    fail: bool = False                 # simulated execution outcome
    timeout_ms: int | None = Field(default=None, ge=0)
    idempotency_key: str | None = None
    attempts: int = 0
    max_attempts: int = 1
    node_id: str | None = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    version: int = 0                    # optimistic-concurrency token

    def touch(self) -> None:
        self.updated_at = time.time()
        self.version += 1

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


class Node(BaseModel):
    """A cluster node with fixed capacity and mutable usage/health."""

    id: str
    capacity: Resources
    used: Resources = Resources(cpus=0, memory_mb=0, gpus=0)
    state: NodeState = NodeState.HEALTHY

    @property
    def available(self) -> Resources:
        return Resources(
            cpus=self.capacity.cpus - self.used.cpus,
            memory_mb=self.capacity.memory_mb - self.used.memory_mb,
            gpus=self.capacity.gpus - self.used.gpus,
        )

    def can_fit(self, request: Resources) -> bool:
        return self.state is NodeState.HEALTHY and request.fits_in(self.available)

    def allocate(self, request: Resources) -> None:
        self.used = Resources(
            cpus=self.used.cpus + request.cpus,
            memory_mb=self.used.memory_mb + request.memory_mb,
            gpus=self.used.gpus + request.gpus,
        )

    def release(self, request: Resources) -> None:
        self.used = Resources(
            cpus=self.used.cpus - request.cpus,
            memory_mb=self.used.memory_mb - request.memory_mb,
            gpus=self.used.gpus - request.gpus,
        )


# ---------------------------------------------------------------- API schemas


class JobSubmitRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    cpus: int = Field(ge=1)
    memory_mb: int = Field(ge=1)
    gpus: int = Field(default=0, ge=0)
    priority: int = Field(default=0, ge=0)
    run_time_ms: int = Field(default=100, ge=0)
    fail: bool = False
    timeout_ms: int | None = Field(default=None, ge=1)
    max_attempts: int = Field(default=1, ge=1, le=10)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class JobView(BaseModel):
    id: str
    name: str
    state: JobState
    priority: int
    cpus: int
    memory_mb: int
    gpus: int
    attempts: int
    max_attempts: int
    node_id: str | None
    created_at: float
    updated_at: float

    @classmethod
    def from_job(cls, job: Job) -> "JobView":
        return cls(
            id=job.id,
            name=job.name,
            state=job.state,
            priority=job.priority,
            cpus=job.request.cpus,
            memory_mb=job.request.memory_mb,
            gpus=job.request.gpus,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            node_id=job.node_id,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class NodeView(BaseModel):
    id: str
    state: NodeState
    capacity: Resources
    used: Resources
    available: Resources
    running_jobs: list[str]


class RetryResponse(BaseModel):
    job_id: str
    state: JobState
    attempts: int


class SubmitJobResponse(BaseModel):
    job: JobView
    replayed: bool = False


class NodeStateRequest(BaseModel):
    drained: bool
