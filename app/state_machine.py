"""Allowed job state transitions.

Keeping the table in one place means HTTP, scheduler and executor layers
all agree on what "reasonable" means, and illegal transitions get a
single stable error code (``invalid_state_transition``).

    pending   -> running | cancelled
    running   -> succeeded | failed | cancelled
    failed    -> pending   (retry only)
    succeeded / cancelled are terminal.
"""
from __future__ import annotations

from app.errors import ErrorCode, SchedulerError
from app.models import JobState

ALLOWED: dict[JobState, frozenset[JobState]] = {
    JobState.PENDING: frozenset({JobState.RUNNING, JobState.CANCELLED}),
    JobState.RUNNING: frozenset(
        {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}
    ),
    JobState.FAILED: frozenset({JobState.PENDING}),
    JobState.SUCCEEDED: frozenset(),
    JobState.CANCELLED: frozenset(),
}


def ensure_transition(src: JobState, dst: JobState) -> None:
    if dst not in ALLOWED.get(src, frozenset()):
        raise SchedulerError(
            ErrorCode.INVALID_STATE_TRANSITION,
            f"job cannot transition from {src.value} to {dst.value}",
            status_code=409,
        )


def can_transition(src: JobState, dst: JobState) -> bool:
    return dst in ALLOWED.get(src, frozenset())
