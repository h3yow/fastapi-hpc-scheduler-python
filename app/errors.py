"""Stable error codes and domain exceptions.

Every rejected request gets the same envelope::

    {"error": {"code": "<stable_code>", "message": "<human detail>"}}

The ``code`` values are part of the API contract and never change; the
message text may.
"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ErrorCode(str, Enum):
    INTERNAL_ERROR = "internal_error"
    VALIDATION_ERROR = "validation_error"
    JOB_NOT_FOUND = "job_not_found"
    NODE_NOT_FOUND = "node_not_found"
    INVALID_STATE_TRANSITION = "invalid_state_transition"
    INVALID_RESOURCES = "invalid_resources"
    JOB_ALREADY_CANCELLED = "job_already_cancelled"
    JOB_NOT_CANCELLABLE = "job_not_cancellable"
    JOB_NOT_RETRYABLE = "job_not_retryable"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    NODE_UNAVAILABLE = "node_unavailable"
    INSUFFICIENT_RESOURCES = "insufficient_resources"
    JOB_STATE_CHANGED = "job_state_changed"


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class SchedulerError(Exception):
    def __init__(self, code: ErrorCode, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
