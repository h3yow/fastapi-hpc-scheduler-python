"""Idempotency-key helpers.

The same key always maps to the same job; replaying it returns the
original job instead of creating a second one. Reusing a key with a
*different* request payload is rejected with ``idempotency_conflict``.
"""
from __future__ import annotations

import hashlib
import json

from app.models import JobSubmitRequest

_FIELDS = (
    "name", "cpus", "memory_mb", "gpus", "priority", "run_time_ms",
    "fail", "timeout_ms", "max_attempts",
)


def fingerprint(request: JobSubmitRequest) -> str:
    payload = {field: getattr(request, field) for field in _FIELDS}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
