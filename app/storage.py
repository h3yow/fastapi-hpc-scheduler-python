"""Persistence: file-backed JSON store with atomic writes.

Only local files are used (no external services). Writes go through a
temporary file + ``os.replace`` so a crash can never leave a truncated
state file behind.
"""
from __future__ import annotations

import json
import os
import tempfile


class Store:
    """Durable store for a single JSON document."""

    def __init__(self, path: str = "scheduler_state.json") -> None:
        self.path = path

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def load(self) -> dict | None:
        if not self.exists():
            return None
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def save(self, state: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".scheduler_state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, separators=(",", ":"))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
