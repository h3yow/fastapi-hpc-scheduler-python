"""持久化层：SQLite 存储作业与节点，重启后状态可恢复。

只负责读写，不含调度与状态机逻辑。所有方法由 Service 在锁内调用，
连接使用 check_same_thread=False 以支持 FastAPI 线程池。
"""
from __future__ import annotations

import sqlite3

from app.models import Job, JobStatus, Node, Resources

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL,
    cpu INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    status TEXT NOT NULL,
    node_id TEXT,
    duration_seconds REAL NOT NULL,
    should_fail INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    cpu INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    healthy INTEGER NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ---- jobs ----

    def save_job(self, job: Job) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO jobs
               (job_id, idempotency_key, name, priority, cpu, memory_mb, status,
                node_id, duration_seconds, should_fail, attempt,
                created_at, updated_at, error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (job.job_id, job.idempotency_key, job.name, job.priority,
             job.resources.cpu, job.resources.memory_mb, job.status.value,
             job.node_id, job.duration_seconds, int(job.should_fail),
             job.attempt, job.created_at, job.updated_at, job.error),
        )
        self._conn.commit()

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            idempotency_key=row["idempotency_key"],
            name=row["name"],
            priority=row["priority"],
            resources=Resources(row["cpu"], row["memory_mb"]),
            status=JobStatus(row["status"]),
            node_id=row["node_id"],
            duration_seconds=row["duration_seconds"],
            should_fail=bool(row["should_fail"]),
            attempt=row["attempt"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error=row["error"],
        )

    def get_job(self, job_id: str) -> Job | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def get_job_by_key(self, idempotency_key: str) -> Job | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?",
            (idempotency_key,)).fetchone()
        return self._row_to_job(row) if row else None

    def list_jobs(self) -> list[Job]:
        rows = self._conn.execute(
            "SELECT * FROM jobs ORDER BY created_at, job_id").fetchall()
        return [self._row_to_job(r) for r in rows]

    # ---- nodes ----

    def save_node(self, node: Node) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO nodes (node_id, cpu, memory_mb, healthy)
               VALUES (?,?,?,?)""",
            (node.node_id, node.total.cpu, node.total.memory_mb,
             int(node.healthy)),
        )
        self._conn.commit()

    def list_nodes(self) -> list[Node]:
        rows = self._conn.execute(
            "SELECT * FROM nodes ORDER BY node_id").fetchall()
        return [
            Node(node_id=r["node_id"],
                 total=Resources(r["cpu"], r["memory_mb"]),
                 healthy=bool(r["healthy"]))
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
