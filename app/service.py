"""应用服务：组合 Store 与 Scheduler，处理幂等、并发与状态流转。

并发模型：一把 RLock 保护所有状态变更与资源账本；
作业执行在后台线程模拟，完成回调也走同一把锁，
乱序/过期回调通过 attempt 令牌识别并丢弃。
"""
from __future__ import annotations

import logging
import threading
import time

from app.errors import ErrorCode, SchedulerError
from app.models import (Job, JobStatus, JobSubmitRequest, Node, Resources,
                        can_transition)
from app.scheduler import Scheduler, ScheduleOutcome
from app.store import Store

logger = logging.getLogger("hpc.service")


class JobService:
    def __init__(self, store: Store, scheduler: Scheduler) -> None:
        self._store = store
        self._scheduler = scheduler
        self._lock = threading.RLock()
        self._closed = False
        self._recover()

    # ---- 启动恢复：重启后重建资源账本并续跑中断的作业 ----

    def _recover(self) -> None:
        with self._lock:
            for job in self._store.list_jobs():
                if job.status == JobStatus.RUNNING and job.node_id:
                    # 重启前在跑的作业：账本补记占用，并重新启动执行线程
                    self._scheduler._allocations[job.node_id].add(job.job_id)
                    self._scheduler._avail[job.node_id] = (
                        self._scheduler._avail[job.node_id] - job.resources)
                    logger.info("recover running job %s on node %s",
                                job.job_id, job.node_id)
                    self._start_worker(job.job_id, job.attempt,
                                       job.duration_seconds, job.should_fail)
            self._drain_queue()

    # ---- 提交（幂等） ----

    def submit(self, req: JobSubmitRequest) -> Job:
        with self._lock:
            existing = self._store.get_job_by_key(req.idempotency_key)
            if existing is not None:
                if self._same_spec(existing, req):
                    logger.info("idempotent hit key=%s -> job %s",
                                req.idempotency_key, existing.job_id)
                    return existing
                raise SchedulerError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    f"idempotency key {req.idempotency_key!r} 已对应不同参数的作业")

            resources = Resources(req.cpu, req.memory_mb)
            self._validate_resources(resources)

            job = Job(job_id=Job.new_id(),
                      idempotency_key=req.idempotency_key,
                      name=req.name, priority=req.priority,
                      resources=resources,
                      duration_seconds=req.duration_seconds,
                      should_fail=req.should_fail)
            self._store.save_job(job)
            logger.info("submitted job %s key=%s prio=%d res=%s",
                        job.job_id, job.idempotency_key, job.priority,
                        resources)
            self._try_start(job)
            return job

    @staticmethod
    def _same_spec(job: Job, req: JobSubmitRequest) -> bool:
        return (job.name == req.name and job.priority == req.priority
                and job.resources.cpu == req.cpu
                and job.resources.memory_mb == req.memory_mb
                and job.duration_seconds == req.duration_seconds
                and job.should_fail == req.should_fail)

    def _validate_resources(self, resources: Resources) -> None:
        nodes = self._store.list_nodes()
        if not any(resources.fits_in(n.total) for n in nodes):
            raise SchedulerError(
                ErrorCode.INVALID_RESOURCE_REQUEST,
                f"请求资源 cpu={resources.cpu} mem={resources.memory_mb}MB "
                "超过所有节点总量，永远无法调度")

    # ---- 查询 ----

    def get(self, job_id: str) -> Job:
        with self._lock:
            job = self._store.get_job(job_id)
            if job is None:
                raise SchedulerError(ErrorCode.JOB_NOT_FOUND,
                                     f"作业 {job_id} 不存在")
            return job

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return self._store.list_jobs()

    def list_nodes(self) -> list[Node]:
        with self._lock:
            return self._store.list_nodes()

    def node_availability(self, node_id: str) -> Resources:
        with self._lock:
            return self._scheduler.available(node_id)

    # ---- 取消 ----

    def cancel(self, job_id: str) -> Job:
        with self._lock:
            job = self.get(job_id)
            if job.status == JobStatus.CANCELLED:
                raise SchedulerError(ErrorCode.ALREADY_CANCELLED,
                                     f"作业 {job_id} 已是取消状态，不可重复取消")
            if not can_transition(job.status, JobStatus.CANCELLED):
                raise SchedulerError(
                    ErrorCode.INVALID_STATE_TRANSITION,
                    f"作业 {job_id} 状态 {job.status.value} 不允许取消")
            self._release_if_allocated(job)
            self._transition(job, JobStatus.CANCELLED)
            logger.info("cancelled job %s", job.job_id)
            self._drain_queue()
            return job

    # ---- 失败重试 ----

    def retry(self, job_id: str) -> Job:
        with self._lock:
            job = self.get(job_id)
            if not can_transition(job.status, JobStatus.PENDING):
                raise SchedulerError(
                    ErrorCode.INVALID_STATE_TRANSITION,
                    f"只有 failed 状态可重试，当前 {job.status.value}")
            job.error = None
            self._transition(job, JobStatus.PENDING)
            logger.info("retry job %s attempt->%d", job.job_id, job.attempt)
            self._drain_queue()
            return job

    # ---- 节点健康 ----

    def set_node_health(self, node_id: str, healthy: bool) -> None:
        with self._lock:
            if node_id not in self._scheduler.node_ids():
                raise SchedulerError(ErrorCode.JOB_NOT_FOUND,
                                     f"节点 {node_id} 不存在")
            self._scheduler.set_node_health(node_id, healthy)
            for n in self._store.list_nodes():
                if n.node_id == node_id:
                    n.healthy = healthy
                    self._store.save_node(n)
            logger.info("node %s healthy=%s", node_id, healthy)
            if healthy:
                self._drain_queue()

    # ---- 内部：调度与执行 ----

    def _try_start(self, job: Job) -> None:
        """锁内调用：尝试把 PENDING 作业调度到节点并启动执行。"""
        if job.status != JobStatus.PENDING:
            return
        decision = self._scheduler.try_schedule(job)
        if decision.outcome == ScheduleOutcome.SCHEDULED:
            job.node_id = decision.node_id
            job.attempt += 1
            self._transition(job, JobStatus.RUNNING)
            logger.info("scheduled job %s -> node %s (attempt %d)",
                        job.job_id, job.node_id, job.attempt)
            self._start_worker(job.job_id, job.attempt,
                               job.duration_seconds, job.should_fail)
        else:
            logger.info("job %s queued: %s", job.job_id, decision.outcome.value)

    def _drain_queue(self) -> None:
        """锁内调用：按优先级、再按到达顺序扫描待调度作业。"""
        pending = [j for j in self._store.list_jobs()
                   if j.status == JobStatus.PENDING]
        pending.sort(key=lambda j: (-j.priority, j.created_at, j.job_id))
        for job in pending:
            self._try_start(job)

    def _start_worker(self, job_id: str, attempt: int,
                      duration: float, should_fail: bool) -> None:
        t = threading.Thread(
            target=self._run_job, args=(job_id, attempt, duration, should_fail),
            name=f"job-{job_id[:8]}-a{attempt}", daemon=True)
        t.start()

    def _run_job(self, job_id: str, attempt: int,
                 duration: float, should_fail: bool) -> None:
        """模拟执行。任何异常都不能透传出去，资源必须在锁内最终一致。"""
        try:
            time.sleep(duration)
            if should_fail:
                raise RuntimeError("模拟执行失败")
            outcome, error = JobStatus.SUCCEEDED, None
        except Exception as exc:  # noqa: BLE001 - 兜底，绝不透传
            outcome, error = JobStatus.FAILED, str(exc)
        try:
            with self._lock:
                if self._closed:
                    return
                job = self._store.get_job(job_id)
                if job is None or job.status != JobStatus.RUNNING \
                        or job.attempt != attempt:
                    # 乱序/过期回调：作业已被取消或重试，直接丢弃
                    logger.warning("stale worker callback job=%s attempt=%d "
                                   "ignored", job_id, attempt)
                    return
                job.error = error
                self._release_if_allocated(job)
                self._transition(job, outcome)
                logger.info("job %s finished: %s", job_id, outcome.value)
                self._drain_queue()
        except Exception:  # noqa: BLE001
            logger.exception("worker callback failed for job %s", job_id)

    def _release_if_allocated(self, job: Job) -> None:
        self._scheduler.release(job)
        job.node_id = None

    def _transition(self, job: Job, dst: JobStatus) -> None:
        assert can_transition(job.status, dst), \
            f"非法流转 {job.status} -> {dst}"
        job.status = dst
        job.updated_at = time.time()
        self._store.save_job(job)

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
        self._store.close()
