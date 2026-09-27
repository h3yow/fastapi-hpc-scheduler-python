"""测试辅助：带日志的等待与构造。"""
import logging
import time

from app.models import JobStatus, JobSubmitRequest

log = logging.getLogger("test")


def make_req(key: str, **kw) -> JobSubmitRequest:
    defaults = dict(name=f"job-{key}", priority=0, cpu=1, memory_mb=256,
                    duration_seconds=0.02)
    defaults.update(kw)
    req = JobSubmitRequest(idempotency_key=key, **defaults)
    log.info("submit spec: key=%s prio=%s cpu=%s mem=%s fail=%s",
             key, req.priority, req.cpu, req.memory_mb, req.should_fail)
    return req


def wait_status(service, job_id: str, statuses, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = service.get(job_id)
        if job.status in statuses:
            log.info("job %s reached status %s", job_id, job.status.value)
            return job
        time.sleep(0.01)
    raise AssertionError(
        f"job {job_id} 未在 {timeout}s 内到达 {statuses}，"
        f"当前 {service.get(job_id).status}")
