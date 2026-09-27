"""调度核心单元测试：优先级、到达顺序、资源账本。"""
import logging

from app.models import Job, Resources
from app.scheduler import Scheduler, ScheduleOutcome

log = logging.getLogger("test")


def make_nodes(svc_nodes):
    return svc_nodes


def test_best_fit_and_release(service):
    sch = service._scheduler
    j = Job("j1", "k1", "t", 0, Resources(4, 4096))
    d = sch.try_schedule(j)
    log.info("decision: %s -> %s", d.outcome.value, d.node_id)
    assert d.outcome == ScheduleOutcome.SCHEDULED
    j.node_id = d.node_id
    assert sch.available(d.node_id) == Resources(0, 0)
    sch.release(j)
    assert sch.available(d.node_id) == Resources(4, 4096)


def test_priority_then_fifo_order(service):
    # 占满全部节点，再提交三个排队作业，释放后按 优先级>到达顺序 调度
    big1 = service.submit(__req("big1", cpu=4, memory_mb=4096, duration_seconds=5.0))
    big2 = service.submit(__req("big2", cpu=4, memory_mb=4096, duration_seconds=5.0))
    low = service.submit(__req("low", priority=1))
    high = service.submit(__req("high", priority=9))
    mid = service.submit(__req("mid", priority=5))
    log.info("queued order expectation: high > mid > low")
    service.cancel(big1.job_id)  # 释放 n1，触发 drain
    assert service.get(high.job_id).status.value == "running"
    service.cancel(big2.job_id)
    assert service.get(mid.job_id).status.value == "running"
    service.cancel(high.job_id)
    assert service.get(low.job_id).status.value == "running"


def __req(key, **kw):
    from app.models import JobSubmitRequest
    d = dict(name=key, cpu=1, memory_mb=256, duration_seconds=5.0)
    d.update(kw)
    r = JobSubmitRequest(idempotency_key=key, **d)
    log.info("submit key=%s prio=%s cpu=%s", key, r.priority, r.cpu)
    return r
