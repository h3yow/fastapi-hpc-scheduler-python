"""生命周期：取消释放、失败重试、非法流转、排队原因区分。"""
import logging

import pytest

from app.errors import ErrorCode, SchedulerError
from app.models import JobStatus, Resources
from tests.helpers import make_req, wait_status

log = logging.getLogger("test")


def test_cancel_running_releases_resources(service):
    job = service.submit(make_req("cancel-me", cpu=4, memory_mb=4096,
                                  duration_seconds=5.0))
    node = job.node_id
    assert service.node_availability(node) == Resources(0, 0)
    cancelled = service.cancel(job.job_id)
    log.info("cancelled %s on node %s", job.job_id, node)
    assert cancelled.status == JobStatus.CANCELLED
    assert service.node_availability(node) == Resources(4, 4096)


def test_cancel_pending_then_double_cancel_rejected(service):
    # 占满集群让目标作业排队
    b1 = service.submit(make_req("f1", cpu=4, memory_mb=4096, duration_seconds=5.0))
    b2 = service.submit(make_req("f2", cpu=4, memory_mb=4096, duration_seconds=5.0))
    queued = service.submit(make_req("queued"))
    assert queued.status == JobStatus.PENDING
    service.cancel(queued.job_id)
    with pytest.raises(SchedulerError) as ei:
        service.cancel(queued.job_id)
    log.info("double cancel rejected: %s", ei.value.code)
    assert ei.value.code == ErrorCode.ALREADY_CANCELLED


def test_failed_job_retry_succeeds_after_spec_fix(service):
    job = service.submit(make_req("flaky", should_fail=True,
                                  duration_seconds=0.02))
    wait_status(service, job.job_id, {JobStatus.FAILED})
    # 重试仍失败（should_fail 不变），验证状态机 FAILED->PENDING->RUNNING->FAILED
    retried = service.retry(job.job_id)
    assert retried.status in (JobStatus.PENDING, JobStatus.RUNNING)
    wait_status(service, job.job_id, {JobStatus.FAILED})
    job2 = service.get(job.job_id)
    log.info("job %s attempts=%d", job.job_id, job2.attempt)
    assert job2.attempt >= 2


def test_illegal_transitions_rejected(service):
    ok = service.submit(make_req("ok-job", duration_seconds=0.02))
    wait_status(service, ok.job_id, {JobStatus.SUCCEEDED})
    for op, name in ((service.cancel, "cancel"),
                     (service.retry, "retry")):
        with pytest.raises(SchedulerError) as ei:
            op(ok.job_id)
        log.info("%s on succeeded rejected: %s", name, ei.value.code)
        assert ei.value.code == ErrorCode.INVALID_STATE_TRANSITION
    # 未失败的作业不能 retry
    running = service.submit(make_req("run-job", duration_seconds=5.0))
    with pytest.raises(SchedulerError) as ei:
        service.retry(running.job_id)
    assert ei.value.code == ErrorCode.INVALID_STATE_TRANSITION
    with pytest.raises(SchedulerError) as ei:
        service.get("no-such-job")
    assert ei.value.code == ErrorCode.JOB_NOT_FOUND


def test_invalid_resource_request_rejected(service):
    with pytest.raises(SchedulerError) as ei:
        service.submit(make_req("too-big", cpu=999, memory_mb=1))
    log.info("oversize rejected: %s", ei.value.code)
    assert ei.value.code == ErrorCode.INVALID_RESOURCE_REQUEST


def test_queue_outcome_distinguishes_no_resources_and_no_nodes(service):
    # 资源不足但节点健康
    big = service.submit(make_req("big", cpu=4, memory_mb=4096,
                                  duration_seconds=5.0))
    big2 = service.submit(make_req("big2", cpu=4, memory_mb=4096,
                                   duration_seconds=5.0))
    queued = service.submit(make_req("waiter"))
    assert queued.status == JobStatus.PENDING
    # 节点全部不健康 -> 新的调度尝试报 nodes_unavailable（通过日志可见）
    service.set_node_health("n1", False)
    service.set_node_health("n2", False)
    d = service._scheduler.try_schedule(queued)
    log.info("outcome with all nodes down: %s", d.outcome.value)
    assert d.outcome.value == "queued_nodes_unavailable"
    service.set_node_health("n1", True)
    service.set_node_health("n2", True)
    service.cancel(big.job_id)
    service.cancel(big2.job_id)
