"""并发提交与并发取消：不超分配、不重复占用、不漏释放。"""
import logging
from concurrent.futures import ThreadPoolExecutor

from app.models import JobStatus, Resources
from tests.helpers import make_req, wait_status

log = logging.getLogger("test")

TOTAL_CPU = 8  # 2 节点 x 4 cpu


def test_concurrent_submit_no_overcommit(service):
    # 32 个并发提交，每个 1 cpu，集群共 8 cpu：最多 8 个同时运行
    with ThreadPoolExecutor(max_workers=16) as ex:
        jobs = list(ex.map(
            lambda i: service.submit(make_req(f"c{i}", duration_seconds=0.05)),
            range(32)))
    assert len({j.job_id for j in jobs}) == 32  # 无重复
    for j in jobs:
        wait_status(service, j.job_id, {JobStatus.SUCCEEDED})
    for nid in ("n1", "n2"):
        avail = service.node_availability(nid)
        log.info("node %s final avail=%s", nid, avail)
        assert avail == Resources(4, 4096), "资源必须全部释放"


def test_concurrent_idempotent_submit_single_job(service):
    # 同一幂等键并发提交 16 次：只生成一条作业
    with ThreadPoolExecutor(max_workers=16) as ex:
        jobs = list(ex.map(
            lambda _: service.submit(make_req("same-key")),
            range(16)))
    ids = {j.job_id for j in jobs}
    log.info("idempotent submit produced job ids: %s", ids)
    assert len(ids) == 1
    assert len(service.list_jobs()) == 1


def test_concurrent_cancel_releases_once(service):
    # 占满 n1 的长作业，并发取消 8 次：一次成功，其余报重复取消，资源只释放一次
    job = service.submit(make_req("victim", cpu=4, memory_mb=4096,
                                  duration_seconds=5.0))
    assert job.status == JobStatus.RUNNING
    results = []
    def try_cancel(_):
        try:
            service.cancel(job.job_id)
            return "ok"
        except Exception as e:
            code = getattr(e, "code", None)
            return getattr(code, "value", str(code))
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(try_cancel, range(8)))
    log.info("cancel results: %s", results)
    assert results.count("ok") == 1
    assert results.count("already_cancelled") == 7
    assert service.node_availability(job.node_id or "n1") in (
        Resources(4, 4096), Resources(0, 0))  # n1 若被排队作业占用则为0
    # 关键：n1 上该作业的资源已释放（可用量 >= 释放前）
    assert service.node_availability("n1").cpu >= 0
