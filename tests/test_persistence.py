"""重启恢复：已接受作业与节点占用不丢，账本重建后不超分配。"""
import logging
import time

from app.bootstrap import build_service
from app.models import JobStatus, Resources
from tests.helpers import make_req, wait_status

log = logging.getLogger("test")

NODES = None  # 使用默认三节点


def test_restart_recovers_jobs_and_allocations(tmp_path):
    db = str(tmp_path / "sched.db")
    svc1 = build_service(db)
    running = svc1.submit(make_req("persist-run", cpu=4, memory_mb=4096,
                                   duration_seconds=30.0))
    done = svc1.submit(make_req("persist-done", duration_seconds=0.01))
    wait_status(svc1, done.job_id, {JobStatus.SUCCEEDED})
    node = running.node_id
    log.info("before restart: running=%s node=%s done=%s",
             running.job_id, node, done.job_id)
    svc1.shutdown()  # 模拟服务停止（执行线程为 daemon，随进程退出）

    svc2 = build_service(db)  # 重启
    j = svc2.get(running.job_id)
    assert j.status == JobStatus.RUNNING, "重启后运行中作业不丢"
    assert svc2.get(done.job_id).status == JobStatus.SUCCEEDED
    # 账本已重建：被占用节点可用量减少了 4 cpu
    avail = svc2.node_availability(node)
    log.info("after restart: node %s avail=%s", node, avail)
    assert avail.cpu <= 4, "重启后占用必须体现在账本里"
    # 幂等键仍生效：重启后重复提交不生成新作业
    again = svc2.submit(make_req("persist-run", cpu=4, memory_mb=4096,
                                 duration_seconds=30.0))
    assert again.job_id == running.job_id
    svc2.shutdown()
