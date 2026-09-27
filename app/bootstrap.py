"""组装：从数据库路径构建 JobService，并确保默认节点存在。"""
from __future__ import annotations

from app.models import Node, Resources
from app.scheduler import Scheduler
from app.service import JobService
from app.store import Store

# 本地模拟集群拓扑
DEFAULT_NODES = [
    Node("node-1", Resources(cpu=8, memory_mb=8192)),
    Node("node-2", Resources(cpu=8, memory_mb=8192)),
    Node("node-3", Resources(cpu=16, memory_mb=16384)),
]


def build_service(db_path: str,
                  default_nodes: list[Node] | None = None) -> JobService:
    store = Store(db_path)
    if not store.list_nodes():
        for n in (default_nodes or DEFAULT_NODES):
            store.save_node(n)
    nodes = store.list_nodes()
    return JobService(store, Scheduler(nodes))
