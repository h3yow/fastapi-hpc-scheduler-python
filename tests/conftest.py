import logging

import pytest

from app.bootstrap import build_service
from app.models import Node, Resources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s")

# 小集群：便于构造资源不足与 best-fit 场景
TEST_NODES = [
    Node("n1", Resources(cpu=4, memory_mb=4096)),
    Node("n2", Resources(cpu=4, memory_mb=4096)),
]


@pytest.fixture()
def service():
    svc = build_service(":memory:", default_nodes=TEST_NODES)
    yield svc
    svc.shutdown()
