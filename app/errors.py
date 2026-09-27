"""稳定错误分类：所有拒绝原因都有唯一 code，HTTP 层按 code 映射状态码。"""
from __future__ import annotations

import enum


class ErrorCode(str, enum.Enum):
    JOB_NOT_FOUND = "job_not_found"
    INVALID_STATE_TRANSITION = "invalid_state_transition"
    ALREADY_CANCELLED = "already_cancelled"
    INVALID_RESOURCE_REQUEST = "invalid_resource_request"
    NO_RESOURCES = "no_resources"              # 资源不足，作业排队
    NODES_UNAVAILABLE = "nodes_unavailable"    # 无可用健康节点
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"  # 同键不同参数
    INTERNAL = "internal"


class SchedulerError(Exception):
    """领域错误：code 稳定，message 面向调用方，不透传底层异常。"""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
