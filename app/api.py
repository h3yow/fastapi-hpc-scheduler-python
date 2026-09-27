"""HTTP 层：只做参数解析与错误码 -> HTTP 状态映射，不含调度逻辑。"""
from __future__ import annotations

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.errors import ErrorCode, SchedulerError
from app.models import Job, JobSubmitRequest, JobView, NodeView
from app.service import JobService

_STATUS_BY_CODE = {
    ErrorCode.JOB_NOT_FOUND: 404,
    ErrorCode.INVALID_STATE_TRANSITION: 409,
    ErrorCode.ALREADY_CANCELLED: 409,
    ErrorCode.INVALID_RESOURCE_REQUEST: 422,
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.NO_RESOURCES: 200,   # 排队是正常结果，不是错误
    ErrorCode.NODES_UNAVAILABLE: 200,
    ErrorCode.INTERNAL: 500,
}


class ErrorBody(BaseModel):
    code: str
    message: str


def _to_view(job: Job) -> JobView:
    return JobView(
        job_id=job.job_id, idempotency_key=job.idempotency_key,
        name=job.name, priority=job.priority,
        cpu=job.resources.cpu, memory_mb=job.resources.memory_mb,
        status=job.status, node_id=job.node_id, attempt=job.attempt,
        created_at=job.created_at, updated_at=job.updated_at,
        error=job.error)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(SchedulerError)
    async def on_scheduler_error(_: Request, exc: SchedulerError):
        status = _STATUS_BY_CODE.get(exc.code, 500)
        return JSONResponse(
            status_code=status,
            content=ErrorBody(code=exc.code.value, message=exc.message)
            .model_dump())

    @app.exception_handler(Exception)
    async def on_unexpected(_: Request, exc: Exception):
        # 底层异常不透传，统一归为 internal
        return JSONResponse(
            status_code=500,
            content=ErrorBody(code=ErrorCode.INTERNAL.value,
                              message="内部错误").model_dump())


def create_router(service: JobService) -> APIRouter:
    router = APIRouter()

    @router.post("/jobs", response_model=JobView, status_code=201)
    def submit_job(req: JobSubmitRequest):
        return _to_view(service.submit(req))

    @router.get("/jobs", response_model=list[JobView])
    def list_jobs():
        return [_to_view(j) for j in service.list_jobs()]

    @router.get("/jobs/{job_id}", response_model=JobView)
    def get_job(job_id: str):
        return _to_view(service.get(job_id))

    @router.post("/jobs/{job_id}/cancel", response_model=JobView)
    def cancel_job(job_id: str):
        return _to_view(service.cancel(job_id))

    @router.post("/jobs/{job_id}/retry", response_model=JobView)
    def retry_job(job_id: str):
        return _to_view(service.retry(job_id))

    @router.get("/nodes", response_model=list[NodeView])
    def list_nodes():
        views = []
        for n in service.list_nodes():
            avail = service.node_availability(n.node_id)
            views.append(NodeView(
                node_id=n.node_id, healthy=n.healthy,
                total_cpu=n.total.cpu, total_memory_mb=n.total.memory_mb,
                avail_cpu=avail.cpu, avail_memory_mb=avail.memory_mb))
        return views

    @router.post("/nodes/{node_id}/health", response_model=list[NodeView])
    def set_health(node_id: str, healthy: bool = True):
        service.set_node_health(node_id, healthy)
        return list_nodes()

    return router
