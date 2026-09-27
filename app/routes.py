"""HTTP routes.

Thin layer only: request parsing, delegating to :class:`SchedulerService`,
and shaping responses. Scheduling/state rules live elsewhere.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request, status

from app.models import (
    JobSubmitRequest,
    JobView,
    NodeStateRequest,
    NodeView,
    SubmitJobResponse,
)

logger = logging.getLogger("http")

router = APIRouter(prefix="/api/v1", tags=["scheduler"])


def service(request: Request):
    return request.app.state.service


@router.post("/jobs", response_model=SubmitJobResponse, status_code=status.HTTP_201_CREATED)
def submit_job(payload: JobSubmitRequest, request: Request):
    logger.info("http POST /jobs name=%s priority=%s key=%s",
                payload.name, payload.priority, payload.idempotency_key)
    svc = service(request)
    job, created = svc.submit_job(payload)
    return SubmitJobResponse(job=JobView.from_job(job), replayed=not created)


@router.get("/jobs", response_model=list[JobView])
def list_jobs(request: Request):
    return [JobView.from_job(j) for j in service(request).list_jobs()]


@router.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: str, request: Request):
    return JobView.from_job(service(request).get_job(job_id))


@router.post("/jobs/{job_id}/cancel", response_model=JobView)
def cancel_job(job_id: str, request: Request):
    logger.info("http POST /jobs/%s/cancel", job_id)
    return JobView.from_job(service(request).cancel_job(job_id))


@router.post("/jobs/{job_id}/retry", response_model=JobView)
def retry_job(job_id: str, request: Request):
    logger.info("http POST /jobs/%s/retry", job_id)
    return JobView.from_job(service(request).retry_job(job_id))


@router.get("/nodes", response_model=list[NodeView])
def list_nodes(request: Request):
    return service(request).list_nodes()


@router.post("/nodes/{node_id}/state", response_model=NodeView)
def set_node_state(node_id: str, payload: NodeStateRequest, request: Request):
    svc = service(request)
    svc.set_node_state(node_id, payload.drained)
    return next(n for n in svc.list_nodes() if n.id == node_id)
