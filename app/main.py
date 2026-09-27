"""FastAPI application factory and lifespan wiring."""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.errors import ErrorCode, ErrorResponse, SchedulerError
from app.routes import router
from app.service import SchedulerService

logger = logging.getLogger("app")


def _error_body(code: str, message: str) -> dict:
    return ErrorResponse(error={"code": code, "message": message}).model_dump()


def create_app(state_path: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        path = state_path or os.environ.get(
            "HPC_STATE_PATH", "scheduler_state.json"
        )
        svc = SchedulerService(path)
        app.state.service = svc
        await svc.start()
        try:
            yield
        finally:
            await svc.stop()

    app = FastAPI(
        title="HPC Job Scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(SchedulerError)
    async def on_scheduler_error(request: Request, exc: SchedulerError):
        logger.warning("rejected %s code=%s detail=%s",
                       request.url.path, exc.code.value, exc.message)
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(exc.code.value, exc.message),
        )

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request: Request, exc: RequestValidationError):
        # Keep body-shape rejections in the same stable envelope.
        return JSONResponse(
            status_code=422,
            content=_error_body(
                ErrorCode.VALIDATION_ERROR.value,
                "request validation failed: " + str(exc.errors()),
            ),
        )

    @app.exception_handler(Exception)
    async def on_unexpected_error(request: Request, exc: Exception):
        # Never leak internals (stack traces, driver errors, paths) to callers.
        logger.exception("unhandled error on %s", request.url.path)
        return JSONResponse(
            status_code=500,
            content=_error_body(
                ErrorCode.INTERNAL_ERROR.value,
                "internal server error",
            ),
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
