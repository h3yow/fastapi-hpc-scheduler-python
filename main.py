import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import create_router, register_exception_handlers
from app.bootstrap import build_service

DB_PATH = os.environ.get("SCHEDULER_DB", "scheduler.db")


def create_app(db_path: str = DB_PATH) -> FastAPI:
    service = build_service(db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        service.shutdown()

    app = FastAPI(title="HPC Job Scheduler", lifespan=lifespan)
    app.state.service = service
    register_exception_handlers(app)
    app.include_router(create_router(service))

    @app.get("/")
    def read_root():
        return {"service": "hpc-job-scheduler", "docs": "/docs"}

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
