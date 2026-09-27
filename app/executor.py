"""Simulated job execution.

Execution is fully local (``asyncio.sleep`` for ``run_time_ms``) so no
external services or credentials are needed. The executor is the single
place where raw execution problems are caught: they are normalised into
*timeout / failure* outcomes, logged, and never propagated to callers.

Out-of-order completion is handled by re-checking the job state under
the state lock: if the job is no longer ``running`` (e.g. it was
cancelled concurrently), the late result is dropped and resources are
left to whichever path owns them.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from app.errors import ErrorCode
from app.models import Job, JobState
from app.scheduler import Scheduler
from app.state import SchedulerState
from app.state_machine import can_transition

logger = logging.getLogger("executor")

FinishCallback = Callable[[], Awaitable[None]]


class Executor:
    def __init__(
        self,
        state: SchedulerState,
        scheduler: Scheduler,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.state = state
        self.scheduler = scheduler
        self.sleep = sleep

    async def launch(
        self,
        job_id: str,
        on_finish: FinishCallback | None = None,
    ) -> None:
        """Run a placed job to completion (or failure/timeout). Never raises."""
        try:
            job = self._require_running(job_id)
            if job is None:
                return
            logger.info(
                "execute start job=%s node=%s run_time_ms=%s timeout_ms=%s fail=%s",
                job_id, job.node_id, job.run_time_ms, job.timeout_ms, job.fail,
            )

            timed_out = job.timeout_ms is not None and job.run_time_ms > job.timeout_ms
            await self.sleep(job.run_time_ms / 1000.0)

            # Re-read under the lock: the job may have been cancelled meanwhile.
            with self.state.lock:
                current = self.state.jobs.get(job_id)
                if current is None or current.state is not JobState.RUNNING:
                    logger.warning(
                        "execute stale-result dropped job=%s state=%s",
                        job_id, current.state.value if current else "missing",
                    )
                    return
                if timed_out:
                    outcome = JobState.FAILED
                    reason = "timeout"
                elif job.fail:
                    outcome = JobState.FAILED
                    reason = "simulated_failure"
                else:
                    outcome = JobState.SUCCEEDED
                    reason = "completed"
                if not can_transition(current.state, outcome):
                    logger.warning(
                        "execute illegal-transition ignored job=%s %s->%s",
                        job_id, current.state.value, outcome.value,
                    )
                    return
                current.state = outcome
                current.touch()

            self.scheduler.release(job_id)
            logger.info(
                "execute finish job=%s outcome=%s reason=%s",
                job_id, outcome.value, reason,
            )
        except asyncio.CancelledError:
            # Cancellation of the server task must not be swallowed.
            logger.info("execute task cancelled job=%s", job_id)
            raise
        except Exception as exc:  # noqa: BLE001 - deliberate boundary
            logger.error(
                "execute error job=%s normalized_to=failed code=%s detail=%r",
                job_id, ErrorCode.INTERNAL_ERROR.value, exc,
            )
            claimed = False
            with self.state.lock:
                current = self.state.jobs.get(job_id)
                if current is not None and current.state is JobState.RUNNING:
                    current.state = JobState.FAILED
                    current.touch()
                    claimed = True
            # Only the path that owns the transition may release resources;
            # a concurrent cancel/finish already did so otherwise.
            if claimed:
                self.scheduler.release(job_id)
        finally:
            if on_finish is not None:
                try:
                    await on_finish()
                except Exception:  # noqa: BLE001
                    logger.exception("finish callback failed job=%s", job_id)

    def _require_running(self, job_id: str) -> Job | None:
        with self.state.lock:
            job = self.state.jobs.get(job_id)
            if job is None:
                logger.warning("execute missing job=%s", job_id)
                return None
            if job.state is not JobState.RUNNING:
                logger.warning(
                    "execute skip job=%s state=%s (expected running)",
                    job_id, job.state.value,
                )
                return None
            return job.model_copy()
