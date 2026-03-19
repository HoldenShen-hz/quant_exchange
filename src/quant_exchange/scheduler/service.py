"""Simple interval scheduler used for sync and reporting jobs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


@dataclass(slots=True)
class ScheduledJob:
    """In-memory representation of a recurring job."""

    job_code: str
    job_name: str
    job_type: str
    interval_seconds: int
    callback: Callable[[dict[str, Any]], dict[str, Any] | None]
    payload: dict[str, Any] = field(default_factory=dict)
    status: str = "ACTIVE"
    last_run_at: datetime | None = None

    def is_due(self, now: datetime) -> bool:
        """Return whether the job should run at the supplied timestamp."""

        if self.status != "ACTIVE":
            return False
        if self.last_run_at is None:
            return True
        return now - self.last_run_at >= timedelta(seconds=self.interval_seconds)


class JobScheduler:
    """Execute due jobs and optionally persist job definitions and run history."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.jobs: dict[str, ScheduledJob] = {}
        self.run_history: list[dict[str, Any]] = []

    def register_job(self, job: ScheduledJob) -> None:
        """Register a job and persist its definition when a database is available."""

        self.jobs[job.job_code] = job
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ops_scheduled_jobs",
                "job_code",
                job.job_code,
                {
                    "job_code": job.job_code,
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "interval_seconds": job.interval_seconds,
                    "payload": job.payload,
                    "status": job.status,
                },
                extra_columns={
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "status": job.status,
                },
            )

    def run_due_jobs(self, now: datetime | None = None) -> list[dict[str, Any]]:
        """Run all jobs that are due and return their execution summaries."""

        now = now or datetime.now(timezone.utc)
        summaries: list[dict[str, Any]] = []
        for job in self.jobs.values():
            if not job.is_due(now):
                continue
            result = job.callback(job.payload) or {}
            job.last_run_at = now
            summary = {
                "run_no": f"{job.job_code}:{now.isoformat()}",
                "job_code": job.job_code,
                "status": "SUCCESS",
                "result_summary": result,
            }
            summaries.append(summary)
            self.run_history.append(summary)
            if self.persistence is not None:
                self.persistence.upsert_record(
                    "ops_job_runs",
                    "run_no",
                    summary["run_no"],
                    summary,
                    extra_columns={"job_code": job.job_code, "status": "SUCCESS"},
                )
        return summaries
