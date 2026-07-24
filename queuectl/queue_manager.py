import json
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from queuectl.models import Job, JobState, current_iso_utc
from queuectl.db import Database
from queuectl.config import ConfigManager

class QueueManager:
    def __init__(self, db: Optional[Database] = None, config: Optional[ConfigManager] = None):
        self.config = config or ConfigManager()
        self.db = db or Database(self.config.db_path)

    def enqueue(self, input_data: Union[str, Dict[str, Any]]) -> Job:
        """
        Enqueue a new job. `input_data` can be:
        - A JSON string e.g. '{"id":"job1", "command":"echo hi", "max_retries": 3}'
        - A plain command string e.g. "echo hello"
        - A Python dict with job fields
        """
        job_data = {}
        if isinstance(input_data, str):
            try:
                parsed = json.loads(input_data)
                if isinstance(parsed, dict):
                    job_data = parsed
                else:
                    job_data = {"command": str(parsed)}
            except (json.JSONDecodeError, TypeError):
                job_data = {"command": input_data}
        elif isinstance(input_data, dict):
            job_data = dict(input_data)

        if "command" not in job_data or not job_data["command"]:
            raise ValueError("Job command is required.")

        default_max_retries = self.config.get_int("max_retries", 3)
        default_timeout = self.config.get_int("default_job_timeout", 60)

        job_id = job_data.get("id") or str(uuid.uuid4())[:8]
        max_retries = int(job_data.get("max_retries", default_max_retries))
        priority = int(job_data.get("priority", 0))
        run_at = job_data.get("run_at")
        timeout = int(job_data.get("timeout", default_timeout))

        job = Job(
            id=job_id,
            command=job_data["command"],
            state=JobState.PENDING,
            attempts=0,
            max_retries=max_retries,
            created_at=current_iso_utc(),
            updated_at=current_iso_utc(),
            priority=priority,
            run_at=run_at,
            timeout=timeout,
        )

        return self.db.insert_job(job, available_at=run_at)

    def get_job(self, job_id: str) -> Optional[Job]:
        """Fetch a specific job by ID."""
        return self.db.get_job(job_id)

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        """List jobs filtered by state."""
        return self.db.list_jobs(state)

    def status(self) -> Dict[str, Any]:
        """Get summary status of queue states and active workers."""
        counts = self.db.count_jobs_by_state()
        workers = self.db.get_active_workers()
        return {
            "counts": counts,
            "active_workers": len(workers),
            "workers": workers,
            "total_jobs": sum(counts.values()),
        }

    def dlq_list(self) -> List[Job]:
        """List all dead-lettered (state='dead') jobs."""
        return self.db.list_jobs(state=JobState.DEAD)

    def dlq_retry(self, job_id: str) -> Job:
        """Re-queue a dead-lettered job back to pending with 0 attempts."""
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Job with ID '{job_id}' not found.")
        if job.state != JobState.DEAD:
            raise ValueError(f"Job '{job_id}' is in state '{job.state}', not 'dead'. Only DLQ jobs can be retried.")

        now = current_iso_utc()
        self.db.update_job_status(
            job_id=job_id,
            state=JobState.PENDING,
            attempts=0,
            available_at=now,
            worker_id=None
        )
        updated_job = self.db.get_job(job_id)
        return updated_job
