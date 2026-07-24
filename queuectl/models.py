from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import json
import uuid
from typing import Optional, Dict, Any

def current_iso_utc() -> str:
    """Return current timestamp in ISO 8601 UTC format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class JobState:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"

    ALL = {PENDING, PROCESSING, COMPLETED, FAILED, DEAD}

@dataclass
class Job:
    command: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    state: str = JobState.PENDING
    attempts: int = 0
    max_retries: int = 3
    created_at: str = field(default_factory=current_iso_utc)
    updated_at: str = field(default_factory=current_iso_utc)
    priority: int = 0
    run_at: Optional[str] = None
    timeout: int = 60
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    worker_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert Job object to standard dictionary representation."""
        return {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "priority": self.priority,
            "run_at": self.run_at,
            "timeout": self.timeout,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "worker_id": self.worker_id,
        }

    def to_json(self) -> str:
        """Convert job to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Construct Job object from dictionary."""
        return cls(
            id=data.get("id") or str(uuid.uuid4())[:8],
            command=data["command"],
            state=data.get("state", JobState.PENDING),
            attempts=int(data.get("attempts", 0)),
            max_retries=int(data.get("max_retries", 3)),
            created_at=data.get("created_at") or current_iso_utc(),
            updated_at=data.get("updated_at") or current_iso_utc(),
            priority=int(data.get("priority", 0)),
            run_at=data.get("run_at"),
            timeout=int(data.get("timeout", 60)),
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            exit_code=data.get("exit_code"),
            worker_id=data.get("worker_id"),
        )
