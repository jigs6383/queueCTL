import sqlite3
import os
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Generator
from queuectl.models import Job, JobState, current_iso_utc
from queuectl.config import get_db_path

class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for high concurrency
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self) -> None:
        """Create necessary database tables if they do not exist."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                run_at TEXT,
                available_at TEXT,
                timeout INTEGER NOT NULL DEFAULT 60,
                stdout TEXT DEFAULT '',
                stderr TEXT DEFAULT '',
                exit_code INTEGER,
                worker_id TEXT
            )
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_state_available 
            ON jobs(state, available_at, priority DESC, created_at ASC)
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                state TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                current_job_id TEXT
            )
            """)
            conn.commit()

    def insert_job(self, job: Job, available_at: Optional[str] = None) -> Job:
        """Insert a new job into the database."""
        now = current_iso_utc()
        avail = available_at or job.run_at or now

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO jobs (
                    id, command, state, attempts, max_retries,
                    created_at, updated_at, priority, run_at, available_at,
                    timeout, stdout, stderr, exit_code, worker_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id, job.command, job.state, job.attempts, job.max_retries,
                    job.created_at, now, job.priority, job.run_at, avail,
                    job.timeout, job.stdout, job.stderr, job.exit_code, job.worker_id
                )
            )
            conn.commit()
        return job

    def claim_next_job(self, worker_id: str) -> Optional[Job]:
        """Atomically claim the next eligible job using BEGIN IMMEDIATE transaction."""
        now = current_iso_utc()
        conn = self.get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            # Select pending jobs or failed jobs whose backoff timer (available_at) has passed
            cursor.execute(
                """
                SELECT * FROM jobs 
                WHERE (state = ? OR state = ?) 
                  AND (available_at IS NULL OR available_at <= ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (JobState.PENDING, JobState.FAILED, now)
            )
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None

            job_id = row["id"]
            cursor.execute(
                """
                UPDATE jobs 
                SET state = ?, worker_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (JobState.PROCESSING, worker_id, now, job_id)
            )
            conn.commit()
            
            job_dict = dict(row)
            job_dict["state"] = JobState.PROCESSING
            job_dict["worker_id"] = worker_id
            job_dict["updated_at"] = now
            return Job.from_dict(job_dict)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_job_status(
        self,
        job_id: str,
        state: str,
        attempts: Optional[int] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        exit_code: Optional[int] = None,
        available_at: Optional[str] = None,
        worker_id: Optional[str] = None
    ) -> None:
        """Update job state and execution attributes."""
        now = current_iso_utc()
        fields = ["state = ?", "updated_at = ?"]
        params = [state, now]

        if attempts is not None:
            fields.append("attempts = ?")
            params.append(attempts)
        if stdout is not None:
            fields.append("stdout = ?")
            params.append(stdout)
        if stderr is not None:
            fields.append("stderr = ?")
            params.append(stderr)
        if exit_code is not None:
            fields.append("exit_code = ?")
            params.append(exit_code)
        if available_at is not None:
            fields.append("available_at = ?")
            params.append(available_at)
        if worker_id is not None:
            fields.append("worker_id = ?")
            params.append(worker_id)

        params.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?"

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            conn.commit()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Fetch job by ID."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if row:
                return Job.from_dict(dict(row))
        return None

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        """List all jobs optionally filtered by state."""
        with self.connection() as conn:
            cursor = conn.cursor()
            if state:
                cursor.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC", (state,))
            else:
                cursor.execute("SELECT * FROM jobs ORDER BY created_at DESC")
            return [Job.from_dict(dict(row)) for row in cursor.fetchall()]

    def count_jobs_by_state(self) -> Dict[str, int]:
        """Return counts of jobs grouped by state."""
        counts = {s: 0 for s in JobState.ALL}
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
            for row in cursor.fetchall():
                if row["state"] in counts:
                    counts[row["state"]] = row["count"]
        return counts

    def register_worker(self, worker_id: str, pid: int) -> None:
        """Register worker process in DB."""
        now = current_iso_utc()
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO workers (id, pid, state, started_at, updated_at, current_job_id)
                VALUES (?, ?, 'active', ?, ?, NULL)
                ON CONFLICT(id) DO UPDATE SET pid = excluded.pid, state = 'active', updated_at = excluded.updated_at
                """,
                (worker_id, pid, now, now)
            )
            conn.commit()

    def update_worker_heartbeat(self, worker_id: str, current_job_id: Optional[str] = None) -> None:
        """Update heartbeat of worker process."""
        now = current_iso_utc()
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE workers SET updated_at = ?, current_job_id = ? WHERE id = ?",
                (now, current_job_id, worker_id)
            )
            conn.commit()

    def unregister_worker(self, worker_id: str) -> None:
        """Mark worker process as stopped or delete from DB."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
            conn.commit()

    def get_active_workers(self) -> List[Dict[str, Any]]:
        """Get list of active workers registered in DB."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM workers WHERE state = 'active'")
            return [dict(row) for row in cursor.fetchall()]

    def clean_stale_workers(self) -> None:
        """Remove workers that are no longer alive."""
        # Simple cleanup helper if needed
        pass
