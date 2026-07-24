import os
import sys
import time
import signal
import subprocess
import multiprocessing
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from queuectl.models import Job, JobState, current_iso_utc
from queuectl.db import Database
from queuectl.config import ConfigManager

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

class Worker:
    def __init__(self, worker_id: str, db_path: Optional[str] = None):
        self.worker_id = worker_id
        self.config = ConfigManager(db_path)
        self.db = Database(self.config.db_path)
        self.stop_requested = False
        self.current_process: Optional[subprocess.Popen] = None
        self._setup_signals()

    def _setup_signals(self):
        """Set up signal handlers for graceful shutdown."""
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except (ValueError, AttributeError):
            # Signals might not be supported in some thread/subprocess contexts
            pass

    def _handle_signal(self, signum, frame):
        """Signal handler to request graceful stop without interrupting in-flight job."""
        sys.stdout.write(f"[{self.worker_id}] Received stop signal ({signum}). Finishing in-flight job before stopping...\n")
        sys.stdout.flush()
        self.stop_requested = True

    def run(self):
        """Main worker polling and execution loop."""
        pid = os.getpid()
        self.db.register_worker(self.worker_id, pid)
        sys.stdout.write(f"[{self.worker_id}] Worker started (PID: {pid}).\n")
        sys.stdout.flush()

        poll_interval = self.config.get_float("worker_poll_interval", 1.0)

        try:
            while not self.stop_requested:
                self.db.update_worker_heartbeat(self.worker_id)
                job = self.db.claim_next_job(self.worker_id)

                if job:
                    self.db.update_worker_heartbeat(self.worker_id, current_job_id=job.id)
                    self._execute_job(job)
                    self.db.update_worker_heartbeat(self.worker_id, current_job_id=None)
                else:
                    # Sleep in small increments so stop request can be detected quickly
                    sleep_time = 0
                    while sleep_time < poll_interval and not self.stop_requested:
                        time.sleep(0.2)
                        sleep_time += 0.2
        finally:
            self.db.unregister_worker(self.worker_id)
            sys.stdout.write(f"[{self.worker_id}] Worker stopped cleanly.\n")
            sys.stdout.flush()

    def _execute_job(self, job: Job):
        """Execute job command and update state, retry, or move to DLQ."""
        new_attempts = job.attempts + 1
        sys.stdout.write(f"[{self.worker_id}] Processing Job '{job.id}' (Attempt {new_attempts}/{job.max_retries}): {job.command}\n")
        sys.stdout.flush()

        stdout_data, stderr_data, exit_code = "", "", -1
        timed_out = False

        try:
            self.current_process = subprocess.Popen(
                job.command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            try:
                stdout_data, stderr_data = self.current_process.communicate(timeout=job.timeout)
                exit_code = self.current_process.returncode
            except subprocess.TimeoutExpired:
                self.current_process.kill()
                stdout_data, stderr_data = self.current_process.communicate()
                exit_code = -1
                timed_out = True
                stderr_data = (stderr_data or "") + f"\nJob timed out after {job.timeout} seconds."
        except Exception as e:
            exit_code = -1
            stderr_data = f"Failed to execute command: {str(e)}"
        finally:
            self.current_process = None

        is_success = (exit_code == 0) and not timed_out

        if is_success:
            sys.stdout.write(f"[{self.worker_id}] Job '{job.id}' COMPLETED successfully.\n")
            sys.stdout.flush()
            self.db.update_job_status(
                job_id=job.id,
                state=JobState.COMPLETED,
                attempts=new_attempts,
                stdout=stdout_data or "",
                stderr=stderr_data or "",
                exit_code=exit_code,
                worker_id=self.worker_id
            )
        else:
            backoff_base = self.config.get_float("backoff_base", 2.0)
            if new_attempts < job.max_retries:
                # Exponential backoff delay = base ^ attempts seconds
                delay_seconds = float(backoff_base ** new_attempts)
                next_avail = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
                next_avail_str = next_avail.strftime("%Y-%m-%dT%H:%M:%SZ")

                sys.stdout.write(
                    f"[{self.worker_id}] Job '{job.id}' FAILED (Exit Code: {exit_code}). "
                    f"Rescheduling in {delay_seconds:.1f}s (Attempt {new_attempts}/{job.max_retries}).\n"
                )
                sys.stdout.flush()

                self.db.update_job_status(
                    job_id=job.id,
                    state=JobState.FAILED,
                    attempts=new_attempts,
                    stdout=stdout_data or "",
                    stderr=stderr_data or "",
                    exit_code=exit_code,
                    available_at=next_avail_str,
                    worker_id=self.worker_id
                )
            else:
                # Exhausted retries -> Dead Letter Queue (DLQ)
                sys.stdout.write(
                    f"[{self.worker_id}] Job '{job.id}' FAILED permanently. "
                    f"Max retries ({job.max_retries}) exhausted. Moved to DLQ.\n"
                )
                sys.stdout.flush()

                self.db.update_job_status(
                    job_id=job.id,
                    state=JobState.DEAD,
                    attempts=new_attempts,
                    stdout=stdout_data or "",
                    stderr=stderr_data or "",
                    exit_code=exit_code,
                    worker_id=self.worker_id
                )


def run_worker_process(worker_id: str, db_path: Optional[str] = None):
    """Entry point for a standalone worker process."""
    worker = Worker(worker_id, db_path)
    worker.run()


class WorkerPoolManager:
    """Manages spawning and stopping worker processes."""

    def __init__(self, db_path: Optional[str] = None):
        self.config = ConfigManager(db_path)
        self.pid_file = os.path.expanduser("~/.queuectl/workers.pid")

    def _read_pids(self) -> List[int]:
        if not os.path.exists(self.pid_file):
            return []
        try:
            with open(self.pid_file, "r") as f:
                return [int(line.strip()) for line in f if line.strip().isdigit()]
        except Exception:
            return []

    def _write_pids(self, pids: List[int]):
        os.makedirs(os.path.dirname(self.pid_file), exist_ok=True)
        with open(self.pid_file, "w") as f:
            for pid in pids:
                f.write(f"{pid}\n")

    def start_workers(self, count: int, detach: bool = True) -> List[int]:
        """Start N worker processes."""
        existing_pids = self._read_pids()
        alive_pids = []
        for pid in existing_pids:
            if self._is_process_alive(pid):
                alive_pids.append(pid)

        new_pids = []
        for i in range(count):
            worker_id = f"worker-{os.getpid()}-{i+1}-{int(time.time()*1000)%10000}"
            p = multiprocessing.Process(
                target=run_worker_process,
                args=(worker_id, self.config.db_path)
            )
            p.daemon = False
            p.start()
            new_pids.append(p.pid)

        all_pids = alive_pids + new_pids
        self._write_pids(all_pids)
        return new_pids

    def stop_workers(self) -> List[int]:
        """Gracefully stop all managed worker processes using SIGTERM."""
        pids = self._read_pids()
        stopped_pids = []
        for pid in pids:
            if self._is_process_alive(pid):
                try:
                    os.kill(pid, signal.SIGTERM)
                    stopped_pids.append(pid)
                except Exception as e:
                    sys.stderr.write(f"Failed to send SIGTERM to PID {pid}: {e}\n")
        
        self._write_pids([])
        return stopped_pids

    @staticmethod
    def _is_process_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            if sys.platform == "win32":
                import ctypes
                PROCESS_QUERY_INFORMATION = 0x0400
                STILL_ACTIVE = 259
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
                if not handle:
                    return False
                exit_code = ctypes.c_ulong()
                ret = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                kernel32.CloseHandle(handle)
                return bool(ret and exit_code.value == STILL_ACTIVE)
            else:
                os.kill(pid, 0)
                return True
        except Exception:
            return False
