import os
import sys
import time
import tempfile
import pytest
from queuectl.models import Job, JobState
from queuectl.db import Database
from queuectl.config import ConfigManager
from queuectl.queue_manager import QueueManager
from queuectl.worker import Worker, run_worker_process, WorkerPoolManager

@pytest.fixture
def temp_db():
    """Create a temporary database file for test isolation."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Database(path)
    config = ConfigManager(path)
    qm = QueueManager(db, config)
    yield qm, db, config, path
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def test_enqueue_and_job_schema(temp_db):
    """Test job enqueuing and JSON schema compliance."""
    qm, db, config, path = temp_db

    # Test enqueueing with JSON string format
    json_str = '{"id":"test-1","command":"echo Hello","max_retries":3}'
    job = qm.enqueue(json_str)

    assert job.id == "test-1"
    assert job.command == "echo Hello"
    assert job.state == JobState.PENDING
    assert job.attempts == 0
    assert job.max_retries == 3
    assert job.created_at is not None
    assert job.updated_at is not None

    # Test fetching job from DB
    fetched = db.get_job("test-1")
    assert fetched is not None
    assert fetched.command == "echo Hello"


def test_successful_job_execution(temp_db):
    """Scenario 1: A job that completes successfully."""
    qm, db, config, path = temp_db

    job = qm.enqueue('{"id":"job-succ","command":"echo Successful Execution"}')
    assert job.state == JobState.PENDING

    # Run 1 worker iteration
    worker = Worker("worker-test", path)
    claimed_job = db.claim_next_job("worker-test")
    assert claimed_job is not None
    assert claimed_job.id == "job-succ"

    worker._execute_job(claimed_job)

    updated_job = db.get_job("job-succ")
    assert updated_job.state == JobState.COMPLETED
    assert updated_job.attempts == 1
    assert "Successful Execution" in updated_job.stdout
    assert updated_job.exit_code == 0


def test_failed_job_backoff_and_dlq(temp_db):
    """Scenario 2 & 4: Invalid/failing job retries with backoff and moves to DLQ."""
    qm, db, config, path = temp_db

    config.set("backoff_base", "0.1")  # Fast backoff for testing
    job = qm.enqueue('{"id":"job-fail","command":"non_existent_command_xyz_123","max_retries":2}')
    
    worker = Worker("worker-fail-test", path)

    # Attempt 1
    claimed = db.claim_next_job("worker-fail-test")
    assert claimed is not None
    worker._execute_job(claimed)

    updated = db.get_job("job-fail")
    assert updated.state == JobState.FAILED
    assert updated.attempts == 1

    # Wait out tiny backoff
    time.sleep(0.2)

    # Attempt 2 (Max retries reached -> DLQ)
    claimed2 = db.claim_next_job("worker-fail-test")
    assert claimed2 is not None
    worker._execute_job(claimed2)

    dlq_job = db.get_job("job-fail")
    assert dlq_job.state == JobState.DEAD
    assert dlq_job.attempts == 2

    # Check DLQ list
    dlq_jobs = qm.dlq_list()
    assert len(dlq_jobs) == 1
    assert dlq_jobs[0].id == "job-fail"


def test_dlq_retry(temp_db):
    """Test retrying a dead-lettered job."""
    qm, db, config, path = temp_db

    # Manually insert a dead job
    job = Job(id="dead-1", command="echo RetryMe", state=JobState.DEAD, attempts=3, max_retries=3)
    db.insert_job(job)

    assert len(qm.dlq_list()) == 1

    retried_job = qm.dlq_retry("dead-1")
    assert retried_job.state == JobState.PENDING
    assert retried_job.attempts == 0
    assert len(qm.dlq_list()) == 0


def test_concurrent_workers_no_duplicate_processing(temp_db):
    """Scenario 3: Multiple workers running concurrently without double-processing."""
    qm, db, config, path = temp_db

    # Enqueue 10 jobs
    for i in range(10):
        qm.enqueue(f'{{"id":"job-{i}","command":"echo Job {i}"}}')

    claimed_jobs = set()

    # Create 3 workers and try claiming jobs concurrently
    workers = [Worker(f"worker-{w}", path) for w in range(3)]
    
    for _ in range(10):
        for w in workers:
            claimed = db.claim_next_job(w.worker_id)
            if claimed:
                assert claimed.id not in claimed_jobs, f"Job {claimed.id} was double-claimed!"
                claimed_jobs.add(claimed.id)
                w._execute_job(claimed)

    assert len(claimed_jobs) == 10
    counts = qm.status()["counts"]
    assert counts[JobState.COMPLETED] == 10
    assert counts[JobState.PENDING] == 0


def test_persistence_survival_across_restarts(temp_db):
    """Scenario 5: Job state survives queue system restarts."""
    qm, db, config, path = temp_db

    qm.enqueue('{"id":"persist-1","command":"echo Persist"}')
    qm.enqueue('{"id":"persist-2","command":"echo Persist2"}')

    # Close connections / instantiate new DB instance from same file path
    new_db = Database(path)
    new_qm = QueueManager(new_db)

    j1 = new_qm.get_job("persist-1")
    j2 = new_qm.get_job("persist-2")

    assert j1 is not None and j1.command == "echo Persist"
    assert j2 is not None and j2.command == "echo Persist2"
    assert new_qm.status()["total_jobs"] == 2


def test_configuration_management(temp_db):
    """Test getting and setting configuration parameters."""
    qm, db, config, path = temp_db

    config.set("max_retries", "5")
    config.set("backoff_base", "3.0")

    assert config.get_int("max_retries") == 5
    assert config.get_float("backoff_base") == 3.0

    # Job enqueued without explicit max_retries should inherit config default
    job = qm.enqueue("echo test_config")
    assert job.max_retries == 5
