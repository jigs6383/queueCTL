from queuectl.queue_manager import QueueManager
from queuectl.worker import Worker
from queuectl.db import Database
from queuectl.config import ConfigManager
import time

# Enqueue a demo job
qm = QueueManager()
job = qm.enqueue({'command':'echo demo-live-output','id':'demo-live-1'})
print('Enqueued job:', job.id)

# Create a worker and claim+execute job
config = ConfigManager()
db = Database(config.db_path)
worker = Worker('demo-fg-1', config.db_path)
claimed = db.claim_next_job(worker.worker_id)
if claimed:
    print(f"Claimed job: {claimed.id}")
    worker._execute_job(claimed)
    updated = db.get_job(claimed.id)
    print('Updated state:', updated.state, 'attempts:', updated.attempts)
else:
    print('No job claimed')
