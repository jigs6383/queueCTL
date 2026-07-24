from queuectl.worker import Worker
from queuectl.db import Database
from queuectl.config import ConfigManager
import time

config = ConfigManager()
db = Database(config.db_path)
worker = Worker('one-off-processor', config.db_path)

processed = 0
while True:
    job = db.claim_next_job(worker.worker_id)
    if not job:
        break
    print(f"Claimed job: {job.id} - {job.command}")
    try:
        worker._execute_job(job)
        processed += 1
    except Exception as e:
        print(f"Error executing {job.id}: {e}")
    # small pause so DB updates settle
    time.sleep(0.1)

print(f"Processed {processed} job(s)")
