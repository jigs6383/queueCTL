# QueueCTL - Architecture & Technical Specification

## System Architecture

`queuectl` is a background job queue system built for high performance, persistence, and concurrency safety across multiple worker processes.

```
                  +-------------------------------+
                  |        queuectl CLI           |
                  +---------------+---------------+
                                  |
                                  v
+---------------------------------+---------------------------------+
|                         SQLite Database                           |
|                       (WAL Mode + Transaction Locks)              |
+--------+------------------------+------------------------+--------+
         |                        |                        |
         v                        v                        v
  +--------------+         +--------------+         +--------------+
  | Worker Proc 1|         | Worker Proc 2|         | Worker Proc N|
  +--------------+         +--------------+         +--------------+
         |                        |                        |
  +--------------+         +--------------+         +--------------+
  | Subprocess 1 |         | Subprocess 2 |         | Subprocess N |
  +--------------+         +--------------+         +--------------+
```

---

## 1. Concurrency & Locking Strategy

To prevent race conditions where multiple workers process the same job:
1. **SQLite WAL Mode**: `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;` are configured at connection startup.
2. **Atomic Claiming**: Jobs are claimed inside an explicit `BEGIN IMMEDIATE` transaction block:
   ```sql
   BEGIN IMMEDIATE;
   SELECT * FROM jobs 
   WHERE (state = 'pending' OR state = 'failed') 
     AND (available_at IS NULL OR available_at <= CURRENT_TIMESTAMP)
   ORDER BY priority DESC, created_at ASC
   LIMIT 1;

   UPDATE jobs 
   SET state = 'processing', worker_id = ?, updated_at = ?
   WHERE id = ?;
   COMMIT;
   ```
3. Because `BEGIN IMMEDIATE` acquires a write lock before evaluating the candidate job, no two concurrent workers can view or claim the same pending job simultaneously.

---

## 2. Exponential Backoff & Retry Logic

When a job command fails (non-zero exit code, command not found, or timeout):
- `attempts` is incremented by 1.
- If `attempts < max_retries`:
  $$\text{delay\_seconds} = \text{backoff\_base}^{\text{attempts}}$$
  `available_at` is set to $\text{now} + \text{delay\_seconds}$. State becomes `failed`. The job will not be polled by any worker until `available_at <= current_time`.
- If `attempts >= max_retries`:
  State transitions permanently to `dead` (Dead Letter Queue - DLQ).

---

## 3. Worker Lifecycle & Graceful Shutdown

1. **Heartbeat & Registration**: Workers register their PID and status in the `workers` table.
2. **Polled Loop**: Workers periodically check for eligible jobs.
3. **Signal Handling (`SIGTERM` / `SIGINT`)**:
   - Signal handler sets `self.stop_requested = True`.
   - If worker is executing a job subprocess (`subprocess.Popen`), it allows the job to complete and records output before exiting.
   - If idle, worker breaks poll loop immediately and unregisters cleanly.

---

## 4. Web Dashboard & Metrics

The built-in web dashboard provides real-time visibility into queue states, active worker count, and individual job logs via an auto-refreshing interface powered by Python's standard `http.server`.
