# QueueCTL - CLI Background Job Queue System

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`queuectl` is a production-grade CLI background job queue system built with Python, SQLite (WAL mode), and multiprocessing. It manages background job execution with configurable worker processes, exponential backoff retries, dead letter queue (DLQ) handling, job logging, scheduled execution, priority queueing, and a real-time web dashboard.

---

## 🚀 Features

- ⚡ **Atomic Concurrency Handling**: Prevents duplicate job processing using SQLite WAL mode and `BEGIN IMMEDIATE` transaction locking across multi-worker pools.
- 🔁 **Exponential Backoff Retries**: Automatically retries failing jobs with configurable exponential backoff ($\text{delay} = \text{backoff\_base}^{\text{attempts}}$).
- 💀 **Dead Letter Queue (DLQ)**: Permanently failed jobs after exhausting retries are moved to DLQ and can be inspected or re-queued (`queuectl dlq retry <job_id>`).
- 🛑 **Graceful Worker Shutdown**: Workers handle `SIGINT`/`SIGTERM` by letting active in-flight subprocesses finish before terminating.
- 📜 **Job Output Logging**: Standard output (`stdout`), standard error (`stderr`), and exit codes are captured and stored per job (`queuectl log <job_id>`).
- ⏱️ **Scheduled & Priority Jobs**: Support for delayed execution (`run_at`) and job priority (`priority`).
- 🌐 **Web Dashboard**: Included real-time HTTP monitoring dashboard (`queuectl dashboard`).
- 💾 **Persistent SQLite Storage**: Queue state survives system restarts and process crashes.

---

## 📦 Setup & Installation Instructions

### Prerequisites
- Python 3.8+
- `pip`

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/queuectl.git
   cd queuectl
   ```

2. **Install in editable mode:**
   ```bash
   pip install -e .
   ```
   *(Or install requirements manually: `pip install -r requirements.txt`)*

3. **Verify installation:**
   ```bash
   queuectl --help
   ```

---

## 💻 CLI Commands & Usage Examples

### 1. Enqueue Jobs (`queuectl enqueue`)

Enqueue a job using a JSON string or CLI options:

```bash
# Using JSON string
queuectl enqueue '{"id":"job-01","command":"echo Hello World","max_retries":3}'

# Using --command option
queuectl enqueue --command "python -c 'print(42)'" --max-retries 5 --priority 10
```

**Output:**
```
✅ Job enqueued successfully!
   ID:          job-01
   Command:     echo Hello World
   State:       pending
   Max Retries: 3
```

---

### 2. Manage Workers (`queuectl worker`)

Start $N$ concurrent worker processes:

```bash
# Launch 3 background worker processes
queuectl worker start --count 3

# Or run single worker in foreground
queuectl worker start --foreground
```

Gracefully stop running workers:

```bash
queuectl worker stop
```

**Output:**
```
🛑 Sent graceful stop signal to 3 worker process(es). PIDs: 1245, 1246, 1247
```

---

### 3. Check Queue Status (`queuectl status`)

Displays current job state counts and active managed workers:

```bash
queuectl status
```

**Sample Output:**
```
📊 QUEUE STATUS
===================================
Job State      Count
-----------  -------
PENDING            2
PROCESSING         1
COMPLETED         15
FAILED             0
DEAD               1

👷 WORKERS
-----------------------------------
Active Managed Workers: 3
Worker ID                 PID    Started At            Current Job
-----------------------  -----  --------------------  -------------
worker-1021-1-4912        1245   2026-07-24T10:20:00Z  job-01
worker-1021-2-4913        1246   2026-07-24T10:20:00Z  idle
worker-1021-3-4914        1247   2026-07-24T10:20:00Z  idle
```

---

### 4. List Jobs (`queuectl list`)

List all jobs or filter by state (`pending`, `processing`, `completed`, `failed`, `dead`):

```bash
queuectl list --state pending
```

**Sample Output:**
```
📋 JOBS LIST (State: PENDING)
======================================================================
ID        Command                         State    Attempts    Created At            Worker
--------  ------------------------------  -------  ----------  --------------------  --------
job-01    echo Hello World                PENDING  0/3         2026-07-24T10:24:00Z  -
job-02    python script.py                PENDING  1/3         2026-07-24T10:24:10Z  -
```

---

### 5. Dead Letter Queue (`queuectl dlq`)

List dead-lettered jobs:

```bash
queuectl dlq list
```

Requeue a dead-lettered job back to `pending` with reset attempts (0):

```bash
queuectl dlq retry job-02
```

---

### 6. View Job Execution Logs (`queuectl log`)

Inspect captured stdout, stderr, and exit codes:

```bash
queuectl log job-01
```

**Sample Output:**
```
📜 JOB EXECUTION LOG (ID: job-01)
==================================================
Command:     echo Hello World
State:       COMPLETED
Exit Code:   0
Worker:      worker-1021-1-4912

--- STDOUT ---
Hello World

--- STDERR ---
(empty)
```

---

### 7. Manage Configuration (`queuectl config`)

Set or get configuration values (`max-retries`, `backoff-base`, `worker_poll_interval`):

```bash
queuectl config set max-retries 5
queuectl config set backoff-base 2.0
queuectl config get
```

---

### 8. Web Dashboard (`queuectl dashboard`)

Launch the real-time Web Dashboard monitoring interface:

```bash
queuectl dashboard --port 8080
```
Open [http://localhost:8080](http://localhost:8080) in your browser.

---

## 🏗️ Architecture & Core Design Choices

For a complete architectural specification, see [architecture.md](file:///C:/Users/gokul/.gemini/antigravity/scratch/queuectl/architecture.md).

### Summary:
1. **SQLite Storage & WAL Mode**: SQLite was chosen for zero-dependency local persistence. `PRAGMA journal_mode=WAL` allows simultaneous concurrent readers and quick serialized writes.
2. **Race Condition Prevention**: Workers fetch and mark jobs inside `BEGIN IMMEDIATE` transactions to prevent duplicate job claims across processes.
3. **Job Lifecycle**:
   ```
   [pending] ──> [processing] ──> [completed]
       ▲              │
       │              ├──> (Attempt < MaxRetries) ──> [failed] (Exponential Backoff Wait)
       │              │
       └──────────────┴──> (Attempt >= MaxRetries) ──> [dead] (DLQ)
   ```

---

## 🧪 Testing Instructions

Run the automated pytest suite covering all 7 core scenarios:

```bash
python -m pytest -v
```

### Covered Test Cases:
1. `test_enqueue_and_job_schema`: Enqueueing & JSON schema validation.
2. `test_successful_job_execution`: Successful job completion & stdout capture.
3. `test_failed_job_backoff_and_dlq`: Repeated failure, backoff calculation, and DLQ transition.
4. `test_dlq_retry`: DLQ job re-queuing to pending state.
5. `test_concurrent_workers_no_duplicate_processing`: Multi-worker concurrent execution without double-claiming.
6. `test_persistence_survival_across_restarts`: Database queue survival across system restarts.
7. `test_configuration_management`: Dynamically updating queue settings.

---

## 🧠 Assumptions & Trade-offs

- **Subprocess Shell Execution**: Commands are executed via `subprocess.Popen(command, shell=True)`. This allows flexibility for complex shell scripts but requires commands to be trusted inputs.
- **Process Signals on Windows vs POSIX**: Signal handling (`SIGTERM`/`SIGINT`) works natively on Linux/macOS and Windows. On Windows, worker processes also monitor PID/heartbeat state in SQLite.

---

## 📹 Demo Recording

- **CLI Demo Video**: [Link to Demo Video Recording](https://drive.google.com/file/d/placeholder-demo-video/view) *(Placeholder)*
