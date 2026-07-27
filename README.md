# QueueCTL

A CLI-based background job queue system built using Python.


## Demo

Google Drive Link

https://drive.google.com/drive/folders/1TZNDi0vBBfioat_yaMUHloL0l5Zr2Qj8?usp=sharing



## Features

- Background job processing
- Multiple workers
- Retry with exponential backoff
- Dead Letter Queue (DLQ)
- Persistent storage
- Configurable retry settings

---

## Tech Stack

- Python
- SQLite
- Click
- setuptools

---

## Project Structure

queuectl/
├── queuectl/
├── scripts/
├── tests/
├── README.md
├── requirements.txt
└── setup.py

---

## Setup

Clone the repository

```bash
git clone https://github.com/jigs6383/queueCTL.git
cd queuectl
```

Install dependencies

```bash
pip install -r requirements.txt
```

Run

```bash
python -m queuectl.cli
```

---

## CLI Commands

Enqueue a job

```bash
queuectl enqueue '{"id":"job1","command":"echo Hello"}'
```

Start workers

```bash
queuectl worker start --count 2
```

Status

```bash
queuectl status
```

List jobs

```bash
queuectl list
```

DLQ

```bash
queuectl dlq list
```

Retry DLQ Job

```bash
queuectl dlq retry job1
```

---

## Architecture

Job States

Pending
↓

Processing
↓

Completed

OR

Failed
↓

Retry
↓

Dead Letter Queue

Persistence

Jobs are stored in SQLite and loaded on startup.

Workers

Workers poll pending jobs, lock them, execute commands, retry failures using exponential backoff, and move permanently failed jobs to the DLQ.

---

## Testing

✔ Successful job

✔ Failed job

✔ Retry

✔ DLQ

✔ Persistence

✔ Multiple workers

---

## Demo

Google Drive Link

https://drive.google.com/your-demo-link
