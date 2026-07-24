import sys
import json
import click
from tabulate import tabulate
from typing import Optional
from queuectl.queue_manager import QueueManager
from queuectl.config import ConfigManager
from queuectl.worker import WorkerPoolManager, run_worker_process
from queuectl.dashboard import run_dashboard

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

@click.group()
@click.option("--db-path", envvar="QUEUECTL_DB_PATH", help="Path to SQLite database file")
@click.pass_context
def cli(ctx, db_path: Optional[str]):
    """queuectl - CLI Background Job Queue System"""
    ctx.ensure_object(dict)
    config = ConfigManager(db_path)
    qm = QueueManager(config=config)
    ctx.obj["config"] = config
    ctx.obj["qm"] = qm

# --- ENQUEUE ---
@cli.command("enqueue")
@click.argument("job_data", required=False)
@click.option("--command", "-c", help="Command string to execute")
@click.option("--max-retries", "-r", type=int, help="Maximum retry attempts")
@click.option("--priority", "-p", type=int, default=0, help="Job priority")
@click.option("--run-at", help="Scheduled ISO8601 execution time")
@click.option("--timeout", type=int, help="Execution timeout in seconds")
@click.pass_context
def enqueue_cmd(ctx, job_data: Optional[str], command: Optional[str], max_retries: Optional[int], priority: int, run_at: Optional[str], timeout: Optional[str]):
    """Add a new job to the queue.

    Usage examples:\n
      queuectl enqueue '{"id":"job1","command":"echo Hello"}'\n
      queuectl enqueue '{"command":"sleep 2"}'\n
      queuectl enqueue --command "echo Hello" --max-retries 5
    """
    qm: QueueManager = ctx.obj["qm"]

    if job_data:
        # Check if user provided JSON string or raw command string
        try:
            parsed = json.loads(job_data)
            if isinstance(parsed, dict):
                if max_retries is not None:
                    parsed["max_retries"] = max_retries
                if priority != 0:
                    parsed["priority"] = priority
                if run_at:
                    parsed["run_at"] = run_at
                if timeout is not None:
                    parsed["timeout"] = timeout
                payload = parsed
            else:
                payload = {"command": str(parsed)}
        except (json.JSONDecodeError, TypeError):
            payload = {"command": job_data}
    elif command:
        payload = {"command": command}
    else:
        click.echo("Error: Please provide job JSON string or --command option.", err=True)
        sys.exit(1)

    if max_retries is not None and "max_retries" not in payload:
        payload["max_retries"] = max_retries
    if priority != 0 and "priority" not in payload:
        payload["priority"] = priority
    if run_at and "run_at" not in payload:
        payload["run_at"] = run_at
    if timeout is not None and "timeout" not in payload:
        payload["timeout"] = timeout

    try:
        job = qm.enqueue(payload)
        click.echo(f"✅ Job enqueued successfully!")
        click.echo(f"   ID:          {job.id}")
        click.echo(f"   Command:     {job.command}")
        click.echo(f"   State:       {job.state}")
        click.echo(f"   Max Retries: {job.max_retries}")
    except Exception as e:
        click.echo(f"❌ Failed to enqueue job: {e}", err=True)
        sys.exit(1)


# --- WORKER GROUP ---
@cli.group("worker")
def worker_group():
    """Worker process management commands."""
    pass

@worker_group.command("start")
@click.option("--count", "-n", default=1, type=int, help="Number of worker processes to launch")
@click.option("--foreground", "-f", is_flag=True, help="Run a single worker in foreground mode")
@click.pass_context
def worker_start(ctx, count: int, foreground: bool):
    """Launch worker process(es) that poll and execute jobs."""
    config: ConfigManager = ctx.obj["config"]

    if foreground or count == 1 and sys.stdout.isatty() and not sys.platform == "win32":
        # Run single worker in foreground if requested
        click.echo(f"🚀 Starting 1 worker in foreground...")
        run_worker_process("worker-fg-1", config.db_path)
    else:
        pool = WorkerPoolManager(config.db_path)
        pids = pool.start_workers(count)
        click.echo(f"🚀 Launched {len(pids)} worker process(es). PIDs: {', '.join(map(str, pids))}")

@worker_group.command("stop")
@click.pass_context
def worker_stop(ctx):
    """Gracefully stop running worker processes (finish in-flight job first)."""
    config: ConfigManager = ctx.obj["config"]
    pool = WorkerPoolManager(config.db_path)
    stopped = pool.stop_workers()
    if stopped:
        click.echo(f"🛑 Sent graceful stop signal to {len(stopped)} worker process(es). PIDs: {', '.join(map(str, stopped))}")
    else:
        click.echo("ℹ️ No active managed worker processes found.")


# --- STATUS ---
@cli.command("status")
@click.pass_context
def status_cmd(ctx):
    """Show counts of jobs per state + active workers."""
    qm: QueueManager = ctx.obj["qm"]
    st = qm.status()

    # Compact, line-oriented status matching requested format
    counts = st["counts"]
    pending = counts.get("pending", 0)
    processing = counts.get("processing", 0)
    completed = counts.get("completed", 0)
    failed = counts.get("failed", 0)
    dead = counts.get("dead", 0)

    click.echo(f"Pending: {pending}")
    click.echo(f"Processing: {processing}")
    click.echo(f"Completed: {completed}")
    click.echo(f"Failed: {failed}")
    click.echo(f"Dead: {dead}")
    click.echo(f"Active Workers: {st['active_workers']}")


# --- LIST JOBS ---
@cli.command("list")
@click.option("--state", help="Filter jobs by state (pending, processing, completed, failed, dead)")
@click.pass_context
def list_cmd(ctx, state: Optional[str]):
    """List jobs filtered by state."""
    qm: QueueManager = ctx.obj["qm"]
    jobs = qm.list_jobs(state=state)

    if not jobs:
        msg = f"No jobs found with state '{state}'." if state else "No jobs found in queue."
        click.echo(f"ℹ️ {msg}")
        return

    table_data = []
    for j in jobs:
        table_data.append([
            j.id,
            j.command if len(j.command) <= 30 else j.command[:27] + "...",
            j.state.upper(),
            f"{j.attempts}/{j.max_retries}",
            j.created_at,
            j.worker_id or "-"
        ])

    click.echo(f"\n📋 JOBS LIST" + (f" (State: {state.upper()})" if state else ""))
    click.echo("=" * 70)
    click.echo(tabulate(table_data, headers=["ID", "Command", "State", "Attempts", "Created At", "Worker"], tablefmt="simple"))
    click.echo(f"\nTotal: {len(jobs)} job(s)")


# --- DLQ GROUP ---
@cli.group("dlq")
def dlq_group():
    """Dead Letter Queue (DLQ) operations."""
    pass

@dlq_group.command("list")
@click.pass_context
def dlq_list_cmd(ctx):
    """List dead-lettered jobs."""
    qm: QueueManager = ctx.obj["qm"]
    jobs = qm.dlq_list()

    if not jobs:
        click.echo("✨ Dead Letter Queue is empty!")
        return

    table_data = []
    for j in jobs:
        table_data.append([
            j.id,
            j.command,
            j.attempts,
            j.max_retries,
            j.updated_at,
            (j.stderr[:40] + "...") if len(j.stderr) > 40 else j.stderr
        ])

    click.echo("\n💀 DEAD LETTER QUEUE (DLQ)")
    click.echo("=" * 75)
    click.echo(tabulate(table_data, headers=["Job ID", "Command", "Attempts", "Max Retries", "Failed At", "Error"], tablefmt="simple"))
    click.echo(f"\nTotal Dead Jobs: {len(jobs)}")

@dlq_group.command("retry")
@click.argument("job_id")
@click.pass_context
def dlq_retry_cmd(ctx, job_id: str):
    """Requeue a DLQ job as pending, reset attempts to 0."""
    qm: QueueManager = ctx.obj["qm"]
    try:
        job = qm.dlq_retry(job_id)
        click.echo(f"🔄 Re-queued job '{job.id}' to pending state with reset attempts (0/{job.max_retries}).")
    except ValueError as e:
        click.echo(f"❌ {e}", err=True)
        sys.exit(1)


# --- CONFIG GROUP ---
@cli.group("config")
def config_group():
    """Manage configuration settings."""
    pass

@config_group.command("set")
@click.argument("key")
@click.argument("value")
@click.pass_context
def config_set_cmd(ctx, key: str, value: str):
    """Set config like max-retries, backoff-base."""
    config: ConfigManager = ctx.obj["config"]
    config.set(key, value)
    click.echo(f"⚙️ Config updated: {key} = {value}")

@config_group.command("get")
@click.argument("key", required=False)
@click.pass_context
def config_get_cmd(ctx, key: Optional[str]):
    """Get config setting(s)."""
    config: ConfigManager = ctx.obj["config"]
    if key:
        val = config.get(key)
        click.echo(f"{key}: {val}")
    else:
        all_cfg = config.get_all()
        click.echo("\n⚙️ CONFIGURATION")
        click.echo("=" * 35)
        for k, v in all_cfg.items():
            click.echo(f"  {k}: {v}")


# --- LOG COMMAND ---
@cli.command("log")
@click.argument("job_id")
@click.pass_context
def log_cmd(ctx, job_id: str):
    """View captured stdout and stderr output for a job."""
    qm: QueueManager = ctx.obj["qm"]
    job = qm.get_job(job_id)
    if not job:
        click.echo(f"❌ Job '{job_id}' not found.", err=True)
        sys.exit(1)

    click.echo(f"\n📜 JOB EXECUTION LOG (ID: {job.id})")
    click.echo("=" * 50)
    click.echo(f"Command:     {job.command}")
    click.echo(f"State:       {job.state.upper()}")
    click.echo(f"Exit Code:   {job.exit_code if job.exit_code is not None else 'N/A'}")
    click.echo(f"Worker:      {job.worker_id or 'N/A'}")
    click.echo("\n--- STDOUT ---")
    click.echo(job.stdout if job.stdout else "(empty)")
    click.echo("\n--- STDERR ---")
    click.echo(job.stderr if job.stderr else "(empty)")


# --- DASHBOARD COMMAND ---
@cli.command("dashboard")
@click.option("--port", "-p", default=8080, type=int, help="Dashboard HTTP port")
@click.pass_context
def dashboard_cmd(ctx, port: int):
    """Launch the Web Dashboard for real-time monitoring."""
    config: ConfigManager = ctx.obj["config"]
    run_dashboard(port, config.db_path)


if __name__ == "__main__":
    cli(obj={})
