import os
import sqlite3
from typing import Any, Dict

DEFAULT_CONFIG = {
    "max_retries": "3",
    "backoff_base": "2",
    "worker_poll_interval": "1.0",
    "default_job_timeout": "60",
}

def get_db_path() -> str:
    """Get the SQLite database path."""
    env_path = os.environ.get("QUEUECTL_DB_PATH")
    if env_path:
        return env_path
    
    # Store in ~/.queuectl/queuectl.db by default
    config_dir = os.path.expanduser("~/.queuectl")
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, "queuectl.db")

class ConfigManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def get(self, key: str, default: Any = None) -> str:
        """Get a configuration value from DB or default."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
                row = cursor.fetchone()
                if row:
                    return row["value"]
        except sqlite3.OperationalError:
            pass
        
        normalized_key = key.replace("-", "_")
        if normalized_key in DEFAULT_CONFIG:
            return DEFAULT_CONFIG[normalized_key]
        if key in DEFAULT_CONFIG:
            return DEFAULT_CONFIG[key]
        return default

    def get_int(self, key: str, default: int = 0) -> int:
        val = self.get(key)
        if val is not None:
            try:
                return int(val)
            except ValueError:
                pass
        return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        val = self.get(key)
        if val is not None:
            try:
                return float(val)
            except ValueError:
                pass
        return default

    def set(self, key: str, value: str) -> None:
        """Set a configuration value in DB."""
        normalized_key = key.replace("-", "_")
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (normalized_key, str(value)),
            )
            conn.commit()

    def get_all(self) -> Dict[str, str]:
        """Get all configurations."""
        result = dict(DEFAULT_CONFIG)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT key, value FROM config")
                for row in cursor.fetchall():
                    result[row["key"]] = row["value"]
        except sqlite3.OperationalError:
            pass
        return result
