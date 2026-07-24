import sqlite3, json
from pathlib import Path
p = Path.home() / '.queuectl' / 'queuectl.db'
print('DB:', p)
conn = sqlite3.connect(str(p))
conn.row_factory = sqlite3.Row
c = conn.cursor()
c.execute('SELECT id,state,command,attempts,available_at,created_at FROM jobs ORDER BY created_at')
rows = [dict(r) for r in c.fetchall()]
print('JOBS:')
print(json.dumps(rows, indent=2))
print('\nWORKERS:')
c.execute('SELECT id,pid,started_at,updated_at,current_job_id FROM workers')
print(json.dumps([dict(r) for r in c.fetchall()], indent=2))
conn.close()