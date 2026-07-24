import http.server
import socketserver
import json
import urllib.parse
from typing import Optional
from queuectl.queue_manager import QueueManager
from queuectl.config import ConfigManager

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QueueCTL Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0f172a;
            --card-bg: #1e293b;
            --border: #334155;
            --text: #f8fafc;
            --text-muted: #94a3b8;
            --accent: #6366f1;
            --accent-hover: #4f46e5;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --info: #3b82f6;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
        body { background-color: var(--bg); color: var(--text); padding: 2rem; min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; }
        header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; border-bottom: 1px solid var(--border); padding-bottom: 1rem; }
        h1 { font-size: 1.75rem; font-weight: 700; background: linear-gradient(135deg, #818cf8, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .badge-live { display: inline-flex; align-items: center; gap: 0.5rem; background: rgba(16, 185, 129, 0.1); color: var(--success); padding: 0.4rem 0.8rem; border-radius: 9999px; font-size: 0.875rem; font-weight: 500; border: 1px solid rgba(16, 185, 129, 0.2); }
        .pulse { width: 8px; height: 8px; background: var(--success); border-radius: 50%; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0% { opacity: 0.4; } 50% { opacity: 1; } 100% { opacity: 0.4; } }
        
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
        .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1.25rem; }
        .stat-label { color: var(--text-muted); font-size: 0.875rem; margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
        .stat-value { font-size: 2rem; font-weight: 700; }

        .section-title { font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; color: var(--text); }
        .table-container { background: var(--card-bg); border: 1px solid var(--border); border-radius: 0.75rem; overflow: hidden; margin-bottom: 2rem; }
        table { width: 100%; border-collapse: collapse; text-align: left; }
        th { background: #0f172a; padding: 0.875rem 1rem; font-size: 0.85rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; border-bottom: 1px solid var(--border); }
        td { padding: 0.875rem 1rem; border-bottom: 1px solid var(--border); font-size: 0.9rem; font-family: monospace; }
        tr:last-child td { border-bottom: none; }
        tr:hover td { background: rgba(255, 255, 255, 0.02); }

        .status-badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 0.375rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
        .status-pending { background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }
        .status-processing { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
        .status-completed { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
        .status-failed { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
        .status-dead { background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>⚡ QueueCTL Dashboard</h1>
            <div class="badge-live"><span class="pulse"></span> Auto-Refreshing (3s)</div>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Active Workers</div>
                <div class="stat-value" id="val-workers" style="color: var(--accent);">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Pending</div>
                <div class="stat-value" id="val-pending" style="color: var(--info);">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Processing</div>
                <div class="stat-value" id="val-processing" style="color: var(--warning);">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Completed</div>
                <div class="stat-value" id="val-completed" style="color: var(--success);">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Failed</div>
                <div class="stat-value" id="val-failed" style="color: var(--danger);">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">DLQ (Dead)</div>
                <div class="stat-value" id="val-dead" style="color: #c084fc;">0</div>
            </div>
        </div>

        <div class="section-title">Recent Jobs</div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Job ID</th>
                        <th>Command</th>
                        <th>State</th>
                        <th>Attempts</th>
                        <th>Created At</th>
                        <th>Worker</th>
                    </tr>
                </thead>
                <tbody id="jobs-body">
                    <tr><td colspan="6" style="text-align:center; color: var(--text-muted);">Loading jobs...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function updateDashboard() {
            try {
                const res = await fetch('/api/data');
                const data = await res.json();
                
                document.getElementById('val-workers').innerText = data.status.active_workers;
                document.getElementById('val-pending').innerText = data.status.counts.pending || 0;
                document.getElementById('val-processing').innerText = data.status.counts.processing || 0;
                document.getElementById('val-completed').innerText = data.status.counts.completed || 0;
                document.getElementById('val-failed').innerText = data.status.counts.failed || 0;
                document.getElementById('val-dead').innerText = data.status.counts.dead || 0;

                const tbody = document.getElementById('jobs-body');
                if (!data.jobs || data.jobs.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: var(--text-muted);">No jobs found in queue</td></tr>';
                    return;
                }

                tbody.innerHTML = data.jobs.map(j => `
                    <tr>
                        <td style="color: var(--accent); font-weight: 600;">${j.id}</td>
                        <td>${escapeHtml(j.command)}</td>
                        <td><span class="status-badge status-${j.state}">${j.state}</span></td>
                        <td>${j.attempts}/${j.max_retries}</td>
                        <td>${j.created_at}</td>
                        <td>${j.worker_id || '-'}</td>
                    </tr>
                `).join('');
            } catch (err) {
                console.error("Failed to fetch dashboard data:", err);
            }
        }

        function escapeHtml(str) {
            return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        }

        updateDashboard();
        setInterval(updateDashboard, 3000);
    </script>
</body>
</html>
"""

class DashboardHandler(http.server.BaseHTTPRequestHandler):
    qm: Optional[QueueManager] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))
        elif parsed.path == "/api/data":
            self.send_response(200)
            self.send_header("Content-Type", "json")
            self.end_headers()
            
            st = self.qm.status()
            jobs = [j.to_dict() for j in self.qm.list_jobs()]
            
            payload = {
                "status": st,
                "jobs": jobs[:50]  # Return top 50 recent jobs
            }
            self.wfile.write(json.dumps(payload).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress verbose server log output
        pass

def run_dashboard(port: int = 8080, db_path: Optional[str] = None):
    config = ConfigManager(db_path)
    qm = QueueManager(config=config)
    DashboardHandler.qm = qm

    server = socketserver.TCPServer(("", port), DashboardHandler)
    print(f"🚀 QueueCTL Dashboard running at http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
        server.server_close()
