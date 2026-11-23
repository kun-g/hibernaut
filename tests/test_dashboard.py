from __future__ import annotations

import ast
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from dashboard import DashboardConfig, DashboardServer, build_snapshot, resolve_config
from dashboard.snapshot import RemoteProbeCache, read_outbox_file, read_run_detail

DASHBOARD_SRC = Path(__file__).resolve().parents[1] / "src" / "dashboard"
STDLIB_NAMES: frozenset[str] = frozenset(sys.stdlib_module_names) | {"__future__"}


def _seed_hb_db(db_path: Path, run_dir: Path, workdir: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE event_inbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            inbox_id TEXT NOT NULL,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            session_id TEXT,
            correlation_key TEXT,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY,
            inbox_id TEXT NOT NULL,
            checkpoint TEXT,
            pending_request_id TEXT,
            updated_at TEXT NOT NULL,
            correlation_key TEXT,
            executor_id TEXT,
            executor_session_id TEXT,
            work_key TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            parent_session_id TEXT,
            task_link TEXT
        );
        CREATE TABLE worker_leases (
            inbox_id TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            expires_at REAL NOT NULL
        );
        CREATE TABLE open_runs (
            session_id TEXT PRIMARY KEY,
            inbox_id TEXT NOT NULL,
            executor_id TEXT NOT NULL,
            run_dir TEXT NOT NULL,
            event_ids_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            code_version TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE workspaces (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            work_key TEXT NOT NULL UNIQUE,
            repo_checkout TEXT NOT NULL,
            workdir TEXT NOT NULL,
            branch TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            inbox_id TEXT
        );
        CREATE TABLE deliveries (
            work_key TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            pr_url TEXT NOT NULL DEFAULT '',
            version TEXT NOT NULL DEFAULT '',
            merge_sha TEXT NOT NULL DEFAULT '',
            record_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            inbox_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            content TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            metadata_json TEXT NOT NULL,
            next_retry_at TEXT
        );
        CREATE TABLE terminal_bindings (
            session_id TEXT PRIMARY KEY,
            executor_id TEXT NOT NULL,
            agent TEXT NOT NULL,
            workdir TEXT NOT NULL,
            handle TEXT,
            incarnation_id TEXT,
            native_session_id TEXT,
            updated_at TEXT NOT NULL,
            peer_name TEXT,
            peer_pid INTEGER,
            peer_socket TEXT
        );
        CREATE TABLE peer_cursors (
            receiver TEXT NOT NULL,
            log TEXT NOT NULL,
            offset INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (receiver, log)
        );
        CREATE TABLE peer_messages (
            msg_id TEXT PRIMARY KEY,
            receiver TEXT NOT NULL,
            kind TEXT NOT NULL,
            from_addr TEXT,
            body TEXT NOT NULL,
            record_json TEXT NOT NULL,
            ts REAL NOT NULL,
            consumed_at TEXT
        );
        CREATE TABLE timers (
            timer_id TEXT PRIMARY KEY,
            inbox_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            due_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            recurrence_seconds INTEGER,
            session_id TEXT,
            status TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO event_inbox (event_id, inbox_id, source, external_id, occurred_at, payload_json, session_id, correlation_key, status, attempts, last_error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ev_1",
            "proj:demo",
            "sb:github",
            "gh:1",
            "2026-11-23T03:00:00+00:00",
            json.dumps({"subject": "Fix widget"}),
            "sess_run1",
            "github:acme/demo#1",
            "processing",
            1,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO event_inbox (event_id, inbox_id, source, external_id, occurred_at, payload_json, session_id, correlation_key, status, attempts, last_error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ev_2",
            "proj:demo:sub",
            "gdrive:task",
            "gdrive:abc:2026-11-23T03:01:00Z",
            "2026-11-23T03:01:00+00:00",
            json.dumps({"title": "Deploy follow-up", "drive_file_name": "deploy.md"}),
            None,
            "gdrive:abc",
            "pending",
            0,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO event_inbox (event_id, inbox_id, source, external_id, occurred_at, payload_json, session_id, correlation_key, status, attempts, last_error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ev_3",
            "proj:demo",
            "webhook",
            "wh:new",
            "2026-11-23T03:02:00+00:00",
            json.dumps({"subject": "Brand new task"}),
            None,
            "new-corr-key",
            "pending",
            0,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO sessions (session_id, inbox_id, checkpoint, pending_request_id, updated_at, correlation_key, executor_id, work_key, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "sess_run1",
            "proj:demo",
            None,
            None,
            "2026-11-23T03:00:01+00:00",
            "github:acme/demo#1",
            "sde",
            "github:acme/demo#1",
            "active",
        ),
    )
    conn.execute(
        "INSERT INTO sessions (session_id, inbox_id, checkpoint, pending_request_id, updated_at, correlation_key, executor_id, work_key, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "sess_susp1",
            "proj:demo:sub",
            json.dumps({"question": "Approve deployment?", "sde": {"stage": "needs_input", "requirements": "# Deploy job"}}),
            "req_42",
            "2026-11-23T02:55:00+00:00",
            "gdrive:abc",
            "sde",
            "gdrive:abc",
            "suspended",
        ),
    )
    conn.execute(
        "INSERT INTO worker_leases (inbox_id, worker_id, expires_at) VALUES (?, ?, ?)",
        ("proj:demo", "pool-1-worker-0", time.time() + 45.0),
    )
    conn.execute(
        "INSERT INTO open_runs (session_id, inbox_id, executor_id, run_dir, event_ids_json, started_at, code_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "sess_run1",
            "proj:demo",
            "sde",
            str(run_dir),
            json.dumps(["ev_1"]),
            "2026-11-23T03:00:01+00:00",
            "0.31.0",
        ),
    )
    conn.execute(
        "INSERT INTO workspaces (work_key, repo_checkout, workdir, branch, status, created_at, updated_at, inbox_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "github:acme/demo#1",
            str(workdir.parent),
            str(workdir),
            "hb/issue-1",
            "ready",
            "2026-11-23T02:50:00+00:00",
            "2026-11-23T03:00:00+00:00",
            "proj:demo",
        ),
    )
    conn.execute(
        "INSERT INTO deliveries (work_key, state, pr_url, version, merge_sha, record_json, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "github:acme/demo#1",
            "merged",
            "https://github.com/acme/demo/pull/10",
            "0.31.0",
            "abcdef123456",
            json.dumps({"branch": "hb/issue-1", "change_kind": "feature"}),
            "2026-11-23T02:55:00+00:00",
            "2026-11-23T03:05:00+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO outbox (message_id, inbox_id, created_at, content, status, attempts, metadata_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "msg_1",
            "proj:demo",
            "2026-11-23T03:05:01+00:00",
            "Delivered PR #10",
            "sent",
            1,
            json.dumps({"outcome": "done"}),
        ),
    )
    conn.execute(
        "INSERT INTO terminal_bindings (session_id, executor_id, agent, workdir, handle, updated_at, peer_name, peer_pid, peer_socket) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "sess_run1",
            "sde",
            "claude",
            str(workdir),
            "term_1",
            "2026-11-23T03:00:02+00:00",
            "sde-demo",
            999999,
            "/tmp/fake.sock",
        ),
    )
    conn.execute(
        "INSERT INTO peer_cursors (receiver, log, offset, updated_at) VALUES (?, ?, ?, ?)",
        ("hibernaut", "/tmp/inbox.jsonl", 128, "2026-11-23T03:01:00+00:00"),
    )
    conn.execute(
        "INSERT INTO peer_messages (msg_id, receiver, kind, from_addr, body, record_json, ts, consumed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "pm_1",
            "hibernaut",
            "user",
            "uds:/tmp/1.sock",
            json.dumps({"task_id": "sess_run1", "status": "done", "summary": "All green"}),
            "{}",
            1795400000.0,
            "2026-11-23T03:02:00+00:00",
        ),
    )
    conn.commit()
    conn.close()


def _seed_ledger_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            inbox_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            executor_id TEXT NOT NULL,
            runner_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            outcome_kind TEXT NOT NULL,
            emit_status TEXT,
            correlation_key TEXT,
            trace_id TEXT,
            work_key TEXT,
            parent_session_id TEXT,
            replay_of_run_id TEXT,
            executor_session_id TEXT,
            error_kind TEXT,
            error_summary TEXT,
            exit_code INTEGER,
            log_ref TEXT,
            usage_json TEXT
        );
        CREATE TABLE audit_events (
            audit_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            service TEXT NOT NULL,
            stage TEXT NOT NULL,
            outcome TEXT NOT NULL,
            severity TEXT NOT NULL,
            correlation_key TEXT,
            trace_id TEXT,
            scope TEXT,
            event_id TEXT,
            external_id TEXT,
            session_id TEXT,
            run_id TEXT,
            task_id TEXT,
            actor TEXT,
            duration_ms INTEGER,
            detail_json TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO runs (run_id, schema_version, inbox_id, session_id, executor_id, runner_id, started_at, ended_at, duration_ms, outcome_kind, emit_status, usage_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "run_1",
            "1",
            "proj:demo",
            "sess_run1",
            "sde",
            "local",
            "2026-11-23T03:00:00+00:00",
            "2026-11-23T03:01:00+00:00",
            60000,
            "emit",
            "done",
            json.dumps({"input_tokens": 100}),
        ),
    )
    conn.execute(
        "INSERT INTO audit_events (audit_id, schema_version, occurred_at, service, stage, outcome, severity, scope, detail_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "aud_1",
            "1",
            "2026-11-23T03:00:00+00:00",
            "switchboard",
            "inbound",
            "accepted",
            "info",
            "proj:demo",
            json.dumps({"source": "github"}),
        ),
    )
    conn.commit()
    conn.close()


def _start_mock_hub(expected_token: str) -> tuple[ThreadingHTTPServer, str]:
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return None

        def do_GET(self) -> None:
            if self.path in ("/switchboard/healthz", "/taskboard/healthz"):
                body = json.dumps({"status": "ok"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def do_POST(self) -> None:
            if self.path != "/taskboard/mcp":
                self.send_response(404)
                self.end_headers()
                return
            if self.headers.get("Authorization") != f"Bearer {expected_token}":
                self.send_response(401)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", "0"))
            req = json.loads(self.rfile.read(length).decode("utf-8"))
            method = req.get("method")
            if method == "initialize":
                resp: dict[str, Any] = {
                    "jsonrpc": "2.0",
                    "id": req.get("id"),
                    "result": {"serverInfo": {"name": "taskboard", "version": "0.1.0"}},
                }
            else:
                resp = {
                    "jsonrpc": "2.0",
                    "id": req.get("id"),
                    "result": {
                        "structuredContent": {
                            "tasks": [
                                {
                                    "task_id": "t_1",
                                    "status": "ready",
                                    "priority": 1,
                                    "scope": "proj:demo",
                                    "title": "Ship dashboard",
                                    "assignee": "agent:sde",
                                    "incomplete_blocker_count": 0,
                                    "updated_at": "2026-11-23T03:00:00+00:00",
                                }
                            ]
                        }
                    },
                }
            raw = json.dumps(resp).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    t.start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def test_dashboard_zero_business_dependencies() -> None:
    for py_file in DASHBOARD_SRC.rglob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            else:
                mods = []
            for mod in mods:
                top = mod.split(".")[0]
                assert top in STDLIB_NAMES or top == "dashboard", (
                    f"dashboard/{py_file.name} imported forbidden module '{mod}'"
                )


def test_resolve_config_reads_hibernaut_toml(tmp_path: Path) -> None:
    cfg_file = tmp_path / "hibernaut.toml"
    cfg_file.write_text(
        """
[runtime]
db_path = "./state/hb.db"

[channels]
outbox_dir = "./reports"

[switchboard]
enabled = true
relay_url = "wss://hub.example.test/switchboard/ws/relay"
""",
        encoding="utf-8",
    )
    deploy_dir = tmp_path / "deploy"
    cfg = resolve_config(config_path=cfg_file, hub_deploy_dir=deploy_dir)
    assert cfg.hb_db_path == (tmp_path / "state/hb.db").resolve()
    assert cfg.ledger_db_path == (tmp_path / "state/ledger.db").resolve()
    assert cfg.outbox_dir == (tmp_path / "reports").resolve()
    assert cfg.runs_dir == (tmp_path / "state/runs").resolve()
    assert cfg.hub_base_url == "https://hub.example.test"
    assert cfg.sb_config_path == deploy_dir.resolve() / "switchboard.toml"


def test_snapshot_and_http_server_end_to_end(tmp_path: Path) -> None:
    secret_token = "tb-super-secret-token-xyz"
    hub_srv, hub_url = _start_mock_hub(secret_token)
    try:
        hb_root = tmp_path / "hb"
        hb_root.mkdir()
        workdir = tmp_path / "wt/hb-issue-1"
        workdir.mkdir(parents=True)
        run_dir = hb_root / "runs/sess_run1/run_abc"
        (run_dir / "turns/turn-1").mkdir(parents=True)
        (run_dir / "state.json").write_text(
            json.dumps(
                {
                    "phase": "develop",
                    "prompt": "Implement feature",
                    "state": {
                        "stage": "developing",
                        "workdir": str(workdir),
                        "requirements": "# Fix widget title\nDetails here",
                        "summary": "Working on tests",
                        "handbacks": 1,
                    },
                }
            ),
            encoding="utf-8",
        )
        outbox_dir = hb_root / "outbox"
        outbox_dir.mkdir()
        (outbox_dir / "20261123-120000.000.md").write_text("# Widget Report\nAll good.", encoding="utf-8")
        (tmp_path / "secret.md").write_text("TOP SECRET", encoding="utf-8")

        (hb_root / "hibernaut.toml").write_text(
            f"""
[runtime]
db_path = "./hibernaut.db"
max_concurrency = 4
new_event_policy = "defer"

[channels]
host = "127.0.0.1"
port = 8080
outbox_dir = "outbox"

[switchboard]
enabled = true
mode = "relay"
mcp_url = "{hub_url}/switchboard/mcp"
""",
            encoding="utf-8",
        )
        deploy_dir = tmp_path / "hub-deploy"
        deploy_dir.mkdir()
        (deploy_dir / "switchboard.toml").write_text(
            """
[github]
require_label = "hibernaut"
[routing.github_repos]
"acme/demo" = "proj:demo"
""",
            encoding="utf-8",
        )
        (deploy_dir / "taskboard.toml").write_text(
            """
[intake]
auto_ready_sources = ["github"]
""",
            encoding="utf-8",
        )
        (deploy_dir / "taskboard.env").write_text(
            f"TASKBOARD_MCP_TOKEN={secret_token}\n",
            encoding="utf-8",
        )

        _seed_hb_db(hb_root / "hibernaut.db", run_dir, workdir)
        _seed_ledger_db(hb_root / "ledger.db")

        cfg: DashboardConfig = resolve_config(
            config_path=hb_root / "hibernaut.toml",
            hub_deploy_dir=deploy_dir,
        )
        snap = build_snapshot(cfg, force_remote=True, remote_cache=RemoteProbeCache(ttl_seconds=4.0))
        serialized = json.dumps(snap)
        assert secret_token not in serialized

        assert snap["hb"]["db_ok"] is True
        assert len(snap["hb"]["active_runs"]) == 1
        assert snap["hb"]["active_runs"][0]["title"] == "Fix widget title"
        assert snap["hb"]["active_runs"][0]["branch"] == "hb/issue-1"
        assert len(snap["hb"]["suspended_sessions"]) == 1
        assert snap["hb"]["suspended_sessions"][0]["pending_request_id"] == "req_42"
        assert len(snap["hb"]["peer_bindings"]) == 1
        assert len(snap["hb"]["peer_cursors"]) == 1
        assert len(snap["hb"]["peer_messages"]) == 1
        assert snap["hb"]["ledger"]["db_ok"] is True
        assert len(snap["hb"]["ledger"]["runs"]) == 1
        assert len(snap["hb"]["ledger"]["audits"]) == 1
        assert snap["tb"]["health"]["ok"] is True
        assert snap["tb"]["version"] == "0.1.0"
        assert snap["tb"]["tasks_total"] == 1
        assert snap["sb"]["health"]["ok"] is True
        assert len(snap["sb"]["observed_events"]) == 1

        # Verify event -> related_sessions linkage: populated only for processing events
        ev_by_id = {e["event_id"]: e for e in snap["hb"]["events"]}
        ev1_sessions = ev_by_id["ev_1"]["related_sessions"]
        assert len(ev1_sessions) == 1
        assert ev1_sessions[0]["session_id"] == "sess_run1"
        assert ev1_sessions[0]["status"] == "active"
        assert ev1_sessions[0]["match_by"] == "session_id"
        assert ev1_sessions[0]["has_open_run"] is True
        assert ev1_sessions[0]["open_run"]["branch"] == "hb/issue-1"

        assert ev_by_id["ev_2"]["drive_file_name"] == "deploy.md"
        assert ev_by_id["ev_2"]["related_sessions"] == []
        assert ev_by_id["ev_3"]["related_sessions"] == []

        # Path traversal checks
        assert read_outbox_file(cfg, "../secret.md") is None
        assert read_outbox_file(cfg, "20261123-120000.000.md") == "# Widget Report\nAll good."
        assert read_run_detail(cfg, "../hb") is None
        detail = read_run_detail(cfg, "sess_run1")
        assert detail is not None
        assert detail["turns"] == ["turn-1"]

        # HTTP server smoke test
        dash = DashboardServer(cfg, host="127.0.0.1", port=0)
        dash.start()
        try:
            for route in ("/", "/details", "/assets/style.css", "/assets/common.js"):
                with urllib.request.urlopen(f"{dash.base_url}{route}", timeout=5) as resp:
                    assert resp.status == 200
                    assert len(resp.read()) > 100

            with urllib.request.urlopen(f"{dash.base_url}/api/snapshot?force=1", timeout=5) as resp:
                api_snap = json.loads(resp.read().decode("utf-8"))
                assert api_snap["tb"]["tasks_total"] == 1
                assert secret_token not in json.dumps(api_snap)

            with urllib.request.urlopen(f"{dash.base_url}/api/run/sess_run1", timeout=5) as resp:
                run_body = json.loads(resp.read().decode("utf-8"))
                assert run_body["state_json"]["phase"] == "develop"

            with urllib.request.urlopen(
                f"{dash.base_url}/api/outbox/20261123-120000.000.md", timeout=5
            ) as resp:
                ob_body = json.loads(resp.read().decode("utf-8"))
                assert "Widget Report" in ob_body["content"]

            try:
                urllib.request.urlopen(f"{dash.base_url}/api/outbox/..%2Fsecret.md", timeout=5)
                raise AssertionError("expected 404 for traversal")
            except urllib.error.HTTPError as exc:
                assert exc.code == 404
        finally:
            dash.stop()
    finally:
        hub_srv.shutdown()
        hub_srv.server_close()
