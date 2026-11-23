from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from dashboard.config import DashboardConfig, safe_read_toml


class RemoteProbeCache:
    def __init__(self, ttl_seconds: float = 4.0) -> None:
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._ts = 0.0
        self._data: dict[str, Any] | None = None

    def get(self, *, force: bool = False) -> dict[str, Any] | None:
        if force:
            return None
        with self._lock:
            if self._data is not None and (time.monotonic() - self._ts) < self._ttl:
                return self._data
        return None

    def put(self, data: dict[str, Any]) -> None:
        with self._lock:
            self._ts = time.monotonic()
            self._data = data


def _iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _read_tb_token(env_file: Path) -> str:
    token = os.environ.get("TASKBOARD_MCP_TOKEN", "").strip()
    if token:
        return token
    if not env_file.is_file():
        return ""
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("TASKBOARD_MCP_TOKEN="):
                return stripped.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _open_ro_sqlite(path: Path) -> sqlite3.Connection | None:
    if not path.is_file():
        return None
    try:
        # Strictly read-only URI so the dashboard never writes or contends with live WAL writers.
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _parse_json(raw: str | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _as_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _extract_heading(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            return s.lstrip("#").strip()[:120]
        return s[:120]
    return ""


def _read_hb_version(hb_root: Path) -> str:
    init_py = hb_root / "src/agent_runtime/__init__.py"
    if not init_py.is_file():
        return ""
    try:
        match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', init_py.read_text(encoding="utf-8"))
        if match:
            return match.group(1)
    except Exception:
        pass
    return ""


def _collect_processes() -> dict[str, Any]:
    procs: list[dict[str, Any]] = []
    by_pid: dict[int, dict[str, Any]] = {}
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid=,ppid=,pcpu=,rss=,etime=,command="],
            text=True,
            timeout=3,
        )
        for line in out.splitlines():
            parts = line.strip().split(None, 5)
            if len(parts) < 6:
                continue
            try:
                pid = int(parts[0])
                ppid = int(parts[1])
                cpu = float(parts[2])
                rss_kb = int(parts[3])
            except ValueError:
                continue
            item: dict[str, Any] = {
                "pid": pid,
                "ppid": ppid,
                "cpu": cpu,
                "mem_mb": round(rss_kb / 1024.0, 1),
                "etime": parts[4],
                "command": parts[5],
            }
            procs.append(item)
            by_pid[pid] = item
    except Exception:
        pass

    daemon: dict[str, Any] | None = None
    supervisor: dict[str, Any] | None = None
    ccpeer: dict[str, Any] | None = None
    workers: list[dict[str, Any]] = []
    for p in procs:
        cmd = str(p["command"])
        if "agent_runtime.app.main" in cmd and "python" in cmd:
            daemon = p
        elif "scripts/run_updating.py" in cmd and "hibernaut" in cmd:
            supervisor = p
        elif "ccpeer serve" in cmd and "hibernaut" in cmd:
            ccpeer = p
        elif "claude_gate.py" in cmd and "hibernaut" in cmd:
            workers.append(p)

    relay_connected = False
    relay_peer: str | None = None
    http_listening = False
    if daemon is not None:
        try:
            lsof_out = subprocess.check_output(
                ["lsof", "-a", "-p", str(daemon["pid"]), "-iTCP", "-n", "-P"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=3,
            )
            for line in lsof_out.splitlines():
                if "(LISTEN)" in line:
                    http_listening = True
                if "(ESTABLISHED)" in line and ":443" in line:
                    relay_connected = True
                    tokens = line.split()
                    if len(tokens) >= 9:
                        relay_peer = tokens[-2]
        except Exception:
            pass

    return {
        "by_pid": by_pid,
        "daemon": (
            {
                "pid": daemon["pid"],
                "ppid": daemon["ppid"],
                "etime": daemon["etime"],
                "cpu": daemon["cpu"],
                "mem_mb": daemon["mem_mb"],
                "http_listening": http_listening,
                "relay_connected": relay_connected,
                "relay_peer": relay_peer,
            }
            if daemon
            else None
        ),
        "supervisor": (
            {
                "pid": supervisor["pid"],
                "etime": supervisor["etime"],
                "cpu": supervisor["cpu"],
                "mem_mb": supervisor["mem_mb"],
            }
            if supervisor
            else None
        ),
        "ccpeer": (
            {
                "pid": ccpeer["pid"],
                "etime": ccpeer["etime"],
                "cpu": ccpeer["cpu"],
                "mem_mb": ccpeer["mem_mb"],
            }
            if ccpeer
            else None
        ),
        "gate_workers_count": len(workers),
    }


def _http_get_json(url: str, timeout: float = 4.0) -> dict[str, Any]:
    t0 = time.monotonic()
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            latency_ms = round((time.monotonic() - t0) * 1000)
            return {
                "ok": resp.status == 200,
                "status_code": resp.status,
                "latency_ms": latency_ms,
                "body": _parse_json(body) or body[:200],
                "error": None,
            }
    except urllib.error.HTTPError as exc:
        return {
            "ok": False,
            "status_code": exc.code,
            "latency_ms": round((time.monotonic() - t0) * 1000),
            "body": None,
            "error": f"HTTP {exc.code}",
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": round((time.monotonic() - t0) * 1000),
            "body": None,
            "error": str(exc),
        }


def _mcp_call(
    url: str, token: str, method: str, params: dict[str, Any], timeout: float = 6.0
) -> dict[str, Any]:
    t0 = time.monotonic()
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw_data = json.loads(resp.read().decode("utf-8"))
            parsed_data = _as_dict(raw_data)
            return {
                "ok": isinstance(raw_data, dict) and "result" in parsed_data,
                "latency_ms": round((time.monotonic() - t0) * 1000),
                "result": parsed_data.get("result") if parsed_data else None,
                "error": parsed_data.get("error") if isinstance(raw_data, dict) else "invalid response",
            }
    except urllib.error.HTTPError as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - t0) * 1000),
            "result": None,
            "error": f"HTTP {exc.code}",
        }
    except Exception as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - t0) * 1000),
            "result": None,
            "error": str(exc),
        }


def _probe_remote(
    cfg: DashboardConfig,
    hb_cfg: dict[str, Any],
    cache: RemoteProbeCache | None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    if cache is not None:
        cached = cache.get(force=force)
        if cached is not None:
            return cached

    hub_base = cfg.hub_base_url
    tb_token = _read_tb_token(cfg.tb_env_path)
    gdrive_cfg = _as_dict(hb_cfg.get("gdrive"))
    gdrive_url = str(gdrive_cfg.get("proxy_url", "")).rstrip("/")

    if not hub_base:
        unconfigured: dict[str, Any] = {
            "ok": False,
            "status_code": None,
            "latency_ms": None,
            "body": None,
            "error": "hub_url not configured",
        }
        gd_health = _http_get_json(f"{gdrive_url}/healthz", 2.0) if gdrive_url else None
        data: dict[str, Any] = {
            "hub_base": "",
            "sb_health": unconfigured,
            "tb_health": unconfigured,
            "tb_version": None,
            "tb_token_present": bool(tb_token),
            "tb_tasks": [],
            "tb_tasks_error": "hub_url not configured",
            "tb_mcp_latency_ms": None,
            "gdrive_health": gd_health,
        }
        if cache is not None:
            cache.put(data)
        return data

    sb_health_url = f"{hub_base}/switchboard/healthz"
    tb_health_url = f"{hub_base}/taskboard/healthz"
    tb_mcp_url = f"{hub_base}/taskboard/mcp"

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        f_sb = pool.submit(_http_get_json, sb_health_url, 5.0)
        f_tb = pool.submit(_http_get_json, tb_health_url, 5.0)
        f_tb_init = (
            pool.submit(
                _mcp_call,
                tb_mcp_url,
                tb_token,
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "hibernaut-dashboard", "version": "0.1"},
                },
                6.0,
            )
            if tb_token
            else None
        )
        f_tb_tasks = (
            pool.submit(
                _mcp_call,
                tb_mcp_url,
                tb_token,
                "tools/call",
                {"name": "tb_list_tasks", "arguments": {}},
                8.0,
            )
            if tb_token
            else None
        )
        f_gd = pool.submit(_http_get_json, f"{gdrive_url}/healthz", 2.0) if gdrive_url else None

        sb_health = f_sb.result()
        tb_health = f_tb.result()
        tb_init = f_tb_init.result() if f_tb_init else {"ok": False, "error": "TASKBOARD_MCP_TOKEN missing"}
        tb_tasks_resp = (
            f_tb_tasks.result() if f_tb_tasks else {"ok": False, "error": "TASKBOARD_MCP_TOKEN missing"}
        )
        gd_health = f_gd.result() if f_gd else None

    tasks: list[dict[str, Any]] = []
    tb_tasks_error = tb_tasks_resp.get("error")
    tb_res = _as_dict(tb_tasks_resp.get("result"))
    if tb_tasks_resp.get("ok") and tb_res:
        sc = _as_dict(tb_res.get("structuredContent"))
        raw_tasks = sc.get("tasks")
        if isinstance(raw_tasks, list):
            tasks = [cast(dict[str, Any], t) for t in cast(list[Any], raw_tasks) if isinstance(t, dict)]

    tb_version: str | None = None
    init_res = _as_dict(tb_init.get("result"))
    if tb_init.get("ok") and init_res:
        srv_info = _as_dict(init_res.get("serverInfo"))
        if srv_info.get("version") is not None:
            tb_version = str(srv_info["version"])

    data = {
        "hub_base": hub_base,
        "sb_health": sb_health,
        "tb_health": tb_health,
        "tb_version": tb_version,
        "tb_token_present": bool(tb_token),
        "tb_tasks": tasks,
        "tb_tasks_error": tb_tasks_error,
        "tb_mcp_latency_ms": tb_tasks_resp.get("latency_ms"),
        "gdrive_health": gd_health,
    }
    if cache is not None:
        cache.put(data)
    return data


def _read_run_state(run_dir_str: str) -> dict[str, Any]:
    run_dir = Path(run_dir_str)
    state_file = run_dir / "state.json"
    if not state_file.is_file():
        candidates = sorted(
            run_dir.glob("run_*/state.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            state_file = candidates[0]
        else:
            return {}
    try:
        return _as_dict(json.loads(state_file.read_text(encoding="utf-8")))
    except Exception:
        return {}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _read_hb_data(cfg: DashboardConfig, proc_info: dict[str, Any]) -> dict[str, Any]:
    by_pid: dict[int, dict[str, Any]] = proc_info["by_pid"]
    result: dict[str, Any] = {
        "db_ok": False,
        "counts": {},
        "active_runs": [],
        "suspended_sessions": [],
        "events": [],
        "sb_events": [],
        "deliveries": [],
        "workspaces": [],
        "outbox": [],
        "outbox_files": [],
        "peer_messages": [],
        "peer_bindings": [],
        "peer_cursors": [],
        "timers": [],
        "ledger": {"db_ok": False, "runs": [], "audits": [], "stats": []},
    }

    conn = _open_ro_sqlite(cfg.hb_db_path)
    if conn is not None:
        try:
            result["db_ok"] = True

            def count_group(table: str, col: str) -> dict[str, int]:
                if not _table_exists(conn, table):
                    return {}
                rows = conn.execute(f"SELECT {col}, COUNT(*) AS c FROM {table} GROUP BY {col}").fetchall()
                return {str(r[col]): int(r["c"]) for r in rows}

            def count_all(table: str) -> int:
                if not _table_exists(conn, table):
                    return 0
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                return int(row[0]) if row else 0

            event_counts = count_group("event_inbox", "status")
            session_counts = count_group("sessions", "status")
            workspace_counts = count_group("workspaces", "status")
            delivery_counts = count_group("deliveries", "state")
            outbox_counts = count_group("outbox", "status")
            timer_counts = count_group("timers", "status")
            lease_count = count_all("worker_leases")
            open_run_count = count_all("open_runs")

            sb_source_counts: dict[str, int] = {}
            all_source_counts: dict[str, int] = {}
            if _table_exists(conn, "event_inbox"):
                sb_source_counts = {
                    str(r["source"]): int(r["c"])
                    for r in conn.execute(
                        "SELECT source, COUNT(*) AS c FROM event_inbox WHERE source LIKE 'sb:%' GROUP BY source"
                    ).fetchall()
                }
                all_source_counts = {
                    str(r["source"]): int(r["c"])
                    for r in conn.execute(
                        "SELECT source, COUNT(*) AS c FROM event_inbox GROUP BY source ORDER BY c DESC"
                    ).fetchall()
                }

            result["counts"] = {
                "events": event_counts,
                "events_total": sum(event_counts.values()),
                "sessions": session_counts,
                "workspaces": workspace_counts,
                "deliveries": delivery_counts,
                "outbox": outbox_counts,
                "timers": timer_counts,
                "leases": lease_count,
                "open_runs": open_run_count,
                "sb_sources": sb_source_counts,
                "all_sources": all_source_counts,
            }

            leases: dict[str, dict[str, Any]] = {}
            if _table_exists(conn, "worker_leases"):
                leases = {
                    str(r["inbox_id"]): {"worker_id": r["worker_id"], "expires_at": float(r["expires_at"])}
                    for r in conn.execute("SELECT inbox_id, worker_id, expires_at FROM worker_leases").fetchall()
                }

            terminals: dict[str, dict[str, Any]] = {}
            if _table_exists(conn, "terminal_bindings"):
                cols = {
                    str(r["name"])
                    for r in conn.execute("PRAGMA table_info(terminal_bindings)").fetchall()
                }
                has_peer_socket = "peer_socket" in cols
                sock_expr = "peer_socket" if has_peer_socket else "NULL AS peer_socket"
                tb_all = conn.execute(
                    f"SELECT session_id, executor_id, agent, workdir, handle, peer_name, peer_pid, {sock_expr}, updated_at "
                    "FROM terminal_bindings ORDER BY updated_at DESC"
                ).fetchall()
                for idx, r in enumerate(tb_all):
                    row_d = dict(r)
                    terminals[str(r["session_id"])] = row_d
                    if idx < 20:
                        pid = r["peer_pid"]
                        proc = by_pid.get(int(pid)) if pid is not None else None
                        result["peer_bindings"].append(
                            {
                                "session_id": r["session_id"],
                                "executor_id": r["executor_id"],
                                "agent": r["agent"],
                                "workdir": r["workdir"],
                                "handle": r["handle"],
                                "peer_name": r["peer_name"],
                                "peer_pid": pid,
                                "peer_alive": proc is not None,
                                "peer_cpu": proc["cpu"] if proc else None,
                                "peer_mem_mb": proc["mem_mb"] if proc else None,
                                "peer_socket": r["peer_socket"],
                                "updated_at": r["updated_at"],
                            }
                        )

            if _table_exists(conn, "peer_cursors"):
                pc_rows = conn.execute(
                    "SELECT receiver, log, offset, updated_at FROM peer_cursors ORDER BY updated_at DESC"
                ).fetchall()
                result["peer_cursors"] = [dict(r) for r in pc_rows]

            ws_by_dir: dict[str, dict[str, Any]] = {}
            if _table_exists(conn, "workspaces"):
                ws_rows = conn.execute(
                    "SELECT work_key, inbox_id, repo_checkout, workdir, branch, status, created_at, updated_at "
                    "FROM workspaces ORDER BY updated_at DESC"
                ).fetchall()
                for w in ws_rows:
                    wd = dict(w)
                    wd["exists_on_disk"] = Path(str(wd["workdir"])).exists()
                    ws_by_dir[str(wd["workdir"])] = wd
                    result["workspaces"].append(wd)

            open_run_sids: set[str] = set()
            now_ts = time.time()
            if _table_exists(conn, "open_runs") and _table_exists(conn, "sessions"):
                open_rows = conn.execute(
                    "SELECT o.session_id, o.inbox_id, o.executor_id, o.run_dir, o.event_ids_json, o.started_at, o.code_version, "
                    "s.status AS session_status, s.work_key, s.correlation_key, s.pending_request_id "
                    "FROM open_runs o LEFT JOIN sessions s ON o.session_id = s.session_id "
                    "ORDER BY o.started_at DESC"
                ).fetchall()
                for row in open_rows:
                    sid = str(row["session_id"])
                    open_run_sids.add(sid)
                    inbox_id = str(row["inbox_id"])
                    run_state = _read_run_state(str(row["run_dir"]))
                    inner = _as_dict(run_state.get("state"))
                    req_text = str(inner.get("requirements") or "")
                    title = _extract_heading(req_text) or str(
                        row["work_key"] or row["correlation_key"] or inbox_id
                    )
                    workdir = str(inner.get("workdir") or "")
                    tb = terminals.get(sid, {})
                    if not workdir and tb.get("workdir"):
                        workdir = str(tb["workdir"])
                    ws = ws_by_dir.get(workdir)
                    peer_pid = tb.get("peer_pid")
                    proc = by_pid.get(int(peer_pid)) if peer_pid is not None else None
                    child_cmds: list[str] = []
                    if peer_pid is not None:
                        for p in by_pid.values():
                            if p["ppid"] == int(peer_pid):
                                child_cmds.append(str(p["command"])[:100])
                    lease = leases.get(inbox_id)
                    lease_ttl = round(float(lease["expires_at"]) - now_ts) if lease else None

                    started_at = str(row["started_at"])
                    elapsed_s: int | None = None
                    try:
                        dt = datetime.fromisoformat(started_at)
                        elapsed_s = max(
                            0, int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
                        )
                    except Exception:
                        pass

                    result["active_runs"].append(
                        {
                            "session_id": sid,
                            "inbox_id": inbox_id,
                            "executor_id": row["executor_id"],
                            "title": title,
                            "phase": run_state.get("phase") or "running",
                            "stage": inner.get("stage") or "",
                            "review_status": inner.get("review_status") or "",
                            "handbacks": int(inner.get("handbacks") or 0),
                            "workdir": workdir,
                            "branch": ws["branch"] if ws else "",
                            "started_at": started_at,
                            "elapsed_s": elapsed_s,
                            "code_version": row["code_version"],
                            "worker_id": lease["worker_id"] if lease else None,
                            "lease_ttl_s": lease_ttl,
                            "agent": tb.get("agent"),
                            "terminal_handle": tb.get("handle"),
                            "peer_pid": peer_pid,
                            "peer_alive": proc is not None,
                            "peer_cpu": proc["cpu"] if proc else None,
                            "peer_mem_mb": proc["mem_mb"] if proc else None,
                            "peer_etime": proc["etime"] if proc else None,
                            "child_cmds": child_cmds[:3],
                            "summary": str(inner.get("summary") or "")[:360],
                            "prompt_preview": str(run_state.get("prompt") or "")[:280],
                            "correlation_key": row["correlation_key"],
                            "work_key": row["work_key"],
                            "event_ids": _parse_json(row["event_ids_json"]) or [],
                        }
                    )

            if _table_exists(conn, "sessions"):
                susp_rows = conn.execute(
                    "SELECT session_id, inbox_id, pending_request_id, checkpoint, correlation_key, executor_id, work_key, updated_at "
                    "FROM sessions WHERE status = 'suspended' ORDER BY updated_at DESC LIMIT 20"
                ).fetchall()
                for r in susp_rows:
                    sid = str(r["session_id"])
                    cp = _parse_json(r["checkpoint"])
                    cp_dict = _as_dict(cp)
                    sde_cp = _as_dict(cp_dict.get("sde"))
                    req_text = str(sde_cp.get("requirements") or "")
                    title = _extract_heading(req_text) or str(
                        r["work_key"] or r["correlation_key"] or r["inbox_id"]
                    )
                    question = str(
                        cp_dict.get("question")
                        or cp_dict.get("prompt")
                        or sde_cp.get("review_note")
                        or sde_cp.get("summary")
                        or ""
                    )
                    tb = terminals.get(sid, {})
                    peer_pid = tb.get("peer_pid")
                    result["suspended_sessions"].append(
                        {
                            "session_id": sid,
                            "inbox_id": r["inbox_id"],
                            "pending_request_id": r["pending_request_id"],
                            "executor_id": r["executor_id"],
                            "title": title,
                            "stage": str(sde_cp.get("stage") or "suspended"),
                            "question": question[:320],
                            "work_key": r["work_key"],
                            "correlation_key": r["correlation_key"],
                            "updated_at": r["updated_at"],
                            "also_open_run": sid in open_run_sids,
                            "peer_alive": bool(peer_pid is not None and int(peer_pid) in by_pid),
                        }
                    )

            if _table_exists(conn, "event_inbox"):
                ev_rows = conn.execute(
                    "SELECT seq, event_id, inbox_id, source, external_id, occurred_at, payload_json, session_id, "
                    "correlation_key, status, attempts, last_error "
                    "FROM event_inbox ORDER BY seq DESC LIMIT 50"
                ).fetchall()
                for r in ev_rows:
                    payload = _as_dict(_parse_json(r["payload_json"]))
                    subject = ""
                    if payload:
                        subject = str(
                            payload.get("subject")
                            or payload.get("title")
                            or payload.get("text")
                            or payload.get("body")
                            or payload.get("message")
                            or payload.get("decision")
                            or ""
                        )
                        if not subject and isinstance(payload.get("approval_request_id"), str):
                            subject = (
                                f"approval {payload['approval_request_id']} -> approved={payload.get('approved')}"
                            )
                    subject = " ".join(subject.split())[:140]
                    ev: dict[str, Any] = {
                        "seq": r["seq"],
                        "event_id": r["event_id"],
                        "inbox_id": r["inbox_id"],
                        "source": r["source"],
                        "external_id": r["external_id"],
                        "occurred_at": r["occurred_at"],
                        "session_id": r["session_id"],
                        "correlation_key": r["correlation_key"],
                        "status": r["status"],
                        "attempts": r["attempts"],
                        "last_error": r["last_error"],
                        "subject": subject,
                    }
                    result["events"].append(ev)
                    if str(r["source"]).startswith("sb:"):
                        result["sb_events"].append(ev)

            if _table_exists(conn, "deliveries"):
                deliv_rows = conn.execute(
                    "SELECT work_key, state, pr_url, version, merge_sha, record_json, created_at, updated_at "
                    "FROM deliveries ORDER BY updated_at DESC LIMIT 30"
                ).fetchall()
                for r in deliv_rows:
                    rec = _as_dict(_parse_json(r["record_json"]))
                    result["deliveries"].append(
                        {
                            "work_key": r["work_key"],
                            "state": r["state"],
                            "pr_url": r["pr_url"],
                            "version": r["version"],
                            "merge_sha": str(r["merge_sha"] or "")[:10],
                            "branch": rec.get("branch", ""),
                            "change_kind": rec.get("change_kind", ""),
                            "bump": rec.get("bump", ""),
                            "title": rec.get("pr_title") or rec.get("title") or "",
                            "created_at": r["created_at"],
                            "updated_at": r["updated_at"],
                        }
                    )

            if _table_exists(conn, "outbox"):
                out_rows = conn.execute(
                    "SELECT seq, message_id, inbox_id, created_at, content, status, attempts, last_error, metadata_json, next_retry_at "
                    "FROM outbox ORDER BY seq DESC LIMIT 25"
                ).fetchall()
                for r in out_rows:
                    meta = _as_dict(_parse_json(r["metadata_json"]))
                    result["outbox"].append(
                        {
                            "seq": r["seq"],
                            "message_id": r["message_id"],
                            "inbox_id": r["inbox_id"],
                            "created_at": r["created_at"],
                            "status": r["status"],
                            "attempts": r["attempts"],
                            "last_error": r["last_error"],
                            "next_retry_at": r["next_retry_at"],
                            "outcome": meta.get("outcome") or meta.get("status") or "",
                            "preview": " ".join(str(r["content"] or "").split())[:180],
                        }
                    )

            if _table_exists(conn, "peer_messages"):
                pm_rows = conn.execute(
                    "SELECT msg_id, receiver, kind, from_addr, body, ts, consumed_at "
                    "FROM peer_messages ORDER BY ts DESC LIMIT 20"
                ).fetchall()
                for r in pm_rows:
                    body_raw = str(r["body"] or "").strip()
                    body_parsed = _parse_json(body_raw)
                    if body_parsed is None and body_raw.startswith("'") and body_raw.endswith("'"):
                        body_parsed = _parse_json(body_raw[1:-1])
                    body_dict = _as_dict(body_parsed)
                    task_id = ""
                    status = ""
                    summary = ""
                    if body_dict:
                        task_id = str(body_dict.get("task_id") or "")
                        status = str(body_dict.get("status") or "")
                        summary = str(body_dict.get("summary") or body_dict.get("question") or "")[:160]
                    else:
                        summary = body_raw[:160]
                    result["peer_messages"].append(
                        {
                            "msg_id": r["msg_id"],
                            "kind": r["kind"],
                            "from_addr": r["from_addr"],
                            "task_id": task_id,
                            "status": status,
                            "summary": summary,
                            "ts_iso": datetime.fromtimestamp(float(r["ts"]), timezone.utc)
                            .astimezone()
                            .isoformat(timespec="seconds"),
                            "consumed_at": r["consumed_at"],
                        }
                    )

            if _table_exists(conn, "timers"):
                tm_rows = conn.execute(
                    "SELECT timer_id, inbox_id, kind, due_at, recurrence_seconds, session_id, status "
                    "FROM timers ORDER BY due_at DESC LIMIT 15"
                ).fetchall()
                result["timers"] = [dict(r) for r in tm_rows]
        finally:
            conn.close()

    if cfg.outbox_dir.is_dir():
        md_files = sorted(cfg.outbox_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:25]
        for f in md_files:
            first_line = ""
            try:
                with f.open("r", encoding="utf-8") as fh:
                    first_line = fh.readline().strip().lstrip("#").strip()
            except Exception:
                pass
            stat = f.stat()
            result["outbox_files"].append(
                {
                    "filename": f.name,
                    "title": first_line[:120] or f.name,
                    "size": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                    .astimezone()
                    .isoformat(timespec="seconds"),
                }
            )

    lconn = _open_ro_sqlite(cfg.ledger_db_path)
    if lconn is not None:
        try:
            result["ledger"]["db_ok"] = True
            if _table_exists(lconn, "runs"):
                run_rows = lconn.execute(
                    "SELECT run_id, inbox_id, session_id, executor_id, runner_id, started_at, ended_at, duration_ms, "
                    "outcome_kind, emit_status, correlation_key, work_key, error_kind, error_summary, exit_code, usage_json "
                    "FROM runs ORDER BY started_at DESC LIMIT 30"
                ).fetchall()
                runs_list: list[dict[str, Any]] = []
                for r in run_rows:
                    d = dict(r)
                    d["usage"] = _parse_json(d.pop("usage_json", None))
                    runs_list.append(d)
                result["ledger"]["runs"] = runs_list

                stat_rows = lconn.execute(
                    "SELECT executor_id, COUNT(*) AS total, "
                    "SUM(CASE WHEN outcome_kind='emit' AND (emit_status IS NULL OR emit_status!='failed') THEN 1 ELSE 0 END) AS ok_count, "
                    "SUM(CASE WHEN outcome_kind='suspend' THEN 1 ELSE 0 END) AS suspend_count, "
                    "SUM(CASE WHEN error_kind IS NOT NULL THEN 1 ELSE 0 END) AS err_count, "
                    "CAST(AVG(duration_ms) AS INTEGER) AS avg_ms "
                    "FROM runs GROUP BY executor_id"
                ).fetchall()
                result["ledger"]["stats"] = [dict(r) for r in stat_rows]

            if _table_exists(lconn, "audit_events"):
                audit_rows = lconn.execute(
                    "SELECT audit_id, occurred_at, service, stage, outcome, severity, correlation_key, scope, "
                    "run_id, task_id, actor, duration_ms, detail_json "
                    "FROM audit_events ORDER BY occurred_at DESC LIMIT 40"
                ).fetchall()
                audits_list: list[dict[str, Any]] = []
                for r in audit_rows:
                    d = dict(r)
                    d["detail"] = _parse_json(d.pop("detail_json", None))
                    audits_list.append(d)
                result["ledger"]["audits"] = audits_list
        finally:
            lconn.close()

    return result


def build_snapshot(
    cfg: DashboardConfig,
    *,
    force_remote: bool = False,
    remote_cache: RemoteProbeCache | None = None,
) -> dict[str, Any]:
    hb_cfg = safe_read_toml(cfg.hb_config_path)
    sb_cfg = safe_read_toml(cfg.sb_config_path)
    tb_cfg = safe_read_toml(cfg.tb_config_path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_remote = pool.submit(_probe_remote, cfg, hb_cfg, remote_cache, force=force_remote)
        proc_info = _collect_processes()
        hb_data = _read_hb_data(cfg, proc_info)
        remote = f_remote.result()

    rt = _as_dict(hb_cfg.get("runtime"))
    ch = _as_dict(hb_cfg.get("channels"))
    sb_link = _as_dict(hb_cfg.get("switchboard"))
    projects_raw = _as_dict(hb_cfg.get("projects"))
    projects_summary: list[dict[str, Any]] = []
    for name, pcfg_raw in projects_raw.items():
        pcfg = _as_dict(pcfg_raw)
        if not pcfg:
            continue
        deliv = _as_dict(pcfg.get("delivery"))
        projects_summary.append(
            {
                "name": str(name),
                "repo": pcfg.get("repo", ""),
                "executor": pcfg.get("executor", ""),
                "handoff": pcfg.get("handoff", ""),
                "worktree_backend": pcfg.get("worktree_backend", ""),
                "auto_merge": bool(deliv.get("auto_merge", False)),
                "inbox_id": pcfg.get("inbox_id"),
            }
        )

    tb_tasks: list[dict[str, Any]] = remote["tb_tasks"]
    tb_status_counts: dict[str, int] = {k: 0 for k in ("backlog", "ready", "in_progress", "done", "cancelled")}
    for t in tb_tasks:
        st = str(t.get("status") or "unknown")
        tb_status_counts[st] = tb_status_counts.get(st, 0) + 1

    sb_routing = _as_dict(sb_cfg.get("routing"))
    sb_github = _as_dict(sb_cfg.get("github"))
    sb_outbound = _as_dict(sb_cfg.get("outbound"))
    sb_http = _as_dict(sb_cfg.get("http"))
    sb_relay = _as_dict(sb_cfg.get("relay"))
    tb_http = _as_dict(tb_cfg.get("http"))
    tb_sb = _as_dict(tb_cfg.get("switchboard"))
    tb_intake = _as_dict(tb_cfg.get("intake"))
    gdrive_cfg = _as_dict(hb_cfg.get("gdrive"))
    executors_raw = _as_dict(hb_cfg.get("executors"))

    daemon_info = proc_info["daemon"]
    hub_base = str(remote["hub_base"])

    return {
        "generated_at": _iso_now(),
        "hb": {
            "version": _read_hb_version(cfg.hb_root),
            "daemon": daemon_info,
            "supervisor": proc_info["supervisor"],
            "ccpeer": proc_info["ccpeer"],
            "gate_workers_count": proc_info["gate_workers_count"],
            "config": {
                "max_concurrency": int(rt.get("max_concurrency", 1)),
                "new_event_policy": rt.get("new_event_policy", "defer"),
                "http_bind": f"{ch.get('host', '127.0.0.1')}:{ch.get('port', 8080)}",
                "switchboard_mode": sb_link.get("mode", "disabled") if sb_link.get("enabled") else "disabled",
                "relay_url": sb_link.get("relay_url", ""),
                "consumer_id": sb_link.get("consumer_id", ""),
                "gdrive_enabled": bool(gdrive_cfg.get("enabled", False)),
                "projects": projects_summary,
                "executors": sorted(str(k) for k in executors_raw),
            },
            "gdrive_health": remote["gdrive_health"],
            **hb_data,
        },
        "tb": {
            "url": f"{hub_base}/taskboard" if hub_base else "",
            "health": remote["tb_health"],
            "version": remote["tb_version"],
            "token_present": remote["tb_token_present"],
            "mcp_latency_ms": remote["tb_mcp_latency_ms"],
            "tasks_error": remote["tb_tasks_error"],
            "status_counts": tb_status_counts,
            "tasks_total": len(tb_tasks),
            "tasks": tb_tasks,
            "config": {
                "base_path": tb_http.get("base_path", "/taskboard"),
                "consumer_id": tb_sb.get("consumer_id", "taskboard"),
                "auto_ready_sources": tb_intake.get("auto_ready_sources", []),
            },
        },
        "sb": {
            "url": f"{hub_base}/switchboard" if hub_base else "",
            "health": remote["sb_health"],
            "hb_relay_connected": bool(daemon_info and daemon_info.get("relay_connected")),
            "hb_relay_peer": daemon_info.get("relay_peer") if daemon_info else None,
            "config": {
                "base_path": sb_http.get("base_path", "/switchboard"),
                "require_label": sb_github.get("require_label", ""),
                "unmatched_policy": sb_routing.get("unmatched_policy", ""),
                "default_scope": sb_routing.get("default_scope", ""),
                "github_repos": _as_dict(sb_routing.get("github_repos")),
                "github_owners": _as_dict(sb_routing.get("github_owners")),
                "feishu_chats": _as_dict(sb_routing.get("feishu_chats")),
                "approval_channel": sb_outbound.get("approval_channel", ""),
                "reply_to_origin": sb_outbound.get("reply_to_origin", []),
                "relay": sb_relay,
            },
            "observed_sources": _as_dict(hb_data["counts"]).get("sb_sources", {}),
            "observed_events": hb_data["sb_events"],
            "observed_audits": [
                _as_dict(a)
                for a in cast(list[Any], hb_data["ledger"]["audits"])
                if _as_dict(a).get("service") == "switchboard"
            ],
        },
    }


def read_run_detail(cfg: DashboardConfig, session_id: str) -> dict[str, Any] | None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", session_id):
        return None
    base = cfg.runs_dir.resolve()
    sess_dir = (base / session_id).resolve()
    try:
        sess_dir.relative_to(base)
    except ValueError:
        return None
    if not sess_dir.is_dir():
        return None
    candidates = sorted(
        sess_dir.glob("run_*/state.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    try:
        data = json.loads(candidates[0].read_text(encoding="utf-8"))
    except Exception:
        return None
    turns_dir = candidates[0].parent / "turns"
    turns = sorted(p.name for p in turns_dir.iterdir()) if turns_dir.is_dir() else []
    return {"run_dir": str(candidates[0].parent), "turns": turns, "state_json": data}


def read_outbox_file(cfg: DashboardConfig, filename: str) -> str | None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.md", filename):
        return None
    base = cfg.outbox_dir.resolve()
    target = (base / filename).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return None
    if not target.is_file():
        return None
    return target.read_text(encoding="utf-8", errors="replace")
