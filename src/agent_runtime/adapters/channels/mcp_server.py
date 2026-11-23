from __future__ import annotations

import hmac
import json
from collections.abc import Callable, Mapping
from typing import Any, TextIO

from agent_runtime import __version__
from agent_runtime.adapters.channels.http_types import WebhookResponse
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite.stores import (
    SQLiteInboxAdapter,
    SQLiteSessionStoreAdapter,
)
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import explicit_trace_id, new_id
from agent_runtime.core.ports import OutboxPort
from agent_runtime.engine.handler import EventHandler

_PROTOCOL_VERSION = "2024-11-05"

_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "hibernaut_ingest_event",
        "description": "Ingest a new event or task into Hibernaut Event Inbox.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "inbox_id": {"type": "string"},
                "source": {"type": "string"},
                "payload": {"type": "object"},
                "external_id": {"type": "string"},
                "correlation_key": {"type": "string"},
                "session_id": {"type": "string"},
            },
            "required": ["inbox_id", "source", "payload"],
        },
    },
    {
        "name": "hibernaut_list_sessions",
        "description": "List Hibernaut sessions filtered by inbox_id, lifecycle status or parent_session_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "inbox_id": {"type": "string"},
                "parent_session_id": {"type": "string"},
                "status": {
                    "type": "string",
                    "enum": ["active", "suspended", "archived", "closed"],
                },
            },
        },
    },
    {
        "name": "hibernaut_list_events",
        "description": "List events from Hibernaut Event Inbox filtered by inbox_id or status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "inbox_id": {"type": "string"},
                "status": {
                    "type": "string",
                    "enum": ["pending", "processing", "done", "failed", "dropped"],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
        },
    },
    {
        "name": "hibernaut_list_workspaces",
        "description": "List managed hb-* dynamic worktrees tracked in SQLite workspaces table.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "hibernaut_resolve_approval",
        "description": "Submit a human approval decision to resume a Suspend session.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "inbox_id": {"type": "string"},
                "approval_request_id": {"type": "string"},
                "approved": {"type": "boolean"},
                "comment": {"type": "string"},
            },
            "required": ["inbox_id", "approval_request_id", "approved"],
        },
    },
    {
        "name": "hibernaut_reclaim_workspace",
        "description": "Run reference-count and 3-gate safety reclaim for a managed hb-* workspace.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "work_key": {"type": "string"},
            },
            "required": ["work_key"],
        },
    },
    {
        "name": "hibernaut_close_session",
        "description": "Close a Hibernaut session and trigger worktree reclaim check if no active references remain.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string"},
            },
            "required": ["session_id"],
        },
    },
]


class MCPServerAdapter:
    def __init__(
        self,
        *,
        handler: EventHandler,
        sessions: SQLiteSessionStoreAdapter,
        workspaces: SQLiteWorkspaceStore,
        inbox: SQLiteInboxAdapter | None = None,
        worktree_manager: OrcaWorktreeManager | None = None,
        outbox: OutboxPort | None = None,
        bearer_token: str | None = None,
        worktree_manager_for: Callable[[str | None, str], OrcaWorktreeManager | None] | None = None,
    ) -> None:
        self._handler = handler
        # (inbox_id or None, work_key) -> the manager of the project that owns that workspace, if any.
        self._worktree_manager_for = worktree_manager_for
        self._sessions = sessions
        self._workspaces = workspaces
        self._inbox = inbox
        self._worktree_manager = worktree_manager
        self._outbox = outbox
        self._bearer_token = bearer_token.strip() if bearer_token else None

    @property
    def http_enabled(self) -> bool:
        # Disable /mcp HTTP endpoint by default when bearer_token is unconfigured.
        return bool(self._bearer_token)

    def _verify_bearer(self, headers: Mapping[str, str]) -> bool:
        if not self._bearer_token:
            return False
        lower = {k.lower(): v for k, v in headers.items()}
        auth = lower.get("authorization", "")
        if not auth.startswith("Bearer "):
            return False
        provided = auth.removeprefix("Bearer ").strip()
        return hmac.compare_digest(self._bearer_token, provided)

    def handle_http(self, raw_body: bytes, headers: Mapping[str, str]) -> WebhookResponse:
        if not self._verify_bearer(headers):
            return WebhookResponse(status_code=401, body={"error": "unauthorized"})
        try:
            message = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return WebhookResponse(
                status_code=400,
                body=self._error_response(None, -32700, "Parse error"),
            )
        if not isinstance(message, dict):
            return WebhookResponse(
                status_code=400,
                body=self._error_response(None, -32600, "Invalid Request"),
            )
        reply = self.handle_rpc(message)
        if reply is None:
            return WebhookResponse(status_code=202, body={})
        return WebhookResponse(status_code=200, body=reply)

    def serve_stdio(self, stdin: TextIO, stdout: TextIO) -> None:
        for line in stdin:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                message = json.loads(stripped)
            except json.JSONDecodeError:
                err = self._error_response(None, -32700, "Parse error")
                stdout.write(json.dumps(err, ensure_ascii=False) + "\n")
                stdout.flush()
                continue
            if not isinstance(message, dict):
                err = self._error_response(None, -32600, "Invalid Request")
                stdout.write(json.dumps(err, ensure_ascii=False) + "\n")
                stdout.flush()
                continue
            reply = self.handle_rpc(message)
            if reply is not None:
                stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
                stdout.flush()

    def handle_rpc(self, request: dict[str, Any]) -> dict[str, Any] | None:
        method = request.get("method")
        req_id = request.get("id")
        params = request.get("params") if isinstance(request.get("params"), dict) else {}

        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return self._ok_response(
                req_id,
                {
                    "protocolVersion": _PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "hibernaut-mcp", "version": __version__},
                },
            )
        if method == "ping":
            return self._ok_response(req_id, {})
        if method == "tools/list":
            return self._ok_response(req_id, {"tools": _TOOL_DEFINITIONS})
        if method == "tools/call":
            return self._handle_tool_call(req_id, params)

        if req_id is None:
            return None
        return self._error_response(req_id, -32601, f"Method not found: {method}")

    def _handle_tool_call(self, req_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        tool_name = str(params.get("name") or "")
        args = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
        try:
            payload = self._dispatch_tool(tool_name, args)
        except KeyError:
            return self._error_response(req_id, -32602, f"Unknown tool: {tool_name}")
        except (TypeError, ValueError) as exc:
            return self._error_response(req_id, -32602, str(exc))

        return self._ok_response(
            req_id,
            {
                "content": [
                    {"type": "text", "text": json.dumps(payload, ensure_ascii=False)},
                ],
                "structuredContent": payload,
                "isError": False,
            },
        )

    def _dispatch_tool(self, tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "hibernaut_ingest_event":
            inbox_id = str(args["inbox_id"])
            source = str(args["source"])
            payload = args["payload"]
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            external_id = str(args.get("external_id") or new_id("mcp"))
            correlation_key = (
                str(args["correlation_key"]) if args.get("correlation_key") is not None else None
            )
            session_id = str(args["session_id"]) if args.get("session_id") is not None else None
            result = self._handler.ingest(
                inbox_id=inbox_id,
                source=source,
                external_id=external_id,
                payload=payload,
                session_id=session_id,
                correlation_key=correlation_key,
            )
            return {
                "event_id": result.event_id,
                "status": result.status,
                "duplicate": result.duplicate,
            }

        if tool_name == "hibernaut_list_sessions":
            inbox_id = str(args["inbox_id"]) if args.get("inbox_id") is not None else None
            status = str(args["status"]) if args.get("status") is not None else None
            if status is not None and status not in ("active", "suspended", "archived", "closed"):
                raise ValueError(f"Unsupported session status: {status}")
            parent_session_id = (
                str(args["parent_session_id"]) if args.get("parent_session_id") is not None else None
            )
            rows = self._sessions.list_sessions(
                inbox_id=inbox_id,
                status=status,
                parent_session_id=parent_session_id,
            )
            return {
                "sessions": [
                    {
                        "session_id": s.session_id,
                        "inbox_id": s.inbox_id,
                        "status": s.status,
                        "work_key": s.work_key,
                        "correlation_key": s.correlation_key,
                        "executor_id": s.executor_id,
                        "executor_session_id": s.executor_session_id,
                        "pending_approval_id": s.pending_request_id,
                        "parent_session_id": s.parent_session_id,
                        "updated_at": s.updated_at,
                    }
                    for s in rows
                ]
            }

        if tool_name == "hibernaut_list_events":
            if self._inbox is None:
                raise ValueError("event inbox is not attached to MCPServerAdapter")
            inbox_id = str(args["inbox_id"]) if args.get("inbox_id") is not None else None
            status = str(args["status"]) if args.get("status") is not None else None
            if status is not None and status not in (
                "pending",
                "processing",
                "done",
                "failed",
                "dropped",
            ):
                raise ValueError(f"Unsupported event status: {status}")
            limit = int(args.get("limit", 50))
            events = self._inbox.list_events(inbox_id=inbox_id, status=status, limit=limit)
            diagnostics = self._inbox.diagnostics([e.event_id for e in events])
            return {
                "events": [
                    {
                        "event_id": e.event_id,
                        "inbox_id": e.inbox_id,
                        "source": e.source,
                        "external_id": e.external_id,
                        "status": e.status,
                        "attempts": diag.attempts if (diag := diagnostics.get(e.event_id)) else 0,
                        "last_error": diag.last_error if diag else None,
                        "correlation_key": e.correlation_key,
                        "session_id": e.session_id,
                        "trace_id": explicit_trace_id(e),
                        "parent_event_id": parent if isinstance(parent := e.payload.get("parent_event_id"), str) else None,
                        "created_at": e.occurred_at,
                    }
                    for e in events
                ]
            }

        if tool_name == "hibernaut_list_workspaces":
            rows = self._workspaces.list_all()
            return {
                "workspaces": [
                    {
                        "work_key": w.work_key,
                        "repo_checkout": w.repo_checkout,
                        "workdir": w.workdir,
                        "branch": w.branch,
                        "status": w.status,
                    }
                    for w in rows
                ]
            }

        if tool_name == "hibernaut_resolve_approval":
            inbox_id = str(args["inbox_id"])
            approval_request_id = str(args["approval_request_id"])
            approved = bool(args["approved"])
            payload: dict[str, Any] = {
                "approval_request_id": approval_request_id,
                "approved": approved,
            }
            if args.get("comment") is not None:
                payload["comment"] = str(args["comment"])
            result = self._handler.ingest(
                inbox_id=inbox_id,
                source="mcp:approval",
                external_id=f"mcp-approval:{approval_request_id}:{approved}",
                payload=payload,
            )
            return {
                "event_id": result.event_id,
                "status": result.status,
                "duplicate": result.duplicate,
            }

        if tool_name == "hibernaut_reclaim_workspace":
            work_key = str(args["work_key"])
            mgr = self._manager_for(None, work_key)
            res = mgr.reclaim(work_key, notify_outbox=self._outbox)
            return {
                "reclaimed": res.reclaimed,
                "reason": res.reason,
                "workdir": res.workdir,
            }

        if tool_name == "hibernaut_close_session":
            session_id = str(args["session_id"])
            existing = self._sessions.get(session_id)
            if existing is None:
                raise ValueError(f"Session not found: {session_id}")
            self._sessions.close_session(session_id)
            work_key = existing.work_key
            active_refs = self._sessions.count_active_by_work_key(work_key, existing.inbox_id) if work_key else 0
            reclaim_payload: dict[str, Any] | None = None
            if work_key and active_refs == 0:
                mgr = self._manager_for(existing.inbox_id, work_key)
                res = mgr.reclaim(work_key, notify_outbox=self._outbox, inbox_id=existing.inbox_id)
                reclaim_payload = {
                    "reclaimed": res.reclaimed,
                    "reason": res.reason,
                    "workdir": res.workdir,
                }
            return {
                "session_id": session_id,
                "status": "closed",
                "work_key": work_key,
                "active_refs": active_refs,
                "reclaim": reclaim_payload,
            }

        raise KeyError(tool_name)

    def _manager_for(self, inbox_id: str | None, work_key: str) -> OrcaWorktreeManager:
        chosen = self._worktree_manager_for(inbox_id, work_key) if self._worktree_manager_for is not None else None
        return chosen or self._worktree_manager or OrcaWorktreeManager(
            workspace_store=self._workspaces,
            session_store=self._sessions,
        )

    def _ok_response(self, req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def _error_response(self, req_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}
