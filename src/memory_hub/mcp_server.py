from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from memory_hub.hub import MemoryHub
from memory_hub.models import (
    AuthPrincipal,
    PostMortemCandidate,
    SearchQuery,
    WriteProposal,
)
from memory_hub.partition import parse_memory_uri
from memory_hub.ports import TokenAuthPort

MAX_BODY_BYTES = 1_048_576


class StaticTokenAuth:
    def __init__(self, tokens: dict[str, AuthPrincipal]) -> None:
        self._tokens = dict(tokens)

    def authenticate(self, bearer_token: str) -> AuthPrincipal | None:
        return self._tokens.get(bearer_token)


MCP_TOOLS: list[dict[str, Any]] = [
    {
        "name": "memory_load_baseline",
        "description": "Load cold/hot segmented baseline bundle for a project_key.",
        "inputSchema": {
            "type": "object",
            "required": ["project_key"],
            "properties": {"project_key": {"type": "string"}},
        },
    },
    {
        "name": "memory_read",
        "description": "Read an on-demand memory document or specific Markdown heading by URI.",
        "inputSchema": {
            "type": "object",
            "required": ["uri"],
            "properties": {
                "uri": {"type": "string"},
                "heading": {"type": "string"},
                "max_bytes": {"type": "integer"},
            },
        },
    },
    {
        "name": "memory_search",
        "description": "Search project/shared memory (and optionally readonly kb:// mounts) via SQLite FTS5.",
        "inputSchema": {
            "type": "object",
            "required": ["scope", "query"],
            "properties": {
                "scope": {"type": "string"},
                "query": {"type": "string"},
                "top_k": {"type": "integer", "default": 8},
                "include_shared": {"type": "boolean", "default": True},
                "include_kb": {"type": "boolean", "default": False},
                "include_archived": {"type": "boolean", "default": False},
            },
        },
    },
    {
        "name": "memory_propose_write",
        "description": "Submit a governed write or proposal to Memory Hub.",
        "inputSchema": {
            "type": "object",
            "required": ["target_uri", "op", "content", "rationale"],
            "properties": {
                "target_uri": {"type": "string"},
                "op": {"type": "string"},
                "content": {"type": "string"},
                "rationale": {"type": "string"},
                "section_heading": {"type": "string"},
                "base_sha256": {"type": "string"},
                "evidence_ref": {"type": "string"},
                "supersedes": {"type": "array", "items": {"type": "string"}},
                "force_new": {"type": "boolean"},
            },
        },
    },
    {
        "name": "memory_approve_proposal",
        "description": "Approve and atomically apply a pending high-sensitivity proposal (admin role).",
        "inputSchema": {
            "type": "object",
            "required": ["proposal_id"],
            "properties": {"proposal_id": {"type": "string"}},
        },
    },
    {
        "name": "memory_list_proposals",
        "description": "List proposals filtered by status (default: pending).",
        "inputSchema": {
            "type": "object",
            "properties": {"status": {"type": "string"}},
        },
    },
]


class MemoryHubHTTPServer:
    def __init__(
        self,
        hub: MemoryHub,
        auth: TokenAuthPort,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self._hub = hub
        self._auth = auth
        self._write_lock = threading.Lock()
        self._httpd = ThreadingHTTPServer((host, port), self._make_handler())
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        addr = self._httpd.server_address
        return str(addr[0]), int(addr[1])

    def start_background(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _authorize(self, auth_header: str | None) -> AuthPrincipal | None:
        if not auth_header or not auth_header.startswith("Bearer "):
            return None
        token = auth_header[len("Bearer ") :].strip()
        if not token:
            return None
        return self._auth.authenticate(token)

    def _dispatch_mcp_tool(self, principal: AuthPrincipal, name: str, args: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if name == "memory_load_baseline":
            pk = str(args["project_key"])
            if not principal.can_access_project(pk):
                return HTTPStatus.FORBIDDEN, {"error": f"Project '{pk}' forbidden"}
            return HTTPStatus.OK, self._hub.load_baseline(pk).to_dict()

        if name == "memory_read":
            uri = str(args["uri"])
            parsed = parse_memory_uri(uri)
            if parsed.scheme == "proj" and not principal.can_access_project(parsed.scope):
                return HTTPStatus.FORBIDDEN, {"error": f"Project '{parsed.scope}' forbidden"}
            if parsed.scheme == "kb" and not principal.allow_kb:
                return HTTPStatus.FORBIDDEN, {"error": "KB access forbidden for token"}
            doc = self._hub.read_on_demand(
                uri,
                heading=args.get("heading"),
                max_bytes=args.get("max_bytes"),
            )
            return HTTPStatus.OK, doc.to_dict()

        if name == "memory_search":
            scope = str(args["scope"])
            if not principal.can_access_project(scope):
                return HTTPStatus.FORBIDDEN, {"error": f"Project '{scope}' forbidden"}
            include_kb = bool(args.get("include_kb", False))
            if include_kb and not principal.allow_kb:
                return HTTPStatus.FORBIDDEN, {"error": "KB search forbidden for token"}
            res = self._hub.search(
                SearchQuery(
                    scope=scope,
                    query=str(args["query"]),
                    top_k=int(args.get("top_k", 8)),
                    include_shared=bool(args.get("include_shared", True)),
                    include_kb=include_kb,
                    include_archived=bool(args.get("include_archived", False)),
                )
            )
            return HTTPStatus.OK, res.to_dict()

        if name == "memory_propose_write":
            if not principal.has_role("propose"):
                return HTTPStatus.FORBIDDEN, {"error": "Role 'propose' required"}
            parsed = parse_memory_uri(str(args["target_uri"]))
            if parsed.scheme == "proj" and not principal.can_access_project(parsed.scope):
                return HTTPStatus.FORBIDDEN, {"error": f"Project '{parsed.scope}' forbidden"}
            payload = dict(args)
            payload.setdefault("author", principal.subject)
            with self._write_lock:
                res = self._hub.propose_write(WriteProposal.from_dict(payload))
            return HTTPStatus.OK, res.to_dict()

        if name == "memory_approve_proposal":
            if not principal.has_role("admin"):
                return HTTPStatus.FORBIDDEN, {"error": "Role 'admin' required"}
            with self._write_lock:
                res = self._hub.approve_proposal(str(args["proposal_id"]), actor=principal.subject)
            return HTTPStatus.OK, res.to_dict()

        if name == "memory_list_proposals":
            status = args.get("status", "pending")
            return HTTPStatus.OK, {"items": self._hub.list_proposals(status=status)}

        return HTTPStatus.NOT_FOUND, {"error": f"Unknown tool: {name}"}

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def _send_json(self, code: int, payload: dict[str, Any]) -> None:
                raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _read_json_body(self) -> dict[str, Any] | None:
                length = int(self.headers.get("Content-Length", "0"))
                if length > MAX_BODY_BYTES:
                    self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "Payload too large"})
                    return None
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    data = json.loads(raw.decode("utf-8"))
                except Exception:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Invalid JSON body"})
                    return None
                if not isinstance(data, dict):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "JSON object required"})
                    return None
                return data

            def do_GET(self) -> None:
                parsed_url = urlparse(self.path)
                if parsed_url.path == "/healthz":
                    self._send_json(HTTPStatus.OK, {"status": "ok"})
                    return
                principal = outer._authorize(self.headers.get("Authorization"))
                if principal is None:
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "Unauthorized"})
                    return
                qs = parse_qs(parsed_url.query)

                try:
                    if parsed_url.path == "/v1/memory/baseline":
                        pk = (qs.get("project_key") or [""])[0]
                        code, data = outer._dispatch_mcp_tool(principal, "memory_load_baseline", {"project_key": pk})
                        self._send_json(code, data)
                        return
                    if parsed_url.path == "/v1/memory/search":
                        args: dict[str, Any] = {
                            "scope": (qs.get("scope") or [""])[0],
                            "query": (qs.get("q") or [""])[0],
                            "top_k": int((qs.get("top_k") or ["8"])[0]),
                            "include_shared": ((qs.get("include_shared") or ["true"])[0].lower() == "true"),
                            "include_kb": ((qs.get("include_kb") or ["false"])[0].lower() == "true"),
                        }
                        code, data = outer._dispatch_mcp_tool(principal, "memory_search", args)
                        self._send_json(code, data)
                        return
                    if parsed_url.path == "/v1/memory/proposals":
                        status = (qs.get("status") or ["pending"])[0]
                        code, data = outer._dispatch_mcp_tool(principal, "memory_list_proposals", {"status": status})
                        self._send_json(code, data)
                        return
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
                except (KeyError, ValueError, FileNotFoundError) as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                except PermissionError as exc:
                    self._send_json(HTTPStatus.FORBIDDEN, {"error": str(exc)})

            def do_POST(self) -> None:
                parsed_url = urlparse(self.path)
                principal = outer._authorize(self.headers.get("Authorization"))
                if principal is None:
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "Unauthorized"})
                    return
                body = self._read_json_body()
                if body is None:
                    return

                try:
                    if parsed_url.path == "/mcp":
                        req_id = body.get("id")
                        method = body.get("method")
                        params = body.get("params") or {}
                        if method == "tools/list":
                            self._send_json(
                                HTTPStatus.OK,
                                {"jsonrpc": "2.0", "id": req_id, "result": {"tools": MCP_TOOLS}},
                            )
                            return
                        if method == "tools/call":
                            name = str(params.get("name", ""))
                            args = params.get("arguments") or {}
                            code, result = outer._dispatch_mcp_tool(principal, name, args)
                            if code != HTTPStatus.OK:
                                self._send_json(
                                    code,
                                    {
                                        "jsonrpc": "2.0",
                                        "id": req_id,
                                        "error": {"code": -32000, "message": result.get("error", "Error")},
                                    },
                                )
                                return
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "jsonrpc": "2.0",
                                    "id": req_id,
                                    "result": {
                                        "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                                        "structuredContent": result,
                                    },
                                },
                            )
                            return
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "Method not found"}},
                        )
                        return

                    if parsed_url.path == "/v1/memory/read":
                        code, data = outer._dispatch_mcp_tool(principal, "memory_read", body)
                        self._send_json(code, data)
                        return
                    if parsed_url.path == "/v1/memory/propose":
                        code, data = outer._dispatch_mcp_tool(principal, "memory_propose_write", body)
                        self._send_json(code, data)
                        return
                    if parsed_url.path == "/v1/memory/approve":
                        code, data = outer._dispatch_mcp_tool(principal, "memory_approve_proposal", body)
                        self._send_json(code, data)
                        return
                    if parsed_url.path == "/v1/memory/postmortem":
                        if not principal.has_role("propose"):
                            self._send_json(HTTPStatus.FORBIDDEN, {"error": "Role 'propose' required"})
                            return
                        cand = PostMortemCandidate.from_dict(body)
                        if not principal.can_access_project(cand.project_key):
                            self._send_json(HTTPStatus.FORBIDDEN, {"error": f"Project '{cand.project_key}' forbidden"})
                            return
                        with outer._write_lock:
                            res = outer._hub.record_postmortem(cand)
                        self._send_json(HTTPStatus.OK, res.to_dict())
                        return
                    if parsed_url.path == "/v1/memory/gc":
                        if not principal.has_role("admin"):
                            self._send_json(HTTPStatus.FORBIDDEN, {"error": "Role 'admin' required"})
                            return
                        pk = str(body["project_key"])
                        closed = set(body["closed_work_keys"]) if "closed_work_keys" in body else None
                        with outer._write_lock:
                            rep = outer._hub.run_mechanical_gc(pk, closed_work_keys=closed)
                        self._send_json(HTTPStatus.OK, rep.to_dict())
                        return
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
                except (KeyError, ValueError, FileNotFoundError) as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                except PermissionError as exc:
                    self._send_json(HTTPStatus.FORBIDDEN, {"error": str(exc)})

        return Handler
