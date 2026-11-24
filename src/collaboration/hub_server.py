from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import threading
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from collaboration.closeout import CloseoutCoordinator, CloseoutStore
from collaboration.digest import DigestAssembler
from collaboration.expert_pool import ExpertPoolRegistry
from collaboration.hub_page import HUB_CSP, HUB_HTML
from collaboration.models import CloseoutCard
from collaboration.role_memory import RoleSkillMemoryStore

_log = logging.getLogger(__name__)

_MAX_BODY_BYTES = 64 * 1024
_MAX_DRAINED_BYTES = 1024 * 1024
_RECENT_SYNCED = 20


class HubStateProvider(Protocol):
    def list_tasks(self) -> list[dict[str, Any]]: ...
    def list_runs(self) -> list[dict[str, Any]]: ...
    def list_project_proposals(self) -> list[dict[str, Any]]: ...


class _DefaultEmptyProvider:
    def list_tasks(self) -> list[dict[str, Any]]:
        return []

    def list_runs(self) -> list[dict[str, Any]]:
        return []

    def list_project_proposals(self) -> list[dict[str, Any]]:
        return []


class ProjectProposalReviewPort(Protocol):
    # KeyError for an unknown id, ValueError when it cannot be decided as it stands.
    def approve(self, proposal_id: str, *, reviewed_by: str) -> dict[str, Any]: ...
    def reject(self, proposal_id: str, *, reviewed_by: str, review_note: str) -> dict[str, Any]: ...


class _HubHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        role_store: RoleSkillMemoryStore,
        closeout_store: CloseoutStore,
        closeout_coordinator: CloseoutCoordinator,
        expert_pool: ExpertPoolRegistry,
        state_provider: HubStateProvider,
        project_reviewer: ProjectProposalReviewPort | None,
        window_label: str,
        bearer_token: str | None,
        reviewer: str,
        base_path: str,
    ) -> None:
        self.role_store = role_store
        self.closeout_store = closeout_store
        self.closeout_coordinator = closeout_coordinator
        self.expert_pool = expert_pool
        self.state_provider = state_provider
        self.project_reviewer = project_reviewer
        self.window_label = window_label
        self.bearer_token = bearer_token
        self.reviewer = reviewer
        self.base_path = base_path
        self.assembler = DigestAssembler()
        super().__init__(address, _HubHandler)

    def digest(self) -> dict[str, Any]:
        failed: list[str] = []

        def read(name: str, source: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
            try:
                return source()
            except Exception:  # noqa: BLE001
                _log.exception("collaboration hub: reading %s failed", name)
                failed.append(name)
                return []

        tasks = read("tasks", self.state_provider.list_tasks)
        runs = read("runs", self.state_provider.list_runs)
        proposals = read("proposals", self.state_provider.list_project_proposals)
        label = self.window_label + (f"（读取失败：{'、'.join(failed)}）" if failed else "")
        return self.assembler.assemble(
            window_label=label,
            tasks=tasks,
            runs=runs,
            role_proposals=self.role_store.list_proposals(),
            project_proposals=proposals,
            closeouts=self.recent_closeouts(),
        ).to_dict()

    def recent_closeouts(self) -> list[CloseoutCard]:
        cards = sorted(self.closeout_store.list_cards(), key=lambda c: c.created_at, reverse=True)
        return [c for c in cards if c.status in ("draft", "confirmed")] + [
            c for c in cards if c.status == "synced"
        ][:_RECENT_SYNCED]

    def proposals(self) -> list[dict[str, Any]]:
        found = [
            {**p.to_dict(), "reviewable": p.status == "pending"} for p in self.role_store.list_proposals()
        ]
        try:
            project = self.state_provider.list_project_proposals()
        except Exception:  # noqa: BLE001
            _log.exception("collaboration hub: reading proposals failed")
            project = []
        return found + [{**p, "reviewable": self.project_reviewer is not None} for p in project]


class _BadRequest(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


class _HubHandler(BaseHTTPRequestHandler):
    server_version = "HibernautCollaborationHub/1.0"
    # A connection that sends nothing must not hold its thread for good.
    timeout = 30

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _srv(self) -> _HubHTTPServer:
        return cast(_HubHTTPServer, self.server)

    def _send(self, code: int, body: bytes, content_type: str, headers: dict[str, str] | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code: int, payload: Any) -> None:
        self._send(code, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def _path(self) -> str:
        # A reverse proxy may pass the prefix on or strip it.
        path = urlparse(self.path).path
        base = self._srv().base_path
        if base and (path == base or path.startswith(f"{base}/")):
            return path[len(base):]
        return path

    def _authorized(self) -> bool:
        expected = self._srv().bearer_token
        if expected is None:
            return True
        scheme, _, provided = self.headers.get("Authorization", "").partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(
            provided.strip().encode("utf-8"), expected.encode("utf-8")
        )

    def _read_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = -1
        if length < 0:
            raise _BadRequest(HTTPStatus.BAD_REQUEST, "invalid Content-Length")
        if length > _MAX_BODY_BYTES:
            # Read what was sent, or the client sees a reset connection instead of the answer.
            self.rfile.read(min(length, _MAX_DRAINED_BYTES))
            raise _BadRequest(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            body: object = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise _BadRequest(HTTPStatus.BAD_REQUEST, "request body is not JSON") from exc
        if not isinstance(body, dict):
            raise _BadRequest(HTTPStatus.BAD_REQUEST, "request body must be an object")
        return cast(dict[str, Any], body)

    def do_GET(self) -> None:
        srv = self._srv()
        path = self._path()
        if path == "" and srv.base_path:
            # The page addresses the API relative to its own directory.
            self._send(HTTPStatus.PERMANENT_REDIRECT, b"", "text/plain", {"Location": f"{srv.base_path}/"})
            return
        if path in ("", "/", "/index.html"):
            self._send(HTTPStatus.OK, HUB_HTML.encode("utf-8"), "text/html; charset=utf-8", {
                "Content-Security-Policy": HUB_CSP, "Referrer-Policy": "no-referrer",
            })
            return
        if path.startswith("/api/") and not self._authorized():
            self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        views: dict[str, Callable[[], Any]] = {
            "/api/digest": srv.digest,
            "/api/proposals": lambda: {"proposals": srv.proposals()},
            "/api/closeouts": lambda: {"closeouts": [c.to_dict() for c in srv.closeout_store.list_cards()]},
            "/api/experts": lambda: {"experts": [p.to_dict() for p in srv.expert_pool.list_profiles()]},
        }
        view = views.get(path)
        if view is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": f"not found: {path}"})
            return
        try:
            payload = view()
        except Exception:  # noqa: BLE001
            _log.exception("collaboration hub: GET %s failed", path)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "reading failed"})
            return
        self._send_json(HTTPStatus.OK, payload)

    def do_POST(self) -> None:
        if not self._authorized():
            self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        path = self._path()
        try:
            body = self._read_body()
        except _BadRequest as exc:
            self._send_json(exc.code, {"error": str(exc)})
            return
        srv = self._srv()
        # Behind a token the reviewer is whoever holds it, never a name the request claims.
        reviewer = srv.reviewer if srv.bearer_token is not None else str(body.get("reviewer") or srv.reviewer)
        parts = path.split("/")
        action = self._action(parts, reviewer, str(body.get("note") or "")) if len(parts) == 5 else None
        if action is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": f"not found: {path}"})
            return
        try:
            payload = action()
        except KeyError as exc:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc.args[0]) if exc.args else "not found"})
        except ValueError as exc:
            self._send_json(HTTPStatus.CONFLICT, {"error": str(exc)})
        except Exception:  # noqa: BLE001
            _log.exception("collaboration hub: POST %s failed", path)
            self._send_json(HTTPStatus.BAD_GATEWAY, {"error": "failed half-way; do the same again to finish it"})
        else:
            self._send_json(HTTPStatus.OK, payload)

    def _action(self, parts: list[str], reviewer: str, note: str) -> Callable[[], dict[str, Any]] | None:
        srv = self._srv()
        _, root, kind, target, verb = parts
        if root != "api":
            return None
        if kind == "closeouts" and verb == "confirm":
            return lambda: self._confirm(target, reviewer)
        if kind != "proposals" or verb not in ("approve", "reject"):
            return None
        if srv.project_reviewer is not None and not target.startswith("rprop-"):
            project = srv.project_reviewer
            if verb == "approve":
                return lambda: project.approve(target, reviewed_by=reviewer)
            return lambda: project.reject(target, reviewed_by=reviewer, review_note=note)
        decide = srv.role_store.approve_proposal if verb == "approve" else srv.role_store.reject_proposal
        return lambda: decide(target, reviewed_by=reviewer, review_note=note).to_dict()

    def _confirm(self, card_id: str, reviewer: str) -> dict[str, Any]:
        srv = self._srv()
        # Told apart from a KeyError out of a sink.
        srv.closeout_store.get(card_id)
        try:
            res = srv.closeout_coordinator.confirm_and_sync(card_id, confirmed_by=reviewer)
        except KeyError as exc:
            raise RuntimeError("a sink failed") from exc
        return {
            "card_id": res.card_id,
            "chief_of_staff_notified": res.chief_of_staff_notified,
            "tasks_created": list(res.tasks_created),
            "proposals_queued": list(res.proposals_queued),
            "already_synced": res.already_synced,
        }


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class CollaborationHubServer:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8790,
        role_store: RoleSkillMemoryStore,
        closeout_store: CloseoutStore,
        closeout_coordinator: CloseoutCoordinator,
        expert_pool: ExpertPoolRegistry | None = None,
        state_provider: HubStateProvider | None = None,
        project_reviewer: ProjectProposalReviewPort | None = None,
        window_label: str = "最近 24 小时",
        # Needed for everything under /api; without one the server only binds to loopback.
        bearer_token: str | None = None,
        reviewer: str = "owner",
        base_path: str = "",
    ) -> None:
        if bearer_token is not None and not bearer_token.strip():
            raise ValueError("bearer_token must not be blank")
        if bearer_token is None and not _is_loopback(host):
            raise ValueError(f"the collaboration hub needs a bearer_token to listen on {host}")
        self._httpd = _HubHTTPServer(
            (host, port),
            role_store=role_store,
            closeout_store=closeout_store,
            closeout_coordinator=closeout_coordinator,
            expert_pool=expert_pool or ExpertPoolRegistry(),
            state_provider=state_provider or _DefaultEmptyProvider(),
            project_reviewer=project_reviewer,
            window_label=window_label,
            bearer_token=bearer_token,
            reviewer=reviewer,
            base_path=base_path.rstrip("/"),
        )
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        addr = self._httpd.server_address
        return (str(addr[0]), int(addr[1]))

    def start_background(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def close(self) -> None:
        # shutdown() waits for a serve_forever() that was never started.
        if self._thread is not None:
            self._httpd.shutdown()
            self._thread.join(timeout=2.0)
        self._httpd.server_close()
