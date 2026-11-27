from __future__ import annotations

import hmac
import json
from collections.abc import Callable, Mapping
from typing import Any, Final, cast

from switchboard import __version__
from switchboard.envelope import CanonicalEvent, EventType, Source, format_utc, new_ulid, normalize_utc, utc_now
from switchboard.inbound._util import lower_headers, obj, parse_json_object, reject, text
from switchboard.pipeline import Clock, InboundPipeline, describe_ingest
from switchboard.ports import AdapterParseResult, CredentialBrokerPort
from switchboard.router import ScopeResolver
from switchboard.store_sqlite import DeliveryRow, ReplayError

_PROTOCOL_VERSION: Final = "2024-11-05"
_WORK_TYPES: Final = ("work.opened", "work.updated", "work.closed", "work.ready")
_SUBMIT_SOURCES: Final = ("mcp", "timer", "taskboard", "hibernaut")
_REPORT_SOURCES: Final = ("hibernaut", "taskboard")

TOOLS: Final[list[dict[str, Any]]] = [
    {
        "name": "submit_work",
        "description": "Submit a work item (external cron jobs use source='timer').",
        "inputSchema": {
            "type": "object",
            "properties": {
                "external_id": {"type": "string"},
                "subject": {"type": "string"},
                "body": {"type": "string"},
                "source": {"type": "string", "enum": list(_SUBMIT_SOURCES)},
                "event_type": {"type": "string", "enum": list(_WORK_TYPES)},
                "scope": {"type": "string"},
                "correlation_key": {"type": "string"},
                "actor": {"type": "string"},
                "work_kind": {"type": "string"},
                "state": {"type": "string"},
                "task_id": {"type": "string"},
                "metadata": {"type": "object"},
            },
            "required": ["external_id", "subject"],
        },
    },
    {
        "name": "post_message",
        "description": "Post a conversation message into a correlation thread.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "correlation_key": {"type": "string"},
                "body": {"type": "string"},
                "scope": {"type": "string"},
                "channel": {"type": "string"},
                "actor": {"type": "string"},
                "external_id": {"type": "string"},
            },
            "required": ["correlation_key", "body"],
        },
    },
    {
        "name": "decide_approval",
        "description": "Record an approval decision; repeated decisions for one request are dropped.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "approval_request_id": {"type": "string"},
                "decision": {"type": "string", "enum": ["approve", "reject"]},
                "correlation_key": {"type": "string"},
                "actor": {"type": "string"},
                "scope": {"type": "string"},
                "session_id": {"type": "string"},
                "comment": {"type": "string"},
            },
            "required": ["approval_request_id", "decision", "correlation_key", "actor"],
        },
    },
    {
        "name": "report_execution",
        "description": "Report a Hibernaut node's run outcome, or a Taskboard notification, as execution.reported "
        "(emit, suspend, error or done); repeated reports with one (source, external_id) are dropped.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "enum": list(_REPORT_SOURCES), "default": "hibernaut"},
                "external_id": {"type": "string"},
                "scope": {"type": "string"},
                "correlation_key": {"type": "string"},
                "subject": {"type": "string"},
                "body": {"type": "string"},
                "actor": {"type": "string"},
                "occurred_at": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "session_id": {"type": "string", "minLength": 1},
                        "approval_request_id": {"type": "string", "minLength": 1},
                        "outcome_kind": {"type": "string", "enum": ["emit", "suspend", "error", "done"]},
                        "deciders": {"type": "array", "items": {"type": "string"},
                                     "description": "For a suspend: the only Feishu open_ids whose click on its "
                                                    "approval card counts; left out, anybody who sees the card."},
                    },
                    "required": ["run_id", "session_id", "outcome_kind"],
                },
            },
            "required": ["external_id", "scope", "subject", "metadata"],
            "if": {"properties": {"metadata": {"properties": {"outcome_kind": {"const": "suspend"}}}}},
            "then": {"properties": {"metadata": {"required": ["approval_request_id"]}}},
            "else": {"required": ["correlation_key"]},
        },
    },
    {
        "name": "replay_delivery",
        "description": "Re-queue a dead_letter or retry delivery for one consumer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "event_id": {"type": "string"},
                "consumer_id": {"type": "string"},
            },
            "required": ["event_id", "consumer_id"],
        },
    },
]


class ToolError(ValueError):
    pass


class MCPInboundAdapter:
    def __init__(
        self,
        credentials: CredentialBrokerPort,
        pipeline: InboundPipeline,
        scopes: ScopeResolver,
        replay: Callable[[str, str], DeliveryRow],
        *,
        clock: Clock = utc_now,
    ) -> None:
        self._credentials = credentials
        self._pipeline = pipeline
        self._scopes = scopes
        self._replay = replay
        self._clock = clock

    def authorized(self, headers: Mapping[str, str]) -> bool:
        expected = self._credentials.get_webhook_secret("mcp")
        supplied = lower_headers(headers).get("authorization", "")
        if not expected or not supplied.startswith("Bearer "):
            return False
        return hmac.compare_digest(expected.encode(), supplied.removeprefix("Bearer ").strip().encode())

    def handle(self, headers: Mapping[str, str], raw_body: bytes) -> AdapterParseResult:
        if not self.authorized(headers):
            return reject(401, "invalid bearer token")
        request = parse_json_object(raw_body)
        if request is None:
            return reject(400, "invalid json-rpc payload")
        response = self.handle_rpc(request)
        if response is None:
            return AdapterParseResult(event=None, http_status=202, response_body={})
        return AdapterParseResult(event=None, http_status=200, response_body=response)

    def handle_rpc(self, request: dict[str, Any]) -> dict[str, Any] | None:
        method = text(request.get("method"))
        req_id = request.get("id")
        if method.startswith("notifications/"):
            return None
        if method == "initialize":
            return _ok(
                req_id,
                {
                    "protocolVersion": _PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "switchboard", "version": __version__},
                },
            )
        if method == "ping":
            return _ok(req_id, {})
        if method == "tools/list":
            return _ok(req_id, {"tools": TOOLS})
        if method == "tools/call":
            params = obj(request.get("params"))
            name = text(params.get("name"))
            args = obj(params.get("arguments"))
            try:
                result = self._call(name, args)
            except ToolError as exc:
                return _ok(req_id, _tool_content({"error": str(exc)}, is_error=True))
            return _ok(req_id, _tool_content(result, is_error=result.get("status") == "invalid"))
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"unknown method {method}"}}

    def _call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if name == "submit_work":
            return self._submit_work(args)
        if name == "post_message":
            return self._post_message(args)
        if name == "decide_approval":
            return self._decide_approval(args)
        if name == "report_execution":
            return self._report_execution(args)
        if name == "replay_delivery":
            return self._replay_delivery(args)
        raise ToolError(f"unknown tool {name!r}")

    def _ingest(self, event: CanonicalEvent) -> dict[str, Any]:
        return dict(describe_ingest(self._pipeline.ingest(event)))

    def _submit_work(self, args: dict[str, Any]) -> dict[str, Any]:
        source = text(args.get("source")) or "mcp"
        if source not in _SUBMIT_SOURCES:
            raise ToolError(f"source must be one of {list(_SUBMIT_SOURCES)}")
        event_type = text(args.get("event_type")) or "work.opened"
        if event_type not in _WORK_TYPES:
            raise ToolError(f"event_type must be one of {list(_WORK_TYPES)}")
        external_id = text(args.get("external_id"))
        metadata: dict[str, Any] = dict(obj(args.get("metadata")))
        metadata.setdefault("work_kind", text(args.get("work_kind")) or "task")
        metadata.setdefault("state", text(args.get("state")) or "open")
        if args.get("task_id"):
            metadata["task_id"] = text(args.get("task_id"))
        return self._ingest(
            CanonicalEvent(
                source=cast(Source, source),
                event_type=cast(EventType, event_type),
                external_id=external_id,
                occurred_at=format_utc(self._clock()),
                scope=self._scopes.fallback(text(args.get("scope")) or None),
                correlation_key=text(args.get("correlation_key")) or f"{source}:{external_id}",
                actor=text(args.get("actor")) or None,
                subject=text(args.get("subject")),
                body=text(args.get("body")),
                metadata=metadata,
            )
        )

    def _post_message(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._ingest(
            CanonicalEvent(
                source="mcp",
                event_type="conversation.message",
                external_id=text(args.get("external_id")) or f"msg:{new_ulid()}",
                occurred_at=format_utc(self._clock()),
                scope=self._scopes.fallback(text(args.get("scope")) or None),
                correlation_key=text(args.get("correlation_key")) or None,
                actor=text(args.get("actor")) or None,
                body=text(args.get("body")),
                metadata={"channel": text(args.get("channel")) or "mcp"},
            )
        )

    def _decide_approval(self, args: dict[str, Any]) -> dict[str, Any]:
        approval_request_id = text(args.get("approval_request_id"))
        decision = text(args.get("decision"))
        return self._ingest(
            CanonicalEvent(
                source="mcp",
                event_type="approval.decided",
                external_id=f"approval:{approval_request_id}",
                occurred_at=format_utc(self._clock()),
                scope=self._scopes.fallback(text(args.get("scope")) or None),
                correlation_key=text(args.get("correlation_key")) or None,
                actor=text(args.get("actor")) or None,
                body=text(args.get("comment")) or decision,
                metadata={
                    "approval_request_id": approval_request_id,
                    "decision": decision,
                    "session_id": text(args.get("session_id")),
                    "channel": "mcp",
                },
            )
        )

    # No default_scope fallback: the report belongs to the node's inbox, and a missing scope fails validation.
    def _report_execution(self, args: dict[str, Any]) -> dict[str, Any]:
        # Taskboard's notifications keep source="taskboard", so Taskboard can drop them as its own.
        source = text(args.get("source")) or "hibernaut"
        if source not in _REPORT_SOURCES:
            raise ToolError(f"source must be one of {list(_REPORT_SOURCES)}")
        return self._ingest(
            CanonicalEvent(
                source=cast(Source, source),
                event_type="execution.reported",
                external_id=text(args.get("external_id")),
                occurred_at=normalize_utc(text(args.get("occurred_at")) or None, self._clock()),
                scope=text(args.get("scope")),
                correlation_key=text(args.get("correlation_key")) or None,
                actor=text(args.get("actor")) or source,
                subject=text(args.get("subject")),
                body=text(args.get("body")),
                metadata=dict(obj(args.get("metadata"))),
            )
        )

    def _replay_delivery(self, args: dict[str, Any]) -> dict[str, Any]:
        event_id = text(args.get("event_id"))
        consumer_id = text(args.get("consumer_id"))
        try:
            row = self._replay(event_id, consumer_id)
        except ReplayError as exc:
            raise ToolError(str(exc)) from exc
        return {
            "status": "replayed",
            "event_id": row.event_id,
            "consumer_id": row.consumer_id,
            "delivery_status": row.status,
            "attempts": row.attempts,
        }


def _ok(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _tool_content(payload: Mapping[str, Any], *, is_error: bool) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}],
        "structuredContent": dict(payload),
        "isError": is_error,
    }
