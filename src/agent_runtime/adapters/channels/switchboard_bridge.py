from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from switchboard.envelope import CanonicalEvent
from switchboard.ports import DeliverStatus

from agent_runtime.core.models import TASK_SYNC_KIND
from agent_runtime.core.ports import ChannelPort

SWITCHBOARD_SOURCE_PREFIX = "sb:"

# Metadata keys the engine reads from the top level of an event payload (approval resume, reply routing,
# worktree sharing). They are also kept verbatim under payload["metadata"].
_LIFTED_METADATA_KEYS = (
    "approval_request_id",
    "decision",
    "work_key",
    "reply_channel",
    "reply_target",
    # Taskboard work.ready fields; the worker reads task_id, trace_id and hop_count to report back.
    "task_id",
    "trace_id",
    "hop_count",
    "dispatch_seq",
    "assignee",
    "task_type",
    "priority",
)

_PASSTHROUGH_REPORT_KEYS = (
    "approval_request_id",
    "reply_channel",
    "reply_target",
    "task_id",
    "executor_id",
    "error_kind",
    "notify_channels",
    "approval_channel",
    "approval_target",
    "task_action",
    "stage",
    "trace_id",
    "hop_count",
)

_OUTCOME_BY_KIND: Mapping[str, str] = {
    "emit": "emit",
    "approval_request": "suspend",
    "silent": "silent",
    TASK_SYNC_KIND: "done",
}


class IngestSink(Protocol):
    def ingest(
        self,
        *,
        inbox_id: str,
        source: str,
        external_id: str,
        payload: dict[str, Any],
        session_id: str | None = None,
        correlation_key: str | None = None,
    ) -> object: ...


def canonical_to_payload(event: CanonicalEvent) -> dict[str, Any]:
    metadata = dict(event.metadata)
    payload: dict[str, Any] = {
        "event_type": event.event_type,
        "subject": event.subject,
        "title": event.subject,
        "body": event.body,
        "actor": event.actor,
        "external_id": event.external_id,
        "occurred_at": event.occurred_at,
        "switchboard_event_id": event.event_id,
        "metadata": metadata,
    }
    for key in _LIFTED_METADATA_KEYS:
        value = metadata.get(key)
        if value is not None and value != "":
            payload[key] = value
    if event.event_type == "approval.decided":
        decision = metadata.get("decision")
        if decision not in ("approve", "reject"):
            raise ValueError("approval.decided needs an approve or reject decision")
        payload["approved"] = decision == "approve"
    preferred = metadata.get("preferred_executor")
    if isinstance(preferred, str) and preferred:
        payload.setdefault("executor_id", preferred)
    if event.source == "github" and "labels" in metadata:
        payload["labels"] = metadata["labels"]
    if event.event_type == "work.closed":
        # The worker closes the session under the inbox lease, so the close cannot race a running Run,
        # and its existing work_key reference count decides whether the worktree is reclaimed.
        payload["action"] = "close_session"
    return payload


class SwitchboardInboundBridge:
    def __init__(self, handler: IngestSink) -> None:
        self._handler = handler

    def handle_canonical_event(self, event: CanonicalEvent) -> DeliverStatus:
        session_id = event.metadata.get("session_id")
        try:
            # event_id is Switchboard's global fingerprint, so an at-least-once redelivery lands on the
            # inbox dedup key and is recorded as dropped.
            self._handler.ingest(
                inbox_id=event.scope,
                source=f"{SWITCHBOARD_SOURCE_PREFIX}{event.source}",
                external_id=event.event_id,
                payload=canonical_to_payload(event),
                session_id=session_id if isinstance(session_id, str) and session_id else None,
                correlation_key=event.correlation_key,
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[hibernaut] [switchboard] ingest of {event.event_id} ({event.event_type}) failed: {exc!r}",
                file=sys.stderr,
                flush=True,
            )
            return "nack"
        return "acked"


def outcome_kind_for(metadata: Mapping[str, Any]) -> str:
    explicit = metadata.get("outcome_kind")
    if isinstance(explicit, str) and explicit:
        return explicit
    return _OUTCOME_BY_KIND.get(str(metadata.get("kind", "")), "error")


def _external_id(inbox_id: str, content: str, metadata: Mapping[str, Any]) -> str:
    message_id = metadata.get("message_id")
    if isinstance(message_id, str) and message_id:
        return f"outbox:{message_id}"
    # Deterministic, so an Outbox retry after a successful publish is a Switchboard duplicate.
    digest = hashlib.sha256(
        json.dumps([inbox_id, content, dict(metadata)], sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:32]
    return f"outbox:{digest}"


class SwitchboardChannelAdapter:
    def __init__(
        self,
        publish: Callable[[CanonicalEvent], Any],
        *,
        fallback: ChannelPort | None = None,
    ) -> None:
        self._publish = publish
        self._fallback = fallback

    def build_report(self, inbox_id: str, content: str, metadata: Mapping[str, Any]) -> CanonicalEvent | None:
        outcome = outcome_kind_for(metadata)
        if outcome == "silent":
            return None
        correlation_key = metadata.get("correlation_key")
        session_id = metadata.get("session_id")
        if not isinstance(correlation_key, str) or not correlation_key:
            if outcome != "suspend":
                raise ValueError("execution.reported needs metadata.correlation_key")
            correlation_key = None
        if not (isinstance(session_id, str) and session_id):
            raise ValueError("execution.reported needs metadata.session_id")
        external_id = _external_id(inbox_id, content, metadata)
        report: dict[str, Any] = {
            "run_id": str(metadata.get("run_id") or external_id),
            "session_id": session_id,
            "outcome_kind": outcome,
        }
        for key in _PASSTHROUGH_REPORT_KEYS:
            value = metadata.get(key)
            if value is not None and value != "":
                report[key] = value
        if "task_id" in report:
            # Taskboard's execution.reported schema requires both; a Run that lost them still reports a valid trace.
            report.setdefault("trace_id", report["run_id"])
            report.setdefault("hop_count", 1)
        legacy_request_id = metadata.get("request_id")
        if outcome == "suspend" and "approval_request_id" not in report and legacy_request_id:
            # Outbox rows written before the worker added approval_request_id carry only request_id.
            report["approval_request_id"] = legacy_request_id
        subject = metadata.get("title") or metadata.get("subject") or "Hibernaut update"
        return CanonicalEvent(
            source="hibernaut",
            event_type="execution.reported",
            external_id=external_id,
            scope=inbox_id,
            correlation_key=correlation_key,
            actor="hibernaut",
            subject=str(subject),
            body=content,
            metadata=report,
        )

    def routes_to_switchboard(self, metadata: Mapping[str, Any]) -> bool:
        if self._fallback is None:
            return True
        key, sid = metadata.get("correlation_key"), metadata.get("session_id")
        if not isinstance(sid, str) or not sid:
            return False
        return outcome_kind_for(metadata) == "suspend" or (isinstance(key, str) and bool(key))

    async def send(self, inbox_id: str, content: str, metadata: dict[str, Any]) -> None:
        if not self.routes_to_switchboard(metadata):
            # Session-less notices (e.g. worktree reclaim alerts) have no origin to answer.
            assert self._fallback is not None
            await self._fallback.send(inbox_id, content, metadata)
            return
        report = self.build_report(inbox_id, content, metadata)
        if report is None:
            return
        result = await asyncio.to_thread(self._publish, report)
        if getattr(result, "status", None) == "invalid":
            problems = getattr(result, "problems", ())
            raise RuntimeError(f"switchboard rejected execution.reported: {'; '.join(map(str, problems))}")
