from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from switchboard.envelope import CanonicalEvent

DeliverStatus = Literal["acked", "nack", "offline", "timeout"]
OutboundKind = Literal["text", "approval_card", "comment"]
IngestStatus = Literal["accepted", "duplicate", "invalid"]
# None lets a sink infer it from the outcome.
AuditSeverity = Literal["info", "warn", "error"]


@dataclass(frozen=True, slots=True)
class AdapterParseResult:
    event: CanonicalEvent | None
    http_status: int = 200
    response_body: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    event_id: str | None = None
    consumers: tuple[str, ...] = ()
    problems: tuple[str, ...] = ()
    # For a duplicate: the event stored first, so adapters can answer with its content.
    existing: CanonicalEvent | None = None


class InboundAdapter(Protocol):
    def handle(self, headers: Mapping[str, str], raw_body: bytes) -> AdapterParseResult: ...


@runtime_checkable
class IngestAwareAdapter(Protocol):
    def finalize(self, parsed: AdapterParseResult, ingest: IngestResult) -> AdapterParseResult: ...


ConsumerHandler = Callable[[CanonicalEvent], DeliverStatus | None]


class EventBusPort(Protocol):
    def publish(self, event: CanonicalEvent) -> IngestResult: ...

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliverOutcome:
    status: DeliverStatus
    error: str | None = None


class ConsumerTransport(Protocol):
    def deliver(self, consumer_id: str, event: CanonicalEvent) -> DeliverOutcome: ...


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    msg_id: str
    idempotency_key: str
    channel: str
    kind: OutboundKind
    target: Mapping[str, Any]
    correlation_key: str | None
    title: str
    body_md: str
    card_payload: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class OutboundEnqueueResult:
    msg_id: str
    duplicate: bool


class OutboundGatewayPort(Protocol):
    def deliver(self, msg: OutboundMessage) -> OutboundEnqueueResult: ...


@dataclass(frozen=True, slots=True)
class SendResult:
    ok: bool
    platform_msg_id: str | None = None
    error: str | None = None
    retryable: bool = True


class OutboundChannelAdapter(Protocol):
    def send(self, msg: OutboundMessage) -> SendResult: ...


class CredentialBrokerPort(Protocol):
    def get_webhook_secret(self, channel: str) -> str | None: ...

    def issue_ephemeral(self, channel: str) -> str | None: ...


# correlation_key, scope, trace_id and severity are optional ledger fields (ADR-0001 AuditRecord); sinks that do not
# store them ignore them, so existing callers and sinks are unaffected.
class AuditSinkPort(Protocol):
    def emit_audit(
        self,
        stage: str,
        outcome: str,
        *,
        source: str | None = None,
        event_id: str | None = None,
        external_id: str | None = None,
        consumer_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
        correlation_key: str | None = None,
        scope: str | None = None,
        trace_id: str | None = None,
        severity: AuditSeverity | None = None,
    ) -> None: ...


class NoopAuditSink:
    def emit_audit(
        self,
        stage: str,
        outcome: str,
        *,
        source: str | None = None,
        event_id: str | None = None,
        external_id: str | None = None,
        consumer_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
        correlation_key: str | None = None,
        scope: str | None = None,
        trace_id: str | None = None,
        severity: AuditSeverity | None = None,
    ) -> None:
        return None


@dataclass(slots=True)
class RecordingAuditSink:
    records: list[dict[str, Any]] = field(default_factory=lambda: [])

    def emit_audit(
        self,
        stage: str,
        outcome: str,
        *,
        source: str | None = None,
        event_id: str | None = None,
        external_id: str | None = None,
        consumer_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
        correlation_key: str | None = None,
        scope: str | None = None,
        trace_id: str | None = None,
        severity: AuditSeverity | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "stage": stage,
            "outcome": outcome,
            "source": source,
            "event_id": event_id,
            "external_id": external_id,
            "consumer_id": consumer_id,
            "detail": dict(detail or {}),
        }
        # Only when given, so a record from a caller that passes none keeps its old shape.
        ledger_fields = {"correlation_key": correlation_key, "scope": scope, "trace_id": trace_id, "severity": severity}
        record.update({key: value for key, value in ledger_fields.items() if value is not None})
        self.records.append(record)


# Audit is a side channel: a failing sink degrades to stderr and never breaks the main path.
def safe_audit(
    sink: AuditSinkPort,
    stage: str,
    outcome: str,
    *,
    source: str | None = None,
    event_id: str | None = None,
    external_id: str | None = None,
    consumer_id: str | None = None,
    detail: Mapping[str, Any] | None = None,
    correlation_key: str | None = None,
    scope: str | None = None,
    trace_id: str | None = None,
    severity: AuditSeverity | None = None,
) -> None:
    # Passed only when given: a sink written against the older signature keeps working.
    ledger_fields: dict[str, Any] = {
        key: value
        for key, value in (
            ("correlation_key", correlation_key),
            ("scope", scope),
            ("trace_id", trace_id),
            ("severity", severity),
        )
        if value is not None
    }
    try:
        sink.emit_audit(
            stage,
            outcome,
            source=source,
            event_id=event_id,
            external_id=external_id,
            consumer_id=consumer_id,
            detail=detail,
            **ledger_fields,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"[switchboard] audit sink failed at {stage}/{outcome}: {exc!r}",
            file=sys.stderr,
        )
