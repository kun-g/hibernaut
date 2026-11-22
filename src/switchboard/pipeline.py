from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any

from switchboard.addressing import address_target
from switchboard.config import DeliveryConfig
from switchboard.envelope import (
    CanonicalEvent,
    EnvelopeError,
    new_ulid,
    parse_utc,
    utc_now,
    validate_envelope,
)
from switchboard.ports import (
    AuditSinkPort,
    ConsumerHandler,
    DeliverOutcome,
    IngestResult,
    NoopAuditSink,
    OutboundGatewayPort,
    OutboundMessage,
    safe_audit,
)
from switchboard.router import Router
from switchboard.store_sqlite import DeliveryRow, DueDelivery, SwitchboardStore

Clock = Callable[[], datetime]


class InboundPipeline:
    def __init__(
        self,
        store: SwitchboardStore,
        *,
        router: Router | None = None,
        audit: AuditSinkPort | None = None,
        clock: Clock = utc_now,
    ) -> None:
        self._store = store
        self._router = router or Router()
        self._audit: AuditSinkPort = audit or NoopAuditSink()
        self._clock = clock
        self._listeners: list[Callable[[], None]] = []

    def on_accepted(self, listener: Callable[[], None]) -> None:
        self._listeners.append(listener)

    def ingest(self, event: CanonicalEvent) -> IngestResult:
        try:
            validate_envelope(event)
        except EnvelopeError as exc:
            self.record_rejection(
                stage="validate",
                http_status=400,
                source=event.source,
                external_id=event.external_id,
                reason=str(exc),
            )
            return IngestResult(status="invalid", problems=tuple(exc.problems))

        consumers = self._router.resolve(event)
        inserted = self._store.insert_inbound(event, consumers, self._clock())
        if not inserted:
            existing = self._store.find_inbound(event.source, event.external_id)
            safe_audit(
                self._audit,
                "ingest",
                "duplicate",
                source=event.source,
                event_id=None if existing is None else existing.event_id,
                external_id=event.external_id,
            )
            return IngestResult(
                status="duplicate",
                event_id=None if existing is None else existing.event_id,
                existing=existing,
            )

        safe_audit(
            self._audit,
            "ingest",
            "accepted",
            source=event.source,
            event_id=event.event_id,
            external_id=event.external_id,
            detail={"consumers": list(consumers), "event_type": event.event_type},
        )
        for listener in self._listeners:
            listener()
        return IngestResult(status="accepted", event_id=event.event_id, consumers=consumers)

    # 401 / 400 rejections never reach inbound_events; they leave only an audit trail.
    def record_rejection(
        self,
        *,
        stage: str,
        http_status: int,
        source: str | None,
        reason: str,
        external_id: str | None = None,
    ) -> None:
        print(
            f"[switchboard] rejected {source or '-'} at {stage} ({http_status}): {reason}",
            file=sys.stderr,
        )
        safe_audit(
            self._audit,
            stage,
            f"rejected_{http_status}",
            source=source,
            external_id=external_id,
            detail={"reason": reason, "http_status": http_status},
        )


class InProcessTransport:
    def __init__(self) -> None:
        self._handlers: dict[str, ConsumerHandler] = {}
        self._lock = threading.Lock()

    def register(self, consumer_id: str, handler: ConsumerHandler) -> None:
        with self._lock:
            self._handlers[consumer_id] = handler

    def unregister(self, consumer_id: str) -> None:
        with self._lock:
            self._handlers.pop(consumer_id, None)

    def deliver(self, consumer_id: str, event: CanonicalEvent) -> DeliverOutcome:
        with self._lock:
            handler = self._handlers.get(consumer_id)
        if handler is None:
            return DeliverOutcome(status="offline")
        try:
            status = handler(event)
        except TimeoutError as exc:
            return DeliverOutcome(status="timeout", error=repr(exc))
        except Exception as exc:  # noqa: BLE001
            return DeliverOutcome(status="nack", error=repr(exc))
        return DeliverOutcome(status=status or "acked")


class DeliveryWorker:
    def __init__(
        self,
        store: SwitchboardStore,
        transport: InProcessTransport,
        *,
        config: DeliveryConfig | None = None,
        gateway: OutboundGatewayPort | None = None,
        audit: AuditSinkPort | None = None,
        clock: Clock = utc_now,
        idle_poll_seconds: float = 1.0,
    ) -> None:
        self._store = store
        self._transport = transport
        self._config = config or DeliveryConfig()
        self._gateway = gateway
        self._audit: AuditSinkPort = audit or NoopAuditSink()
        self._clock = clock
        self._idle_poll_seconds = idle_poll_seconds
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def wake(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="switchboard-delivery", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None

    def replay_delivery(self, event_id: str, consumer_id: str) -> DeliveryRow:
        row = self._store.replay_delivery(event_id, consumer_id, self._clock())
        safe_audit(self._audit, "delivery", "replayed", event_id=event_id, consumer_id=consumer_id)
        self.wake()
        return row

    def run_once(self, max_rounds: int = 1000) -> int:
        delivered = 0
        for _ in range(max_rounds):
            due = self._store.due_deliveries(self._clock())
            if not due:
                break
            for item in due:
                self._deliver(item)
                delivered += 1
        return delivered

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                print(f"[switchboard] delivery loop error: {exc!r}", file=sys.stderr)
            self._wake.wait(self._sleep_seconds())
            self._wake.clear()

    def _sleep_seconds(self) -> float:
        upcoming = self._store.next_delivery_at()
        if upcoming is None:
            return self._idle_poll_seconds
        delta = (parse_utc(upcoming) - self._clock()).total_seconds()
        return max(0.0, min(self._idle_poll_seconds, delta))

    def _deliver(self, item: DueDelivery) -> None:
        row = item.row
        now = self._clock()
        try:
            outcome = self._transport.deliver(row.consumer_id, item.event)
        except Exception as exc:  # noqa: BLE001
            outcome = DeliverOutcome(status="nack", error=repr(exc))

        if outcome.status == "acked":
            self._store.update_delivery(
                row.event_id,
                row.consumer_id,
                status="acked",
                attempts=row.attempts + 1,
                next_retry_at=now,
                last_error=None,
                now=now,
            )
            safe_audit(
                self._audit, "delivery", "acked", event_id=row.event_id, consumer_id=row.consumer_id
            )
            return

        if outcome.status == "offline":
            # Phase 2 relay consumers may be disconnected; that is not a failed attempt.
            self._store.update_delivery(
                row.event_id,
                row.consumer_id,
                status="pending",
                attempts=row.attempts,
                next_retry_at=now + timedelta(seconds=self._backoff(0)),
                last_error=row.last_error,
                now=now,
            )
            return

        attempts = row.attempts + 1
        error = outcome.error or outcome.status
        if attempts >= self._config.max_attempts:
            self._store.update_delivery(
                row.event_id,
                row.consumer_id,
                status="dead_letter",
                attempts=attempts,
                next_retry_at=now,
                last_error=error,
                now=now,
            )
            safe_audit(
                self._audit,
                "delivery",
                "dead_letter",
                event_id=row.event_id,
                consumer_id=row.consumer_id,
                detail={"attempts": attempts, "error": error},
            )
            self._alert_dead_letter(item.event, row.consumer_id, attempts, error, now)
            return

        self._store.update_delivery(
            row.event_id,
            row.consumer_id,
            status="retry",
            attempts=attempts,
            next_retry_at=now + timedelta(seconds=self._backoff(attempts - 1)),
            last_error=error,
            now=now,
        )
        safe_audit(
            self._audit,
            "delivery",
            outcome.status,
            event_id=row.event_id,
            consumer_id=row.consumer_id,
            detail={"attempts": attempts, "error": error},
        )

    def _backoff(self, index: int) -> float:
        schedule = self._config.retry_backoff_seconds
        return float(schedule[min(index, len(schedule) - 1)])

    def _alert_dead_letter(
        self,
        event: CanonicalEvent,
        consumer_id: str,
        attempts: int,
        error: str,
        now: datetime,
    ) -> None:
        channel = self._config.dead_letter_alert_channel
        address = self._config.dead_letter_alert_target
        message = (
            f"Switchboard dead letter: {event.event_type} {event.event_id} -> {consumer_id} "
            f"(scope {event.scope}) after {attempts} attempts: {error}"
        )
        if self._gateway is None or channel is None or address is None:
            print(f"[switchboard] {message}", file=sys.stderr)
            return
        alert = OutboundMessage(
            msg_id=f"msg_{new_ulid()}",
            idempotency_key=f"dead_letter:{event.event_id}:{consumer_id}:{now.isoformat()}",
            channel=channel,
            kind="text",
            target=address_target(channel, address),
            correlation_key=event.correlation_key,
            title="Switchboard dead letter",
            body_md=(
                f"{message}\n\nReplay with MCP `replay_delivery` "
                f"(event_id={event.event_id}, consumer_id={consumer_id})."
            ),
        )
        try:
            self._gateway.deliver(alert)
        except Exception as exc:  # noqa: BLE001
            print(f"[switchboard] dead-letter alert failed: {exc!r}; {message}", file=sys.stderr)


class InProcessEventBus:
    def __init__(
        self,
        pipeline: InboundPipeline,
        transport: InProcessTransport,
        worker: DeliveryWorker | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._transport = transport
        self._worker = worker

    def publish(self, event: CanonicalEvent) -> IngestResult:
        return self._pipeline.ingest(event)

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        self._transport.register(consumer_id, handler)
        if self._worker is not None:
            self._worker.wake()


def describe_ingest(result: IngestResult) -> Mapping[str, Any]:
    return {
        "status": result.status,
        "event_id": result.event_id,
        "duplicate": result.status == "duplicate",
        "consumers": list(result.consumers),
        "problems": list(result.problems),
    }
