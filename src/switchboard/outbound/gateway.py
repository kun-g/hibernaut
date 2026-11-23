from __future__ import annotations

import json
import sys
import threading
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, cast

from switchboard.addressing import address_target, reply_target
from switchboard.config import OutboundConfig
from switchboard.envelope import CanonicalEvent, new_ulid, parse_utc, utc_now
from switchboard.feishu_cards import render_approval_card
from switchboard.pipeline import Clock
from switchboard.ports import (
    AuditSinkPort,
    DeliverStatus,
    NoopAuditSink,
    OutboundChannelAdapter,
    OutboundEnqueueResult,
    OutboundMessage,
    SendResult,
    safe_audit,
)
from switchboard.store_sqlite import OutboundRow, SwitchboardStore

# Taskboard task actions that only sync state; reported without a result (outcome_kind other than emit) they are
# internal and never posted to a person.
_SYNC_TASK_ACTIONS = frozenset({"claim", "update_stage", "release"})


def is_internal_task_sync(meta: Mapping[str, Any]) -> bool:
    # done: a state sync with nothing to tell a person. emit: a real result that also carries a task action (e.g.
    # a failed Emit releasing its task), still delivered. suspend: the approval card, always delivered.
    outcome = meta.get("outcome_kind")
    if outcome == "done":
        return True
    return outcome not in ("emit", "suspend") and meta.get("task_action") in _SYNC_TASK_ACTIONS


class TokenBucket:
    def __init__(self, rate_per_minute: int) -> None:
        self._capacity = float(max(1, rate_per_minute))
        self._per_second = self._capacity / 60.0
        self._state: dict[str, tuple[float, datetime]] = {}

    # Returns 0 when a token was taken, otherwise the seconds until one is available.
    def take(self, key: str, now: datetime) -> float:
        tokens, last = self._state.get(key, (self._capacity, now))
        elapsed = max(0.0, (now - last).total_seconds())
        tokens = min(self._capacity, tokens + elapsed * self._per_second)
        if tokens >= 1.0:
            self._state[key] = (tokens - 1.0, now)
            return 0.0
        self._state[key] = (tokens, now)
        return (1.0 - tokens) / self._per_second


class OutboundGateway:
    def __init__(
        self,
        store: SwitchboardStore,
        adapters: Mapping[str, OutboundChannelAdapter],
        *,
        config: OutboundConfig | None = None,
        audit: AuditSinkPort | None = None,
        clock: Clock = utc_now,
        idle_poll_seconds: float = 1.0,
    ) -> None:
        self._store = store
        self._adapters = dict(adapters)
        self._config = config or OutboundConfig()
        self._audit: AuditSinkPort = audit or NoopAuditSink()
        self._clock = clock
        self._bucket = TokenBucket(self._config.rate_limit_per_target_rpm)
        self._idle_poll_seconds = idle_poll_seconds
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def deliver(self, msg: OutboundMessage) -> OutboundEnqueueResult:
        msg_id, inserted = self._store.insert_outbound(msg, self._clock())
        safe_audit(
            self._audit,
            "outbound",
            "enqueued" if inserted else "duplicate",
            detail={"msg_id": msg_id, "channel": msg.channel, "idempotency_key": msg.idempotency_key},
        )
        if inserted:
            self._wake.set()
        return OutboundEnqueueResult(msg_id=msg_id, duplicate=not inserted)

    def consume_event(self, event: CanonicalEvent) -> DeliverStatus | None:
        msg = self.message_for_report(event)
        if msg is None:
            safe_audit(
                self._audit,
                "outbound",
                "no_target",
                event_id=event.event_id,
                detail={"correlation_key": event.correlation_key},
                correlation_key=event.correlation_key,
                scope=event.scope,
            )
            return None
        self.deliver(msg)
        return None

    def message_for_report(self, event: CanonicalEvent) -> OutboundMessage | None:
        if event.event_type != "execution.reported":
            return None
        meta = event.metadata
        outcome = str(meta.get("outcome_kind", ""))
        key = event.correlation_key
        idempotency_key = f"report:{event.event_id}"
        if is_internal_task_sync(meta):
            return None

        if outcome == "suspend":
            channel = str(meta.get("approval_channel") or self._config.approval_channel)
            address = meta.get("approval_target") or self._config.approval_target
            target: dict[str, Any] | None = None
            if address:
                target = address_target(channel, str(address))
            else:
                origin = self._origin(key)
                if origin is not None and origin[0] == channel:
                    target = origin[1]
            if target is None:
                return None
            card: dict[str, Any] | None = None
            kind: Any = "comment" if channel == "github" else "text"
            if channel == "feishu":
                approval_request_id = meta.get("approval_request_id")
                if not isinstance(approval_request_id, str) or not approval_request_id.strip():
                    return None
                kind = "approval_card"
                card = render_approval_card(
                    title=event.subject,
                    body_md=event.body,
                    approval_request_id=approval_request_id,
                    correlation_key=key or "",
                    scope=event.scope,
                    session_id=str(meta["session_id"]),
                )
            return OutboundMessage(
                msg_id=f"msg_{new_ulid()}",
                idempotency_key=idempotency_key,
                channel=channel,
                kind=kind,
                target=target,
                correlation_key=key,
                title=event.subject,
                body_md=event.body,
                card_payload=card,
            )

        override_channel = meta.get("reply_channel")
        override_address = meta.get("reply_target")
        if override_channel and override_address:
            channel = str(override_channel)
            target = address_target(channel, str(override_address))
        else:
            origin = self._origin(key)
            if origin is None and event.source == "taskboard" and meta.get("notify_kind"):
                origin = self._notify_fallback()
            if origin is None:
                return None
            channel, target = origin
        if channel == "github":
            for label_key in ("add_labels", "remove_labels"):
                labels: Any = meta.get(label_key)
                if isinstance(labels, list | tuple):
                    target[label_key] = [str(label) for label in cast(Sequence[Any], labels)]
        return OutboundMessage(
            msg_id=f"msg_{new_ulid()}",
            idempotency_key=idempotency_key,
            channel=channel,
            kind="comment" if channel == "github" else "text",
            target=target,
            correlation_key=key,
            title=event.subject,
            body_md=event.body,
        )

    def _notify_fallback(self) -> tuple[str, dict[str, Any]] | None:
        # A Taskboard notice with no reply target and no origin that takes replies (e.g. a GitHub issue) still
        # has to reach a person: [outbound] notify_*, else the approval target.
        config = self._config
        if config.notify_target:
            channel = config.notify_channel or config.approval_channel
            return channel, address_target(channel, config.notify_target)
        if config.approval_target:
            return config.approval_channel, address_target(config.approval_channel, config.approval_target)
        return None

    def _origin(self, key: str | None) -> tuple[str, dict[str, Any]] | None:
        origin = reply_target(key)
        if origin is None or origin[0] not in self._config.reply_to_origin:
            return None
        return origin

    def wake(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="switchboard-outbound", daemon=True)
        self._thread.start()

    # From here on no message is claimed; a send in flight still records its result.
    def request_stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def join(self, timeout: float) -> bool:
        if self._thread is not None:
            self._thread.join(timeout)
            if self._thread.is_alive():
                return False
            self._thread = None
        return True

    def stop(self, timeout: float = 5.0) -> bool:
        self.request_stop()
        return self.join(timeout)

    def run_once(self) -> int:
        sent = 0
        for row in self._store.due_outbound(self._clock()):
            if self._stop.is_set():
                break
            if self._send(row):
                sent += 1
        return sent

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                print(f"[switchboard] outbound loop error: {exc!r}", file=sys.stderr)
            # A forced shutdown has closed the store by now.
            if self._stop.is_set():
                return
            self._wake.wait(self._sleep_seconds())
            self._wake.clear()

    def _sleep_seconds(self) -> float:
        upcoming = self._store.next_outbound_at()
        if upcoming is None:
            return self._idle_poll_seconds
        delta = (parse_utc(upcoming) - self._clock()).total_seconds()
        return max(0.0, min(self._idle_poll_seconds, delta))

    def _send(self, row: OutboundRow) -> bool:
        msg = row.message
        now = self._clock()
        bucket_key = f"{msg.channel}:{json.dumps(dict(msg.target), sort_keys=True)}"
        wait = self._bucket.take(bucket_key, now)
        if wait > 0:
            self._reschedule(row, now + timedelta(seconds=wait), row.attempts, row.last_error, now)
            return False

        adapter = self._adapters.get(msg.channel)
        if adapter is None:
            result = SendResult(ok=False, error=f"no adapter for channel {msg.channel!r}", retryable=False)
        else:
            try:
                result = adapter.send(msg)
            except Exception as exc:  # noqa: BLE001
                result = SendResult(ok=False, error=repr(exc), retryable=True)

        attempts = row.attempts + 1
        if result.ok:
            self._store.update_outbound(
                msg.msg_id,
                status="sent",
                attempts=attempts,
                next_retry_at=now,
                platform_msg_id=result.platform_msg_id,
                last_error=None,
                now=now,
            )
            safe_audit(self._audit, "outbound", "sent", detail={"msg_id": msg.msg_id})
            return True

        if not result.retryable or attempts >= self._config.max_retry_attempts:
            self._store.update_outbound(
                msg.msg_id,
                status="failed",
                attempts=attempts,
                next_retry_at=now,
                platform_msg_id=None,
                last_error=result.error,
                now=now,
            )
            safe_audit(
                self._audit,
                "outbound",
                "failed",
                detail={"msg_id": msg.msg_id, "error": result.error, "attempts": attempts},
            )
            print(
                f"[switchboard] outbound {msg.msg_id} to {msg.channel} failed: {result.error}",
                file=sys.stderr,
            )
            return False

        schedule = self._config.retry_backoff_seconds
        delay = float(schedule[min(attempts - 1, len(schedule) - 1)])
        self._reschedule(row, now + timedelta(seconds=delay), attempts, result.error, now)
        return False

    def _reschedule(
        self,
        row: OutboundRow,
        when: datetime,
        attempts: int,
        error: str | None,
        now: datetime,
    ) -> None:
        self._store.update_outbound(
            row.message.msg_id,
            status="pending",
            attempts=attempts,
            next_retry_at=when,
            platform_msg_id=None,
            last_error=error,
            now=now,
        )
