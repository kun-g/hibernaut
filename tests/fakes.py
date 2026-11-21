"""In-Memory Fakes for all L1 Ports, used by Contract Tests and fast engine unit tests."""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from agent_runtime.core.models import (
    Event,
    OutboxMessage,
    PermissionDecision,
    SessionRef,
    TimerKind,
    TimerSpec,
    new_id,
)

BASELINE_FILES = ("instructions.md", "index.md", "reported.md")


class InMemoryInbox:
    def __init__(self) -> None:
        self._events: list[Event] = []
        self._seen_keys: set[tuple[str, str, str]] = set()

    def append(self, event: Event) -> tuple[Event, bool]:
        dedup_key = (event.inbox_id, event.source, event.external_id)
        if dedup_key in self._seen_keys:
            dropped = replace(event, status="dropped")
            self._events.append(dropped)
            return dropped, True
        self._seen_keys.add(dedup_key)
        stored = replace(event, status="pending")
        self._events.append(stored)
        return stored, False

    def fetch_pending(self, inbox_id: str) -> list[Event]:
        return [
            evt
            for evt in self._events
            if evt.inbox_id == inbox_id and evt.status == "pending"
        ]

    def mark_done(self, event_ids: Sequence[str]) -> None:
        target = set(event_ids)
        self._events = [
            replace(evt, status="done") if evt.event_id in target else evt
            for evt in self._events
        ]


class InMemoryTimerStore:
    def __init__(self) -> None:
        self._timers: dict[str, TimerSpec] = {}

    def schedule(
        self,
        kind: TimerKind,
        due_at: str,
        payload: dict[str, Any],
        recurrence_seconds: int | None = None,
        *,
        inbox_id: str = "default",
        session_id: str | None = None,
    ) -> str:
        timer = TimerSpec(
            inbox_id=inbox_id,
            kind=kind,
            due_at=due_at,
            payload=dict(payload),
            recurrence_seconds=recurrence_seconds,
            session_id=session_id,
            status="pending",
        )
        self._timers[timer.timer_id] = timer
        return timer.timer_id

    def fetch_due(self, now_iso: str) -> list[TimerSpec]:
        due = [
            t
            for t in self._timers.values()
            if t.status == "pending" and t.due_at <= now_iso
        ]
        return sorted(due, key=lambda t: t.due_at)

    def mark_triggered(self, timer_id: str, next_due_at: str | None = None) -> None:
        timer = self._timers[timer_id]
        if timer.kind == "schedule" and next_due_at is not None:
            self._timers[timer_id] = replace(timer, due_at=next_due_at, status="pending")
        else:
            self._timers[timer_id] = replace(timer, status="done")


class InMemoryMemoryStore:
    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self._files: dict[str, str] = {name: "" for name in BASELINE_FILES}
        if initial:
            self._files.update(initial)

    def load_baseline(self) -> dict[str, str]:
        return {name: self._files.get(name, "") for name in BASELINE_FILES}

    def read_on_demand(self, path: str) -> str:
        normalized = path.lstrip("/")
        if normalized not in self._files:
            raise FileNotFoundError(f"Memory file not found: {normalized}")
        return self._files[normalized]

    def write(self, path: str, content: str) -> None:
        normalized = path.lstrip("/")
        if ".." in normalized.split("/"):
            raise ValueError(f"Invalid memory path traversal: {path}")
        self._files[normalized] = content


class InMemoryOutbox:
    def __init__(self) -> None:
        self._messages: dict[str, OutboxMessage] = {}
        self._order: list[str] = []
        self.last_errors: dict[str, str] = {}

    def enqueue(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> OutboxMessage:
        msg = OutboxMessage(
            message_id=new_id("msg"),
            inbox_id=inbox_id,
            content=content,
            status="pending",
            attempts=0,
            metadata=dict(metadata or {}),
        )
        self._messages[msg.message_id] = msg
        self._order.append(msg.message_id)
        return msg

    def fetch_pending(self, limit: int = 20) -> list[OutboxMessage]:
        pending = [
            self._messages[mid]
            for mid in self._order
            if self._messages[mid].status == "pending"
        ]
        return pending[:limit]

    def mark_sent(self, message_id: str) -> None:
        msg = self._messages[message_id]
        self._messages[message_id] = replace(msg, status="sent")

    def mark_failed(self, message_id: str, error: str) -> None:
        msg = self._messages[message_id]
        self.last_errors[message_id] = error
        self._messages[message_id] = replace(msg, attempts=msg.attempts + 1)


class InMemoryLock:
    def __init__(self, clock: Any = time.monotonic) -> None:
        self._clock = clock
        self._leases: dict[str, tuple[str, float]] = {}

    def acquire(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        now = float(self._clock())
        current = self._leases.get(inbox_id)
        if current is None or current[1] <= now or current[0] == worker_id:
            self._leases[inbox_id] = (worker_id, now + ttl_seconds)
            return True
        return False

    def renew(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        now = float(self._clock())
        current = self._leases.get(inbox_id)
        if current is not None and current[0] == worker_id and current[1] > now:
            self._leases[inbox_id] = (worker_id, now + ttl_seconds)
            return True
        return False

    def release(self, inbox_id: str, worker_id: str) -> None:
        current = self._leases.get(inbox_id)
        if current is not None and current[0] == worker_id:
            del self._leases[inbox_id]


class InMemorySessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionRef] = {}

    def get(self, session_id: str) -> SessionRef | None:
        return self._sessions.get(session_id)

    def find_by_request_id(self, request_id: str) -> SessionRef | None:
        for session in self._sessions.values():
            if session.pending_request_id == request_id:
                return session
        return None

    def find_by_correlation_key(self, inbox_id: str, correlation_key: str) -> SessionRef | None:
        matches = [
            s
            for s in self._sessions.values()
            if s.inbox_id == inbox_id and s.correlation_key == correlation_key
        ]
        if not matches:
            return None
        matches.sort(key=lambda s: s.updated_at, reverse=True)
        return matches[0]

    def save(self, session: SessionRef) -> None:
        if session.correlation_key is not None:
            for sid, existing in list(self._sessions.items()):
                if (
                    sid != session.session_id
                    and existing.inbox_id == session.inbox_id
                    and existing.correlation_key == session.correlation_key
                ):
                    self._sessions[sid] = replace(existing, correlation_key=None)
        self._sessions[session.session_id] = session


class InMemoryPermission:
    def __init__(self, rules: dict[str, PermissionDecision] | None = None) -> None:
        self._rules = dict(rules or {})

    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
        return self._rules.get(tool_name, PermissionDecision(action="allow", reason="default allow"))


class RecordingChannel:
    def __init__(self, fail_times: int = 0) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self._fail_remaining = fail_times

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise RuntimeError("Simulated channel delivery failure")
        self.sent.append((inbox_id, content, dict(metadata)))
