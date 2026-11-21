from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from agent_runtime.core.models import Event, EventStatus
from agent_runtime.core.ports import InboxPort


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    status: EventStatus
    duplicate: bool


class EventHandler:
    def __init__(
        self,
        inbox: InboxPort,
        on_wake: Callable[[str], None] | None = None,
    ) -> None:
        self._inbox = inbox
        self._on_wake = on_wake

    def ingest(
        self,
        *,
        inbox_id: str,
        source: str,
        external_id: str,
        payload: dict[str, Any],
        session_id: str | None = None,
    ) -> IngestResult:
        event = Event(
            inbox_id=inbox_id,
            source=source,
            external_id=external_id,
            payload=dict(payload),
            session_id=session_id,
        )
        stored, is_duplicate = self._inbox.append(event)
        if not is_duplicate and self._on_wake is not None:
            self._on_wake(inbox_id)
        return IngestResult(
            event_id=stored.event_id,
            status=stored.status,
            duplicate=is_duplicate,
        )
