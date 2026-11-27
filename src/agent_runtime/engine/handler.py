from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Any

from agent_runtime.core.models import INTERNAL_SOURCES, Event, EventStatus
from agent_runtime.core.ports import InboxPort


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    status: EventStatus
    duplicate: bool


class ReservedSource(ValueError):
    pass


# Set on what another host put in, whatever it sent itself.
REMOTE_KEY = "_remote"


class RemoteRefused(ValueError):
    pass


class ApprovalRefused(ValueError):
    pass


def decides(payload: Mapping[str, Any]) -> bool:
    # Whatever names a pending request resumes it, whichever source or event type it claims.
    return payload.get("approval_request_id") is not None or payload.get("event_type") in (
        "approval.decided", "suspend.elapsed")


class EventHandler:
    def __init__(
        self,
        inbox: InboxPort,
        on_wake: Callable[[str], None] | None = None,
    ) -> None:
        self._inbox = inbox
        self._on_wake = on_wake
        self._local_only: Callable[[str], bool] = lambda _inbox_id: False
        self._deciders: Callable[[str], Collection[str] | None] = lambda _inbox_id: None

    def keep_local(self, local_only: Callable[[str], bool]) -> None:
        # The inboxes that take events put in on this host only.
        self._local_only = local_only

    def reserve_approvals(self, deciders: Callable[[str], Collection[str] | None]) -> None:
        # Who alone decides the approvals of an inbox; None: anyone who reaches it. A caller names the decider only
        # where it knows who decided, as Switchboard's verified Feishu card click does; nothing else decides there.
        self._deciders = deciders

    def ingest(
        self,
        *,
        inbox_id: str,
        source: str,
        external_id: str,
        payload: dict[str, Any],
        session_id: str | None = None,
        correlation_key: str | None = None,
        remote: bool = False,
        decided_by: str | None = None,
    ) -> IngestResult:
        # The public entrance: its callers name the source themselves.
        if remote and self._local_only(inbox_id):
            raise RemoteRefused(f"inbox {inbox_id!r} takes events from this host only")
        if source.strip() in INTERNAL_SOURCES:
            raise ReservedSource(f"source {source!r} is reserved for events Hibernaut queues itself")
        if decides(payload):
            # Refused before it is stored, so the (source, external_id) of a real decision stays free.
            deciders = self._deciders(inbox_id)
            if deciders is not None and (remote or decided_by is None or decided_by not in deciders):
                raise ApprovalRefused(f"approvals in inbox {inbox_id!r} count only from its approvers' own clicks")
        return self.queue(Event(
            inbox_id=inbox_id,
            source=source,
            external_id=external_id,
            payload={**payload, REMOTE_KEY: True} if remote else dict(payload),
            session_id=session_id,
            correlation_key=correlation_key,
        ))

    def queue(self, event: Event) -> IngestResult:
        # For events this process built itself, internal sources included; never for what a caller sent in.
        stored, is_duplicate = self._inbox.append(event)
        if not is_duplicate and self._on_wake is not None:
            self._on_wake(event.inbox_id)
        return IngestResult(
            event_id=stored.event_id,
            status=stored.status,
            duplicate=is_duplicate,
        )

    def wake(self, inbox_id: str) -> None:
        # For events that become pending without an append, such as a control-plane replay.
        if self._on_wake is not None:
            self._on_wake(inbox_id)

    def has_open_event(self, *, inbox_id: str, source: str, external_id: str) -> bool:
        return self._inbox.has_open_event(inbox_id, source, external_id)
