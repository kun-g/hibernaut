from __future__ import annotations

from collections.abc import Callable

from agent_runtime.core.models import Event, SessionRef
from agent_runtime.core.ports import SessionStorePort
from agent_runtime.engine.forwarding import closing_event, is_open


def close_session(
    target: SessionRef, sessions: SessionStorePort, queue: Callable[[Event], object], *, cause: str
) -> None:
    # The caller holds the lease of target's inbox. What the session owes or keeps for a decision ends with it, once:
    # the events go first, so a crash in between repeats them and they are deduplicated.
    children = sessions.list_sessions(parent_session_id=target.session_id)
    held = [c for c in children if c.inbox_id == target.inbox_id and c.forward is not None and c.forward.held is not None]
    for session in (target, *held):
        ending = closing_event(session, sessions)
        if ending is not None:
            queue(ending)
        if session is target or is_open(session):
            sessions.close_session(session.session_id)
    # Work forwarded to other inboxes closes with it, each child under its own inbox's lease.
    for child in children:
        if child.inbox_id != target.inbox_id and is_open(child):
            queue(Event(
                inbox_id=child.inbox_id,
                source="session:close",
                external_id=f"close:{cause}:{child.session_id}",
                payload={"action": "close_session"},
                session_id=child.session_id,
            ))
