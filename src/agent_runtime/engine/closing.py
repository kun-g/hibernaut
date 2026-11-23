from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace

from agent_runtime.core.models import CLOSE_SOURCE, Event, ForwardSpec, SessionRef, utc_now_iso
from agent_runtime.core.ports import SessionStorePort
from agent_runtime.engine.forwarding import closing_event, is_open, owner_session_id


def close_session(
    target: SessionRef,
    sessions: SessionStorePort,
    queue: Callable[[Event], object],
    *,
    cause: str,
    closed_parent: str | None = None,
) -> None:
    # The caller holds the lease of target's inbox. What the session owes or keeps for a decision ends with it, once:
    # the events go first, so a crash in between repeats them and they are deduplicated. closed_parent: it closes with
    # that session, so a result it keeps for another one stays.
    held = [
        s for s in sessions.list_sessions(inbox_id=target.inbox_id, status="suspended")
        if s.forward is not None and s.forward.held is not None and s.parent_session_id == target.session_id
        and (closed_parent is None or owner_session_id(s, sessions) == closed_parent)
    ]
    ending = [target, *held]
    told: set[str] = {s.session_id for s in ending}
    for session in ending:
        event = closing_event(session, sessions)
        if event is not None:
            queue(event)
    _close_children(target, sessions, queue, cause, told)
    for session in ending:
        if session is target or is_open(session):
            spec = session.forward or ForwardSpec(target_inbox=session.inbox_id)
            # Kept on the closed session: work it sent out that has no session yet is not started any more.
            sessions.save(replace(
                session, status="closed", correlation_key=None, pending_request_id=None, checkpoint=None,
                updated_at=utc_now_iso(),
                forward=replace(spec, closed=True, pending_returns=(), cancelled_returns=tuple(
                    dict.fromkeys((*spec.cancelled_returns, *spec.pending_returns))
                )),
            ))
    # A session made meanwhile either saw that this one is closed, or is found now.
    _close_children(target, sessions, queue, cause, told)


def _close_children(
    target: SessionRef, sessions: SessionStorePort, queue: Callable[[Event], object], cause: str, told: set[str]
) -> None:
    # Work done for it closes with it, each session by the worker that holds its inbox's lease. Inside its own inbox
    # only work it waits for: a relay there goes on by itself.
    for child in _open_sessions(sessions):
        owes = child.forward is not None and child.forward.return_to is not None
        if child.session_id in told or (child.inbox_id == target.inbox_id and not owes):
            continue
        if owner_session_id(child, sessions) == target.session_id:
            told.add(child.session_id)
            queue(Event(
                inbox_id=child.inbox_id,
                source=CLOSE_SOURCE,
                external_id=f"close:{cause}:{child.session_id}",
                # Checked again there: by then it may work for another session.
                payload={"action": "close_session", "closed_parent": target.session_id},
                session_id=child.session_id,
            ))


def _open_sessions(sessions: SessionStorePort) -> Sequence[SessionRef]:
    return [*sessions.list_sessions(status="active"), *sessions.list_sessions(status="suspended")]
