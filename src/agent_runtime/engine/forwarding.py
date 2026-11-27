from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, Literal, Never, assert_never, cast

from agent_runtime.core.models import (
    FORWARD_SOURCE,
    FORWARD_TOOL,
    MAX_FORWARD_HOPS,
    RETURN_CANCELLED_SOURCE,
    RETURN_SOURCE,
    DeliveryOutcome,
    Emit,
    Event,
    ForwardRefused,
    ForwardSpec,
    InboxRoute,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    TaskLink,
    delivery_chat_reply,
    explicit_trace_id,
    resolve_trace_id,
    resolve_trigger_event,
)
from agent_runtime.core.ports import PermissionPort, SessionStorePort
from agent_runtime.engine.attempts import ATTEMPT_NOTES_KEY

_AGENT_SOURCES = (FORWARD_SOURCE, RETURN_SOURCE)
# Who put an event here: a person in their own one-to-one chat, a listed person in the group bound to this inbox, or
# somebody on this host.
EventOrigin = Literal["chat", "group", "operator"]
# Which project documents the task needs, and the goal it is recorded under, travel with it; everything else the receiver
# learns from the text.
_INHERITED_KEYS = ("activities", "base_ref", ATTEMPT_NOTES_KEY)


def _text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def event_agent_id(event: Event) -> str | None:
    # executor_id is the old name of agent_id.
    return _text(event.payload.get("agent_id")) or _text(event.payload.get("executor_id"))


def event_type(event: Event) -> str | None:
    return _text(event.payload.get("event_type"))


def _names(raw: object) -> Iterable[str]:
    # "a, b", ["a", "b"] or GitHub label objects.
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    found: list[str] = []
    if isinstance(raw, list):
        for item in cast(list[object], raw):
            name: object = cast(dict[str, object], item).get("name") if isinstance(item, dict) else item
            if isinstance(name, str) and name.strip():
                found.append(name.strip())
    return found


def event_tags(event: Event) -> tuple[str, ...]:
    tags = [*_names(event.payload.get("tags")), *_names(event.payload.get("labels"))]
    for key in ("issue", "pull_request"):
        nested: object = event.payload.get(key)
        if isinstance(nested, dict):
            tags += _names(cast(dict[str, object], nested).get("labels"))
    return tuple(dict.fromkeys(tags))


def is_approval(event: Event) -> bool:
    return _text(event.payload.get("approval_request_id")) is not None


def task_hop_count(event: Event) -> object:
    # A forwarded event's own hop_count counts agent hops; Taskboard's travels beside it.
    return event.payload.get("task_hop_count" if event.source in _AGENT_SOURCES else "hop_count")


def task_trace_id(event: Event) -> object:
    # Likewise trace_id, the chain's; a forward queued before the two were kept apart has only the task's.
    if event.source in _AGENT_SOURCES and "task_trace_id" in event.payload:
        return event.payload["task_trace_id"]
    return event.payload.get("trace_id")


def carried_spec(event: Event) -> ForwardSpec | None:
    if event.source not in _AGENT_SOURCES:
        return None
    payload = event.payload
    hops = payload.get("hop_count")
    forwarded = event.source == FORWARD_SOURCE
    # A missing or malformed count reads as used up, so it can never buy more hops.
    valid = isinstance(hops, int) and not isinstance(hops, bool) and hops >= 1
    return ForwardSpec(
        target_inbox=event.inbox_id,
        target_agent=_text(payload.get("agent_id")),
        return_to=_text(payload.get("return_to")) if forwarded else None,
        hop_count=cast(int, hops) if valid else MAX_FORWARD_HOPS,
        forwarded_from=_text(payload.get("forwarded_from")),
        parent_session_id=_text(payload.get("parent_session_id")),
        question_parents=tuple(str(s) for s in payload.get("question_parents", ())) if forwarded else (),
        reply_channel=_text(payload.get("reply_channel")),
        reply_target=_text(payload.get("reply_target")),
        origin_key=_text(payload.get("origin_key")),
        task_key=_text(payload.get("task_key")),
        return_key=_text(payload.get("return_key")),
        delegation_id=forwarded_piece(event) or None,
        work_key=_text(payload.get("work_key")) if payload.get("forwarded_from") == event.inbox_id else None,
        replayed=is_replay(event),
    )


def is_replay(event: Event) -> bool:
    # Asked for by an operator: this event itself, or the work it continues.
    return _text(event.payload.get("_replay_id")) is not None or (
        event.source in _AGENT_SOURCES and event.payload.get("replayed") is True
    )


def is_supplement(event: Event) -> bool:
    return event.source == FORWARD_SOURCE and event.payload.get("supplement") is True


def forwarded_piece(event: Event) -> str | None:
    # The piece of work a forward asks a result for, "" for one that asks for none, None for any other event. Work
    # handed on inside an inbox without asking for a result is the same work, so it is None too, and so is what is added
    # to a task that was sent before.
    if event.source != FORWARD_SOURCE or is_supplement(event):
        return None
    if _text(event.payload.get("return_to")) is None:
        return None if event.payload.get("forwarded_from") == event.inbox_id else ""
    return _text(event.payload.get("delegation_id")) or event.external_id


def passed_on(event: Event) -> bool:
    # A forward of a piece that the sending session owed itself; a piece it sent out bears the forward's own id.
    piece = forwarded_piece(event)
    return bool(piece) and piece != event.external_id


def returned_piece(event: Event) -> str | None:
    return _text(event.payload.get("delegation_id")) if event.source == RETURN_SOURCE else None


def returned_pieces(events: Sequence[Event]) -> set[str]:
    return {piece for evt in events if (piece := returned_piece(evt))}


def settled_return(event: Event, sessions: SessionStorePort) -> bool:
    # A result of a piece that the session waiting for it has settled already, by another result or a cancel.
    piece = returned_piece(event)
    if piece is None or is_replay(event):
        return False
    asked = ForwardSpec(target_inbox=event.inbox_id, return_to=event.inbox_id,
                        parent_session_id=_text(event.payload.get("parent_session_id")))
    for session in (
        sessions.get(event.session_id) if event.session_id else None,
        sessions.find_by_correlation_key(event.inbox_id, event.correlation_key) if event.correlation_key else None,
        _asking_session(sessions, asked),
    ):
        known = session.forward if session is not None and session.inbox_id == event.inbox_id else None
        if known is not None and known.closed and session is not None and session.status == "closed":
            return True
        # The event that settled it may run again, after a crash before it was marked done.
        if known is not None and (piece in known.cancelled_returns or any(
            done == piece and by != event.event_id for done, by in known.settled_returns
        )):
            return True
    return False


def withdrawn(spec: ForwardSpec, inbox_id: str, sessions: SessionStorePort) -> bool:
    # Work whose session was closed, or that this session stopped waiting for: it does not start, and what comes of it
    # goes nowhere. An operator who replays it means it.
    if spec.replayed:
        return False
    for session in (current_session(sessions, spec.parent_session_id), _asking_session(sessions, spec)):
        known = session.forward if session is not None else None
        if session is None or known is None:
            continue
        if spec.delegation_id is not None and spec.delegation_id in known.cancelled_returns:
            return True
        # A relay inside an inbox that asks for no result is not its session's own any more.
        if known.closed and session.status == "closed" and (spec.delegation_id or session.inbox_id != inbox_id):
            return True
    return False


def withdrawn_forward(event: Event, sessions: SessionStorePort) -> bool:
    spec = carried_spec(event) if event.source == FORWARD_SOURCE else None
    return spec is not None and withdrawn(spec, event.inbox_id, sessions)


def same_piece(event: Event, batch: Sequence[Event]) -> bool:
    # A Run answers one piece: two in one batch would leave all but one of them without a result of their own.
    piece = forwarded_piece(event)
    others = {found for evt in batch if (found := forwarded_piece(evt)) is not None}
    return piece is None or not others or piece in others


def own_task(event: Event, sessions: SessionStorePort) -> Event:
    # A piece sent to a conversation whose session still owes or waits for another one gets a session and worktree of
    # its own.
    piece = forwarded_piece(event)
    if piece is None or not event.correlation_key:
        return event
    if event.session_id:
        # Claimed before: bound to the session it ran in, whose key it keeps.
        holder = sessions.get(event.session_id)
    else:
        holder = sessions.find_by_correlation_key(event.inbox_id, event.correlation_key)
    owed = holder.forward if holder is not None else None
    elsewhere = event.session_id is None and bool(piece) and event.payload.get("forwarded_from") != event.inbox_id
    if owed is not None and owed.delegation_id != piece and (
        owed.return_to is not None or (elsewhere and bool(owed.pending_returns))
    ):
        return replace(event, session_id=None, correlation_key=f"forward:{event.external_id}:{event.correlation_key}")
    if holder is not None and holder.correlation_key:
        return replace(event, correlation_key=holder.correlation_key)
    return event


def run_lineage(events: Sequence[Event], session: SessionRef | None) -> tuple[str, str]:
    # The trace of this Run and the event it descends from.
    pieces = [evt for evt in events if forwarded_piece(evt)]
    if pieces:
        # The same one run_spec answers for.
        return resolve_trace_id(pieces[-1]), pieces[-1].event_id
    saved = session.forward if session is not None else None
    trigger = resolve_trigger_event(events)
    if saved is not None and saved.trace_id:
        timers = all(evt.source.startswith("timer:") for evt in events)
        if saved.return_to is not None or any(map(is_approval, events)) or (
            timers and explicit_trace_id(trigger) is None
        ):
            # Still the work it was: what it owes, or what a person decided on, stays in its chain.
            known = [evt for evt in events if explicit_trace_id(evt) == saved.trace_id]
            return saved.trace_id, known[-1].event_id if known else saved.parent_event_id
    link = session.task_link if session is not None else None
    if explicit_trace_id(trigger) is None and link is not None and link.trace_id:
        return link.trace_id, trigger.event_id
    return resolve_trace_id(trigger), trigger.event_id


def with_lineage(spec: ForwardSpec, events: Sequence[Event], session: SessionRef | None) -> ForwardSpec:
    trace_id, parent_event_id = run_lineage(events, session)
    return replace(spec, trace_id=trace_id, parent_event_id=parent_event_id)


def run_spec(events: Sequence[Event], session: SessionRef | None) -> ForwardSpec | None:
    # How the work of this Run reached it: what its events carry, on top of what the session already knew.
    spec = _chain_spec(events, session)
    return with_lineage(spec, events, session) if spec is not None else None


def _chain_spec(events: Sequence[Event], session: SessionRef | None) -> ForwardSpec | None:
    saved = session.forward if session is not None else None
    if saved is not None and saved.closed:
        # At work again.
        saved = replace(saved, closed=False)
    carried = [(evt, spec) for evt in events if (spec := carried_spec(evt)) is not None]
    returned_now = returned_pieces(events)
    waiting = tuple(p for p in (saved.pending_returns if saved is not None else ()) if p not in returned_now)
    settled = tuple(dict.fromkeys((
        *(saved.settled_returns if saved is not None else ()),
        *((piece, evt.event_id) for evt in events if (piece := returned_piece(evt))),
    )))
    if carried:
        hops = max(spec.hop_count for _, spec in carried)
        if saved is not None and (saved.return_to is not None or saved.pending_returns):
            # Still part of the chain this session is in; a chain that is over leaves nothing to the next one.
            hops = max(hops, saved.hop_count)
        forwards = [(evt, spec) for evt, spec in carried if evt.source == FORWARD_SOURCE]
        if forwards:
            source, latest = next((found for found in reversed(forwards) if found[1].return_to), forwards[-1])
            if saved is not None and forwarded_piece(source) is None:
                # The same work, so it is still done for the session it was done for.
                latest = replace(latest, parent_session_id=saved.parent_session_id)
            if latest.return_to is None and saved is not None and saved.return_to is not None:
                # Handed on inside the inbox: the session still owes what it owed.
                return replace(saved, hop_count=hops, pending_returns=waiting, settled_returns=settled,
                               work_key=latest.work_key or saved.work_key)
            return replace(
                latest, hop_count=hops, pending_returns=waiting, settled_returns=settled,
                cancelled_returns=saved.cancelled_returns if saved is not None else (),
                owed_failures=saved.owed_failures if saved is not None else (),
                work_key=latest.work_key or (saved.work_key if saved is not None else None),
            )
        returned = carried[-1][1]
        if saved is None:
            # The parent session is gone: this one answers outside and starts no new parent.
            return replace(returned, hop_count=hops)
        return replace(saved, hop_count=hops, pending_returns=waiting, settled_returns=settled)
    if saved is None:
        return None
    fresh = any(not is_approval(evt) and not evt.source.startswith("timer:") for evt in events)
    if fresh and saved.return_to is None and not (session and session.pending_request_id):
        # A new message from outside starts a new chain.
        return replace(saved, hop_count=0, forwarded_from=None, handed_on=False, question_parents=(), question_inbox=None)
    return saved


def carries_forwards(outcome: RunOutcome) -> bool:
    match outcome:
        case Silent() | Suspend():
            return True
        case Emit(status=status):
            return status == "done"
        case _ as unreachable:
            never_val: Never = unreachable
            assert_never(never_val)


def _reply_fields(spec: ForwardSpec | None, events: Sequence[Event]) -> dict[str, str]:
    # The events' own reply address first, then the one the chain started with.
    fields: dict[str, str] = {}
    for key, saved in (
        ("reply_channel", spec.reply_channel if spec is not None else None),
        ("reply_target", spec.reply_target if spec is not None else None),
    ):
        found = next((v for evt in reversed(events) if (v := _text(evt.payload.get(key)))), None) or saved
        if found:
            fields[key] = found
    return fields


def chain_external_id(kind: str, events: Sequence[Event], index: int) -> str:
    # The same batch run again after a crash repeats its ids and is deduplicated; an explicit replay is new work.
    first = events[0]
    replay = _text(first.payload.get("_replay_id"))
    return f"{kind}:{first.event_id}{f':{replay}' if replay else ''}:{index}"


def is_open(session: SessionRef) -> bool:
    return session.status in ("active", "suspended")


def current_session(sessions: SessionStorePort, session_id: str | None) -> SessionRef | None:
    # The session itself, or the one it went on as.
    session = sessions.get(session_id) if session_id else None
    seen: set[str] = set()
    while session is not None and session.status == "archived" and session.session_id not in seen:
        seen.add(session.session_id)
        following = session.forward.continued_in if session.forward is not None else None
        session = (sessions.get(following) if following else None) or session
    return session


def owner_session_id(session: SessionRef, sessions: SessionStorePort) -> str | None:
    # The session it works for now, which it closes with; parent_session_id on the session is the one it was made for.
    found = (session.forward.parent_session_id if session.forward is not None else None) or session.parent_session_id
    current = current_session(sessions, found)
    return current.session_id if current is not None else found


def _asking_session(sessions: SessionStorePort, spec: ForwardSpec) -> SessionRef | None:
    # The session up the parent chain that belongs to the inbox the result goes back to.
    current = spec.parent_session_id
    for _ in range(MAX_FORWARD_HOPS):
        session = current_session(sessions, current)
        if session is None or session.inbox_id == spec.return_to:
            return session
        current = session.parent_session_id
    return None


def return_session(sessions: SessionStorePort, spec: ForwardSpec) -> str | None:
    session = _asking_session(sessions, spec)
    return session.session_id if session is not None and is_open(session) else None


def _lineage_fields(spec: ForwardSpec) -> dict[str, str]:
    fields = {"trace_id": spec.trace_id, "parent_event_id": spec.parent_event_id}
    return {key: value for key, value in fields.items() if value}


def returned_chat_reply(events: Sequence[Event], outcome: Emit) -> str:
    if outcome.chat_reply or outcome.delivery or not outcome.messages:
        return outcome.chat_reply
    briefs: list[str] = []
    text = "\n\n".join(outcome.messages).strip()
    copied = False
    for event in events:
        if event.source != RETURN_SOURCE:
            continue
        payload = event.payload
        brief = _text(payload.get("chat_reply"))
        delivery = payload.get("delivery")
        if not brief and isinstance(delivery, dict):
            brief = delivery_chat_reply(cast(dict[str, object], delivery))
        if brief:
            briefs.append(brief)
            original = _text(payload.get("text"))
            copied |= original is not None and original in text
    if not briefs:
        return ""
    # A steward may write its own short answer. A quoted, excerpted or lengthy report uses the child's view.
    if copied or len(text) > 240 or any(label in text.casefold() for label in ("workdir:", "orca terminal:", "test evidence:")):
        return "\n".join(dict.fromkeys(briefs))
    links = dict.fromkeys(line for brief in briefs for line in brief.splitlines() if line.startswith("https://"))
    return "\n".join([text, *(link for link in links if link not in text)])


def return_payload(
    *,
    inbox_id: str,
    agent_id: str,
    session_id: str | None,
    spec: ForwardSpec,
    events: Sequence[Event],
    text: str,
    status: str,
    title: str = "",
    delivery: DeliveryOutcome | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "text": text,
        "status": status,
        # Carried back unchanged, so whatever the result leads to goes on from here.
        "hop_count": spec.hop_count,
        "forwarded_from": inbox_id,
        "forwarded_by": agent_id,
        **_lineage_fields(spec),
        **_reply_fields(spec, events),
    }
    for key, value in (("title", title), ("parent_session_id", spec.parent_session_id),
                       ("child_session_id", session_id), ("origin_key", spec.origin_key),
                       ("delegation_id", spec.delegation_id), ("task_key", spec.return_key),
                       ("from_task", spec.task_key)):
        if value:
            payload[key] = value
    if spec.replayed:
        payload["replayed"] = True
    if delivery is not None:
        payload["delivery"] = {"state": delivery.state, "pr_url": delivery.pr_url, "hold": delivery.hold,
                               "hold_detail": delivery.hold_detail}
    return payload


def _waiting_key(spec: ForwardSpec) -> str | None:
    return spec.return_key or spec.origin_key


def return_event(
    spec: ForwardSpec, *, external_id: str, payload: dict[str, Any], sessions: SessionStorePort
) -> Event:
    # Addressed when it is queued: the parent session may have closed while a person decided.
    assert spec.return_to is not None
    parent = return_session(sessions, spec)
    return Event(
        inbox_id=spec.return_to,
        source=RETURN_SOURCE,
        external_id=external_id,
        payload=payload,
        session_id=parent,
        # A named session keeps its own key; the conversation's key would move that key away from its holder.
        correlation_key=_waiting_key(spec) if parent is None else None,
    )


def cancel_event(
    spec: ForwardSpec, *, inbox_id: str, agent_id: str, external_id: str, reason: str, reply: Mapping[str, Any],
    sessions: SessionStorePort,
) -> Event | None:
    # Tells the session that waits for this piece that no result will come: no hop, no agent runs for it.
    waiting = _asking_session(sessions, spec)
    if spec.return_to is None or waiting is None:
        return None
    if not is_open(waiting) and (waiting.status != "archived" or _waiting_key(spec) is None):
        return None
    return Event(
        inbox_id=spec.return_to,
        source=RETURN_CANCELLED_SOURCE,
        external_id=external_id,
        payload={
            "reason": reason,
            "delegation_id": spec.delegation_id,
            "forwarded_from": inbox_id,
            "forwarded_by": agent_id,
            **_lineage_fields(spec),
            **_reply_fields(spec, ()),
            **{key: reply[key] for key in ("reply_channel", "reply_target") if reply.get(key)},
        },
        session_id=waiting.session_id if is_open(waiting) else None,
        # An archived session handed its conversation on to another one.
        correlation_key=None if is_open(waiting) else _waiting_key(spec),
    )


def closing_event(session: SessionRef, sessions: SessionStorePort) -> Event | None:
    # For an open session about to close that owes a result, or keeps one a person has yet to decide on.
    spec = session.forward
    if spec is None or spec.return_to is None or not is_open(session):
        return None
    held = spec.held
    return cancel_event(
        spec, inbox_id=session.inbox_id, agent_id=session.agent_id or "",
        external_id=held.external_id if held is not None else f"return:{session.session_id}:closed",
        reason="its session was closed", reply=held.payload if held is not None else {}, sessions=sessions,
    )


def out_of_hops(spec: ForwardSpec) -> bool:
    # Past the limit, not at it: the last forward still gets its result back, and none can go further than that.
    return spec.hop_count > MAX_FORWARD_HOPS


def _sent_from(routes: Callable[[str], InboxRoute | None], inbox_id: str, asker: str) -> bool:
    # The asking inbox may forward to the project of this one now: the one case a result leaves its project.
    here, there = routes(inbox_id), routes(asker)
    return here is not None and there is not None and any(
        (main := routes(item)) is not None and main.project == here.project for item in there.forward_to
    )


def return_refusal(
    spec: ForwardSpec,
    *,
    inbox_id: str,
    agent_id: str,
    payload: Mapping[str, Any],
    permissions: PermissionPort,
    routes: Callable[[str], InboxRoute | None] | None,
) -> ForwardRefused | None:
    # Why a result may not go back to spec.return_to now: "block" whatever anyone approves, "defer" until a person does.
    assert spec.return_to is not None
    if routes is not None:
        here, there = routes(inbox_id), routes(spec.return_to)
        if here is None or there is None or (
            here.project != there.project and not _sent_from(routes, inbox_id, spec.return_to)
        ):
            return ForwardRefused(f"return_to '{spec.return_to}' is not an inbox of this project", "block")
    if out_of_hops(spec):
        return ForwardRefused(
            f"this work was forwarded {spec.hop_count} hop(s) (limit {MAX_FORWARD_HOPS})", "block"
        )
    decision = permissions.check(FORWARD_TOOL, {
        "inbox_id": inbox_id,
        "agent_id": agent_id,
        "target_inbox": spec.return_to,
        "target_agent": None,
        "return_to": None,
        "hop_count": spec.hop_count,
        "text": payload["text"],
        "status": payload["status"],
    })
    if decision.action == "block":
        return ForwardRefused(decision.reason, "block")
    if decision.action == "defer":
        return ForwardRefused(decision.reason, "defer")
    return None


@dataclass(slots=True)
class ForwardStage:
    # The forwards of one Run. They are only staged here: the worker queues them once the Run has a result.
    inbox_id: str
    agent_id: str
    session: SessionRef
    correlation_key: str | None
    work_key: str | None
    permissions: PermissionPort
    sessions: SessionStorePort
    registered: Collection[str]
    # (inbox_id, tags) -> the agent that inbox's rules and defaults pick; raises AgentNotAllowed.
    default_agent: Callable[[str, Sequence[str]], str]
    # None: this runtime has no projects, so only the permission gate limits the targets.
    routes: Callable[[str], InboxRoute | None] | None
    # Everything the Run has consumed so far, folded events included.
    events: Callable[[], Sequence[Event]]
    approval: Callable[[str], bool]
    # Events of an inbox that are pending or in progress.
    queued: Callable[[str], Sequence[Event]] = lambda _inbox_id: ()
    origin: Callable[[Event], EventOrigin | None] = lambda _event: None
    staged: list[Event] = field(default_factory=list[Event])

    def reset(self) -> None:
        self.staged.clear()

    def forward(
        self,
        target_inbox: str,
        text: str,
        *,
        title: str = "",
        tags: Sequence[str] = (),
        target_agent: str | None = None,
        return_to: str | None = None,
        continues: str | None = None,
        approved_request_id: str | None = None,
    ) -> Event:
        target = target_inbox.strip()
        body = text.strip()
        if not target:
            raise ForwardRefused("target_inbox must not be empty")
        if not body:
            raise ForwardRefused("text must not be empty")
        events = self.events()
        spec = run_spec(events, self.session)
        hops = spec.hop_count if spec is not None else 0
        if hops >= MAX_FORWARD_HOPS:
            raise ForwardRefused(
                f"this work was already forwarded {hops} hop(s) (limit {MAX_FORWARD_HOPS}); answer it here instead"
            )
        # Only from a scope outside every project, and only to the inboxes its binding lists.
        crossing = self._crosses(target)
        binding_route = self._check_inbox("target_inbox", target, crossing)
        if binding_route is not None and binding_route.system and not self.from_person():
            raise ForwardRefused(f"'{target}' takes only what a person asks for in this turn, in their own chat or on "
                                 "this host, or the next step after a system inbox finished their request; ask them")
        relayed = target == self.inbox_id
        agent = _text(target_agent)
        if crossing and agent is not None:
            raise ForwardRefused(f"target_agent cannot be named for '{target}': that project's rules pick the agent")
        if agent is None and relayed:
            # Left open, the event would stick to this agent's own session.
            agent = self.default_agent(target, tags)
        if agent is not None:
            if agent not in self.registered:
                raise ForwardRefused(f"target_agent '{agent}' is not registered")
            binding = binding_route.binding if binding_route is not None else None
            if binding is not None and not binding.allows(agent):
                raise ForwardRefused(
                    f"target_agent '{agent}' is not allowed in '{target}' (allowed: {', '.join(binding.agents)})"
                )
        if relayed and agent == self.agent_id:
            raise ForwardRefused(f"agent '{self.agent_id}' cannot forward to itself in '{target}'")
        back = _text(return_to)
        owed = spec.return_to if spec is not None else None
        added = _text(continues)
        if added is not None:
            self._check_added(target, added, crossing, back, owed, self._root(spec))
        base = (spec.task_key if spec is not None else None) or self._root(spec)
        joins = spec is not None and not crossing and back in (None, self.inbox_id, owed) and self._joins(
            target, agent, base, spec
        )
        if joins:
            # What the receiver owes for the task answers this too.
            back = owed = None
        # Anything but a part this session gets back itself goes on whole, with what is owed for it: an agent that
        # answered outside or another inbox would answer the same work a second time.
        whole = back is None or back != self.inbox_id
        passing = owed is not None and whole and back in (None, owed)
        if crossing and (back not in (None, self.inbox_id) or (passing and owed != self.inbox_id)):
            raise ForwardRefused(f"return_to must be '{self.inbox_id}' for work sent to another project")
        if owed is not None and whole and not passing:
            raise ForwardRefused(f"return_to must be '{self.inbox_id}', or '{owed}' to hand this work on")
        if (passing and any(map(passed_on, self.staged))) or (spec is not None and spec.handed_on and whole):
            raise ForwardRefused(
                "this work is already handed on to another agent, which answers for it; end your turn without a result"
            )
        if passing:
            assert spec is not None
            back = owed
            asker = _asking_session(self.sessions, spec)
            if target == back and (not relayed or (asker is not None and asker.agent_id == agent)):
                raise ForwardRefused(f"'{target}' waits for your result; end your turn with it")
        if back is not None:
            if back == target and not relayed:
                raise ForwardRefused(f"return_to '{back}' is the target inbox itself")
            self._check_inbox("return_to", back, passing and self._owed_across(back))
        wanted = tuple(dict.fromkeys(t for tag in tags if (t := tag.strip())))
        decision = self.permissions.check(FORWARD_TOOL, {
            "inbox_id": self.inbox_id,
            "agent_id": self.agent_id,
            "target_inbox": target,
            "target_agent": agent,
            "return_to": back,
            "hop_count": hops + 1,
            "title": title,
            "text": body,
            "tags": list(wanted),
            **({"continues": added} if added is not None else {}),
        })
        if decision.action == "block":
            raise ForwardRefused(decision.reason, "block")
        if decision.action == "defer" and not (approved_request_id and self.approval(approved_request_id)):
            raise ForwardRefused(decision.reason, "defer")
        event = self._event(target, agent, body, title.strip(), wanted, back, hops, spec, events, passing, crossing,
                            added, joins)
        self.staged.append(event)
        # A copy: the caller may do what it likes with its receipt, what was checked above is what goes out.
        return deepcopy(event)

    def drop_refused(self) -> list[tuple[Event, str]]:
        # What the configuration in force no longer lets out of this inbox, judged again before anything is queued.
        dropped: list[tuple[Event, str]] = []
        for evt in list(self.staged):
            if evt.inbox_id == self.inbox_id:
                continue
            try:
                self._check_inbox("target_inbox", evt.inbox_id, self._crosses(evt.inbox_id))
            except ForwardRefused as exc:
                self.staged.remove(evt)
                dropped.append((evt, str(exc)))
        return dropped

    def from_person(self) -> bool:
        events = self.events()
        saved = self.session.forward
        return any(self.origin(evt) is not None for evt in events) or (saved is not None and (
            (saved.from_person and any(map(is_approval, events)))
            or any(self._carries_request(evt, saved) for evt in events)
        ))

    def _carries_request(self, event: Event, saved: ForwardSpec) -> bool:
        # A finished result of work this session still waits for, sent back by an inbox that takes nothing but a
        # person's own request: the next step of that request goes on from it. A project's result does not, nor does a
        # failed one, so a chain stops at the step that failed.
        piece = returned_piece(event)
        if piece is None or piece not in saved.pending_returns or self.routes is None or is_replay(event):
            return False
        if event.payload.get("status") != "done":
            return False
        sender = _text(event.payload.get("forwarded_from"))
        route = self.routes(sender) if sender is not None else None
        return route is not None and route.system

    def _crosses(self, target: str) -> bool:
        if self.routes is None:
            return False
        here, there = self.routes(self.inbox_id), self.routes(target)
        return here is not None and there is not None and here.project != there.project and target in here.forward_to

    def _owed_across(self, asker: str) -> bool:
        return self.routes is not None and _sent_from(self.routes, self.inbox_id, asker)

    def _root(self, spec: ForwardSpec | None) -> str | None:
        return (spec.origin_key if spec is not None else None) or self.correlation_key

    def _check_added(
        self, target: str, key: str, crossing: bool, back: str | None, owed: str | None, root: str | None
    ) -> None:
        if not crossing:
            raise ForwardRefused("continues names a task sent to another project")
        if back is not None or owed is not None:
            raise ForwardRefused("continues takes no return_to: the task it adds to is answered as it was sent")
        session = self.sessions.find_by_correlation_key(target, key)
        known = session.forward if session is not None else None
        origins = [known.origin_key] if known is not None and known.task_key == key else [
            _text(evt.payload.get("origin_key")) for evt in (*self.queued(target), *self.staged)
            if evt.inbox_id == target and evt.source == FORWARD_SOURCE and evt.payload.get("task_key") == key
        ]
        if not key.startswith(f"forward:{self.inbox_id}:forward:") or root not in origins:
            raise ForwardRefused(
                f"'{key}' is not an open task of this conversation in '{target}'; send it as new work, with everything "
                "the receiver needs"
            )

    def _joins(self, target: str, agent: str | None, base: str | None, spec: ForwardSpec) -> bool:
        # What was added to a task follows it to the session that owes its result, handed on or waited for here. A
        # session that owes nothing takes it like any other follow-up.
        if spec.task_key is None or base is None or not any(map(is_supplement, self.events())):
            return False
        holder = self.sessions.find_by_correlation_key(target, self._key(target, agent, base) or base)
        known = holder.forward if holder is not None else None
        if known is None or known.task_key != spec.task_key or known.return_to is None:
            return False
        return known.delegation_id in spec.pending_returns or spec.return_to is None

    def _key(self, target: str, agent: str | None, base: str | None) -> str | None:
        if target != self.inbox_id or base is None:
            return base
        # A prefix, so no channel reads the task id out of the child's key.
        holder = self.sessions.find_by_correlation_key(target, base)
        return base if holder is not None and holder.agent_id == agent else f"forward:{agent}:{base}"

    def _check_inbox(self, name: str, inbox_id: str, other_project: bool = False) -> InboxRoute | None:
        if self.routes is None:
            return None
        here, there = self.routes(self.inbox_id), self.routes(inbox_id)
        if here is not None and here.system:
            raise ForwardRefused(f"'{self.inbox_id}' passes no work on; answer it here")
        if here is None or there is None or (there.project != here.project and not other_project):
            listed = f", nor one of {', '.join(here.forward_to)}" if here is not None and here.forward_to else ""
            raise ForwardRefused(f"{name} '{inbox_id}' is not an inbox of this project{listed}")
        if there.has_bindings and there.binding is None:
            raise ForwardRefused(f"{name} '{inbox_id}' is covered by no inbox binding of project '{there.project}'")
        return there

    def _event(
        self, target: str, agent: str | None, text: str, title: str, tags: Sequence[str], return_to: str | None,
        hops: int, spec: ForwardSpec | None, events: Sequence[Event], passing: bool, crossing: bool,
        added: str | None, joins: bool,
    ) -> Event:
        root = self._root(spec)
        trace_id, parent_event_id = run_lineage(events, self.session)
        payload: dict[str, Any] = {
            "text": text,
            "hop_count": hops + 1,
            "forwarded_from": self.inbox_id,
            "forwarded_by": self.agent_id,
            "question_parents": [self.session.session_id, *(spec.question_parents if spec else ())],
            "trace_id": trace_id,
            "parent_event_id": parent_event_id,
            **_reply_fields(spec, events),
        }
        for key in _INHERITED_KEYS:
            value = next((evt.payload[key] for evt in reversed(events) if evt.payload.get(key)), None)
            if value is not None:
                payload[key] = value
        external_id = chain_external_id("forward", events, len(self.staged))
        piece = external_id if return_to else None
        parent: str | None = self.session.session_id
        if passing and spec is not None:
            # The receiver owes it to the session that waits for it, as if that one had sent it the work: this session
            # is out of it, so closing it ends nothing there.
            piece, parent = spec.delegation_id, spec.parent_session_id
        replayed = (passing and spec is not None and spec.replayed) or any(map(is_replay, events))
        for key, value in (("title", title), ("tags", list(tags)), ("agent_id", agent), ("return_to", return_to),
                           ("origin_key", root), ("delegation_id", piece), ("parent_session_id", parent),
                           ("replayed", replayed), ("supplement", joins or added is not None)):
            if value:
                payload[key] = value
        if crossing:
            # A task of its own: whatever else this conversation sends there has another key.
            task = added or ":".join(filter(None, ("forward", self.inbox_id, external_id, root)))
            waiting = spec.task_key if spec is not None else None
            key: str | None = task
        else:
            task = spec.task_key if spec is not None else None
            waiting = task
            key = self._key(target, agent, task or root)
        if passing and spec is not None:
            waiting = spec.return_key
        if target == self.inbox_id and self.work_key:
            # The relay works in the same worktree; review_of is how a reviewer names the developer's one.
            payload["work_key"] = payload["review_of"] = self.work_key
        for name, value in (("task_key", task), ("return_key", waiting if return_to else None)):
            if value:
                payload[name] = value
        return Event(
            inbox_id=target,
            source=FORWARD_SOURCE,
            external_id=external_id,
            # Values taken over from the events must not stay shared with them.
            payload=deepcopy(payload),
            correlation_key=key if joins or added is not None else self._distinct(target, key),
        )

    def _distinct(self, target: str, key: str | None) -> str | None:
        # Two pieces of one Run sent to the same inbox are separate tasks, each with its own session and worktree.
        if key is None:
            return None
        taken = {evt.correlation_key for evt in self.staged if evt.inbox_id == target}
        found, n = key, 2
        while found in taken:
            found, n = f"forward:{n}:{key}", n + 1
        return found

    def own_returns(self) -> tuple[str, ...]:
        return tuple(piece for evt in self.staged
                     if evt.payload.get("return_to") == self.inbox_id and not passed_on(evt)
                     and (piece := forwarded_piece(evt)))

    def passes_on(self, spec: ForwardSpec | None) -> bool:
        return spec is not None and spec.delegation_id is not None and any(
            passed_on(evt) and forwarded_piece(evt) == spec.delegation_id for evt in self.staged
        )

    def hand_over(self, link: TaskLink) -> bool:
        # The task goes with the first piece this session gets no result for: its agent finishes the work.
        for index, evt in enumerate(self.staged):
            if is_supplement(evt) or ("return_to" in evt.payload and not passed_on(evt)):
                continue
            fields: dict[str, Any] = {"task_id": link.task_id, "task_trace_id": link.trace_id}
            if link.hop_count is not None:
                fields["task_hop_count"] = link.hop_count
            self.staged[index] = replace(evt, payload={**evt.payload, **fields})
            return True
        return False
