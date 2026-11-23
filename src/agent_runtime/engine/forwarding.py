from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, Never, assert_never, cast

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
)
from agent_runtime.core.ports import PermissionPort, SessionStorePort

_AGENT_SOURCES = (FORWARD_SOURCE, RETURN_SOURCE)
# Which project documents the task needs travels with it; everything else the receiver learns from the text.
_INHERITED_KEYS = ("activities", "base_ref")


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
        reply_channel=_text(payload.get("reply_channel")),
        reply_target=_text(payload.get("reply_target")),
        origin_key=_text(payload.get("origin_key")),
        delegation_id=forwarded_piece(event) or None,
        work_key=_text(payload.get("work_key")) if payload.get("forwarded_from") == event.inbox_id else None,
    )


def forwarded_piece(event: Event) -> str | None:
    # The piece of work a forward asks a result for, "" for one that asks for none, None for any other event. Work
    # handed on inside an inbox without asking for a result is the same work, so it is None too.
    if event.source != FORWARD_SOURCE:
        return None
    if _text(event.payload.get("return_to")) is None:
        return None if event.payload.get("forwarded_from") == event.inbox_id else ""
    return _text(event.payload.get("delegation_id")) or event.external_id


def returned_pieces(events: Sequence[Event]) -> set[str]:
    return {piece for evt in events
            if evt.source == RETURN_SOURCE and (piece := _text(evt.payload.get("delegation_id")))}


def same_piece(event: Event, batch: Sequence[Event]) -> bool:
    # A Run answers one piece: two in one batch would leave all but one of them without a result of their own.
    piece = forwarded_piece(event)
    others = {found for evt in batch if (found := forwarded_piece(evt)) is not None}
    return piece is None or not others or piece in others


def own_task(event: Event, sessions: SessionStorePort) -> Event:
    # A piece sent to a conversation whose session still owes another one gets a session and worktree of its own.
    piece = forwarded_piece(event)
    if piece is None or not event.correlation_key:
        return event
    if event.session_id:
        # Claimed before: bound to the session it ran in, whose key it keeps.
        holder = sessions.get(event.session_id)
    else:
        holder = sessions.find_by_correlation_key(event.inbox_id, event.correlation_key)
    owed = holder.forward if holder is not None else None
    if owed is not None and owed.return_to is not None and owed.delegation_id != piece:
        return replace(event, session_id=None, correlation_key=f"forward:{event.external_id}:{event.correlation_key}")
    if holder is not None and holder.correlation_key:
        return replace(event, correlation_key=holder.correlation_key)
    return event


def run_spec(events: Sequence[Event], session: SessionRef | None) -> ForwardSpec | None:
    # How the work of this Run reached it: what its events carry, on top of what the session already knew.
    saved = session.forward if session is not None else None
    carried = [(evt, spec) for evt in events if (spec := carried_spec(evt)) is not None]
    returned_now = returned_pieces(events)
    waiting = tuple(p for p in (saved.pending_returns if saved is not None else ()) if p not in returned_now)
    if carried:
        hops = max(spec.hop_count for _, spec in carried)
        if saved is not None:
            hops = max(hops, saved.hop_count)
        forwards = [spec for evt, spec in carried if evt.source == FORWARD_SOURCE]
        if forwards:
            latest = next((spec for spec in reversed(forwards) if spec.return_to), forwards[-1])
            if latest.return_to is None and saved is not None and saved.return_to is not None:
                # Handed on inside the inbox: the session still owes what it owed.
                return replace(saved, hop_count=hops, pending_returns=waiting, work_key=latest.work_key or saved.work_key)
            return replace(
                latest, hop_count=hops, pending_returns=waiting,
                cancelled_returns=saved.cancelled_returns if saved is not None else (),
                owed_failures=saved.owed_failures if saved is not None else (),
                work_key=latest.work_key or (saved.work_key if saved is not None else None),
            )
        returned = carried[-1][1]
        if saved is None:
            # The parent session is gone: this one answers outside and starts no new parent.
            return replace(returned, hop_count=hops)
        return replace(saved, hop_count=hops, pending_returns=waiting)
    if saved is None:
        return None
    fresh = any(not is_approval(evt) and not evt.source.startswith("timer:") for evt in events)
    if fresh and saved.return_to is None:
        # A new message from outside starts a new chain.
        return replace(saved, hop_count=0, forwarded_from=None)
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


def _asking_session(sessions: SessionStorePort, spec: ForwardSpec) -> SessionRef | None:
    # The session up the parent chain that belongs to the inbox the result goes back to.
    current = spec.parent_session_id
    for _ in range(MAX_FORWARD_HOPS):
        session = sessions.get(current) if current else None
        if session is None or session.inbox_id == spec.return_to:
            return session
        current = session.parent_session_id
    return None


def return_session(sessions: SessionStorePort, spec: ForwardSpec) -> str | None:
    session = _asking_session(sessions, spec)
    return session.session_id if session is not None and is_open(session) else None


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
        "hop_count": spec.hop_count + 1,
        "forwarded_from": inbox_id,
        "forwarded_by": agent_id,
        **_reply_fields(spec, events),
    }
    for key, value in (("title", title), ("parent_session_id", spec.parent_session_id),
                       ("child_session_id", session_id), ("origin_key", spec.origin_key),
                       ("delegation_id", spec.delegation_id)):
        if value:
            payload[key] = value
    if delivery is not None:
        payload["delivery"] = {"state": delivery.state, "pr_url": delivery.pr_url, "hold": delivery.hold,
                               "hold_detail": delivery.hold_detail}
    return payload


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
        correlation_key=spec.origin_key if parent is None else None,
    )


def cancel_event(
    spec: ForwardSpec, *, inbox_id: str, agent_id: str, external_id: str, reason: str, reply: Mapping[str, Any],
    sessions: SessionStorePort,
) -> Event | None:
    # Tells the session that waits for this piece that no result will come: no hop, no agent runs for it.
    waiting = _asking_session(sessions, spec)
    if spec.return_to is None or waiting is None:
        return None
    if not is_open(waiting) and (waiting.status != "archived" or spec.origin_key is None):
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
            **_reply_fields(spec, ()),
            **{key: reply[key] for key in ("reply_channel", "reply_target") if reply.get(key)},
        },
        session_id=waiting.session_id if is_open(waiting) else None,
        # An archived session handed its conversation on to another one.
        correlation_key=None if is_open(waiting) else spec.origin_key,
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
    return spec.hop_count >= MAX_FORWARD_HOPS


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
        if here is None or there is None or here.project != there.project:
            return ForwardRefused(f"return_to '{spec.return_to}' is not an inbox of this project", "block")
    if out_of_hops(spec):
        return ForwardRefused(
            f"this work already made {spec.hop_count} hop(s) between agents (limit {MAX_FORWARD_HOPS})", "block"
        )
    decision = permissions.check(FORWARD_TOOL, {
        "inbox_id": inbox_id,
        "agent_id": agent_id,
        "target_inbox": spec.return_to,
        "target_agent": None,
        "return_to": None,
        "hop_count": spec.hop_count + 1,
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
                f"this work already made {hops} hop(s) between agents (limit {MAX_FORWARD_HOPS}); answer it here instead"
            )
        binding_route = self._check_inbox("target_inbox", target)
        relayed = target == self.inbox_id
        agent = _text(target_agent)
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
        if back is not None:
            if back == target and not relayed:
                raise ForwardRefused(f"return_to '{back}' is the target inbox itself")
            self._check_inbox("return_to", back)
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
        })
        if decision.action == "block":
            raise ForwardRefused(decision.reason, "block")
        if decision.action == "defer" and not (approved_request_id and self.approval(approved_request_id)):
            raise ForwardRefused(decision.reason, "defer")
        event = self._event(target, agent, body, title.strip(), wanted, back, hops, spec, events)
        self.staged.append(event)
        # A copy: the caller may do what it likes with its receipt, what was checked above is what goes out.
        return deepcopy(event)

    def _check_inbox(self, name: str, inbox_id: str) -> InboxRoute | None:
        if self.routes is None:
            return None
        here, there = self.routes(self.inbox_id), self.routes(inbox_id)
        if here is None or there is None or there.project != here.project:
            raise ForwardRefused(f"{name} '{inbox_id}' is not an inbox of this project")
        if there.has_bindings and there.binding is None:
            raise ForwardRefused(f"{name} '{inbox_id}' is covered by no inbox binding of project '{there.project}'")
        return there

    def _event(
        self, target: str, agent: str | None, text: str, title: str, tags: Sequence[str], return_to: str | None,
        hops: int, spec: ForwardSpec | None, events: Sequence[Event],
    ) -> Event:
        root = (spec.origin_key if spec is not None else None) or self.correlation_key
        payload: dict[str, Any] = {
            "text": text,
            "hop_count": hops + 1,
            "forwarded_from": self.inbox_id,
            "forwarded_by": self.agent_id,
            "parent_session_id": self.session.session_id,
            **_reply_fields(spec, events),
        }
        for key in _INHERITED_KEYS:
            value = next((evt.payload[key] for evt in reversed(events) if evt.payload.get(key)), None)
            if value is not None:
                payload[key] = value
        external_id = chain_external_id("forward", events, len(self.staged))
        piece = external_id if return_to else None
        if spec is not None and return_to is not None and return_to == spec.return_to != self.inbox_id:
            # What this session owes itself is passed on, so whoever answers first settles that one piece.
            piece = spec.delegation_id
        for key, value in (("title", title), ("tags", list(tags)), ("agent_id", agent), ("return_to", return_to),
                           ("origin_key", root), ("delegation_id", piece)):
            if value:
                payload[key] = value
        key = root
        if target == self.inbox_id:
            if self.work_key:
                # The relay works in the same worktree; review_of is how a reviewer names the developer's one.
                payload["work_key"] = payload["review_of"] = self.work_key
            if root is not None:
                # A prefix, so no channel reads the task id out of the child's key.
                holder = self.sessions.find_by_correlation_key(target, root)
                key = root if holder is not None and holder.agent_id == agent else f"forward:{agent}:{root}"
        return Event(
            inbox_id=target,
            source=FORWARD_SOURCE,
            external_id=external_id,
            # Values taken over from the events must not stay shared with them.
            payload=deepcopy(payload),
            correlation_key=self._distinct(target, key),
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
                     if evt.payload.get("return_to") == self.inbox_id and (piece := forwarded_piece(evt)))

    def hand_over(self, link: TaskLink) -> bool:
        # The task goes with the first piece whose agent answers outside by itself.
        for index, evt in enumerate(self.staged):
            if "return_to" in evt.payload:
                continue
            fields: dict[str, Any] = {"task_id": link.task_id}
            if link.trace_id:
                fields["trace_id"] = link.trace_id
            if link.hop_count is not None:
                fields["task_hop_count"] = link.hop_count
            self.staged[index] = replace(evt, payload={**evt.payload, **fields})
            return True
        return False
