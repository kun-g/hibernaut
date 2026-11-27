from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import cast

from switchboard.envelope import CanonicalEvent

from agent_runtime.adapters.channels.switchboard_bridge import SWITCHBOARD_SOURCE_PREFIX
from agent_runtime.core.models import Event
from agent_runtime.engine.forwarding import EventOrigin, is_approval
from agent_runtime.engine.handler import REMOTE_KEY

# Put here by a channel, a timer or Hibernaut itself; everything else came in through this host's own entrance.
_OTHER_PREFIXES = (SWITCHBOARD_SOURCE_PREFIX, "gdrive:", "timer:", "agent:")
_FEISHU = "feishu"


@dataclass(frozen=True, slots=True)
class FeishuGroup:
    # A Feishu group whose listed people talk to one inbox directly: a system inbox, or an inbox of one project.
    chat_id: str
    inbox_id: str
    members: frozenset[str]
    # Set for a project group: whatever its people say is a request about this project only, and the approvals of the
    # project's inboxes count from approvers alone, outside the group. A system group's members decide in the group.
    project: str | None = None
    approvers: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class CardRoute:
    # Where the approval cards of an inbox go (None: Switchboard's own approval chat) and who alone decides them.
    chat_id: str | None
    deciders: frozenset[str]


def _metadata(raw: object) -> Mapping[str, object]:
    return cast(dict[str, object], raw) if isinstance(raw, dict) else {}


def _no_project(_inbox_id: str) -> str | None:
    return None


class GroupEntrances:
    # Chat ids and open ids are what Feishu itself put on the message: only Switchboard's verified Feishu webhook makes
    # source "feishu", and an event another host sends in is marked remote whatever it claims.
    def __init__(
        self, groups: Iterable[FeishuGroup] = (), project_of: Callable[[str], str | None] = _no_project,
    ) -> None:
        self._by_chat = {group.chat_id: group for group in groups}
        # The configuration binds at most one group to an inbox.
        self._by_inbox = {group.inbox_id: group for group in self._by_chat.values()}
        self._project_of = project_of
        approvers: dict[str, set[str]] = {}
        for group in self._by_chat.values():
            if group.project is not None:
                approvers.setdefault(group.project, set()).update(group.approvers)
        self._approvers = {project: frozenset(found) for project, found in approvers.items()}

    @property
    def inboxes(self) -> Collection[str]:
        return self._by_inbox.keys()

    def group_of(self, inbox_id: str) -> FeishuGroup | None:
        return self._by_inbox.get(inbox_id)

    def _project_approvers(self, inbox_id: str) -> frozenset[str] | None:
        # The people whose decision counts on any inbox of a project a group is bound to; None for every other inbox.
        if not self._approvers:
            return None
        bound = self._by_inbox.get(inbox_id)
        project = bound.project if bound is not None else self._project_of(inbox_id)
        return self._approvers.get(project) if project is not None else None

    def card_group(self, inbox_id: str) -> CardRoute | None:
        # None where no group is bound to the inbox or its project.
        group = self._by_inbox.get(inbox_id)
        if group is not None and group.project is None:
            return CardRoute(group.chat_id, group.members)
        approvers = self._project_approvers(inbox_id)
        # A project's cards stay out of its group, which has people in it who may not decide.
        return CardRoute(None, approvers) if approvers is not None else None

    def _group(self, metadata: Mapping[str, object]) -> FeishuGroup | None:
        if metadata.get("chat_type") == "p2p":
            return None
        chat_id = metadata.get("chat_id")
        return self._by_chat.get(chat_id) if isinstance(chat_id, str) else None

    def inbox_for(self, event: CanonicalEvent) -> str | None:
        # Where a Switchboard event goes; None: nowhere, and nobody is answered.
        if event.source != _FEISHU:
            return event.scope
        actor = event.actor or ""
        if event.event_type == "approval.decided":
            bound = self._by_inbox.get(event.scope)
            if bound is not None and bound.project is None:
                # Only a click in the inbox's own group, by one of its people: not the same person in another chat.
                clicked = event.metadata.get("chat_id")
                return event.scope if clicked == bound.chat_id and actor in bound.members else None
            approvers = self._project_approvers(event.scope)
            if approvers is not None:
                return event.scope if actor in approvers else None
            return event.scope
        group = self._group(event.metadata)
        if group is None:
            # The inbox of a project group takes no other chat's messages, wherever Switchboard routes them.
            taken = self._by_inbox.get(event.scope)
            return None if taken is not None and taken.project is not None else event.scope
        return group.inbox_id if actor in group.members else None

    def admits(self, event: Event) -> FeishuGroup | None:
        # The group bound to this event's inbox, when the event is a message of one of its people there.
        group = self._group(_metadata(event.payload.get("metadata")))
        if group is not None and group.inbox_id == event.inbox_id and event.payload.get("actor") in group.members:
            return group
        return None


def event_origin_for(groups: GroupEntrances) -> Callable[[Event], EventOrigin | None]:
    def origin(event: Event) -> EventOrigin | None:
        if is_approval(event) or event.payload.get(REMOTE_KEY):
            return None
        source = event.source.strip().lower()
        if source == f"{SWITCHBOARD_SOURCE_PREFIX}{_FEISHU}":
            if _metadata(event.payload.get("metadata")).get("chat_type") == "p2p":
                return "chat"
            group = groups.admits(event)
            # A project group speaks for its project only, never as a person's own request to a system inbox.
            return "group" if group is not None and group.project is None else None
        return None if source.startswith(_OTHER_PREFIXES) else "operator"

    return origin


event_origin = event_origin_for(GroupEntrances())
