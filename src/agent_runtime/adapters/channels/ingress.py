from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
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
    # A Feishu group whose listed people talk to one system inbox directly.
    chat_id: str
    inbox_id: str
    members: frozenset[str]


def _metadata(raw: object) -> Mapping[str, object]:
    return cast(dict[str, object], raw) if isinstance(raw, dict) else {}


class GroupEntrances:
    # Chat ids and open ids are what Feishu itself put on the message: only Switchboard's verified Feishu webhook makes
    # source "feishu", and an event another host sends in is marked remote whatever it claims.
    def __init__(self, groups: Iterable[FeishuGroup] = ()) -> None:
        self._by_chat = {group.chat_id: group for group in groups}
        self._members_of: dict[str, frozenset[str]] = {}
        for group in self._by_chat.values():
            self._members_of[group.inbox_id] = self._members_of.get(group.inbox_id, frozenset()) | group.members

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
            members = self._members_of.get(event.scope)
            return event.scope if members is None or actor in members else None
        group = self._group(event.metadata)
        if group is None:
            return event.scope
        return group.inbox_id if actor in group.members else None

    def admits(self, event: Event) -> bool:
        group = self._group(_metadata(event.payload.get("metadata")))
        return group is not None and group.inbox_id == event.inbox_id and event.payload.get("actor") in group.members


def event_origin_for(groups: GroupEntrances) -> Callable[[Event], EventOrigin | None]:
    def origin(event: Event) -> EventOrigin | None:
        if is_approval(event) or event.payload.get(REMOTE_KEY):
            return None
        source = event.source.strip().lower()
        if source == f"{SWITCHBOARD_SOURCE_PREFIX}{_FEISHU}":
            if _metadata(event.payload.get("metadata")).get("chat_type") == "p2p":
                return "chat"
            return "group" if groups.admits(event) else None
        return None if source.startswith(_OTHER_PREFIXES) else "operator"

    return origin


event_origin = event_origin_for(GroupEntrances())
