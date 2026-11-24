from __future__ import annotations

from typing import cast

from agent_runtime.adapters.channels.switchboard_bridge import SWITCHBOARD_SOURCE_PREFIX
from agent_runtime.core.models import Event
from agent_runtime.engine.forwarding import EventOrigin, is_approval
from agent_runtime.engine.handler import REMOTE_KEY

# Put here by a channel, a timer or Hibernaut itself; everything else came in through this host's own entrance.
_OTHER_PREFIXES = (SWITCHBOARD_SOURCE_PREFIX, "gdrive:", "timer:", "agent:")


def event_origin(event: Event) -> EventOrigin | None:
    if is_approval(event) or event.payload.get(REMOTE_KEY):
        return None
    source = event.source.strip().lower()
    if source == f"{SWITCHBOARD_SOURCE_PREFIX}feishu":
        metadata: object = event.payload.get("metadata")
        direct = isinstance(metadata, dict) and cast(dict[str, object], metadata).get("chat_type") == "p2p"
        return "chat" if direct else None
    return None if source.startswith(_OTHER_PREFIXES) else "operator"
