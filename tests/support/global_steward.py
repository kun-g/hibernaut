from __future__ import annotations

from typing import Any


ASH = "user:main"


def forward(target: str, text: str, **arguments: Any) -> dict[str, Any]:
    return {"tool": "forward_to_inbox", "arguments": {"target_inbox": target, "text": text, **arguments}}


def reply(message: str) -> dict[str, Any]:
    return {"output": {"action": "reply", "message": message}}
