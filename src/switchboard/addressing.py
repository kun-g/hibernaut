from __future__ import annotations

import re
from typing import Any

_GITHUB_KEY = re.compile(r"^github:(?P<repo>[^#\s]+/[^#\s]+)#(?P<number>\d+)$")


def github_correlation_key(repo: str, number: int | str) -> str:
    return f"github:{repo}#{number}".lower()


def parse_github_key(key: str | None) -> tuple[str, int] | None:
    if not key:
        return None
    match = _GITHUB_KEY.match(key.strip().lower())
    if match is None:
        return None
    return match.group("repo"), int(match.group("number"))


def reply_target(correlation_key: str | None) -> tuple[str, dict[str, Any]] | None:
    github = parse_github_key(correlation_key)
    if github is not None:
        return "github", {"repo": github[0], "number": github[1]}
    if correlation_key and correlation_key.startswith("feishu:p2p:"):
        return "feishu", {"chat_id": correlation_key.removeprefix("feishu:p2p:")}
    if correlation_key and correlation_key.startswith("feishu:thread:"):
        return "feishu", {"reply_to_message_id": correlation_key.removeprefix("feishu:thread:")}
    return None


def address_target(channel: str, address: str) -> dict[str, Any]:
    if channel == "github":
        parsed = parse_github_key(f"github:{address}")
        if parsed is not None:
            return {"repo": parsed[0], "number": parsed[1]}
    if channel == "feishu":
        return {"chat_id": address}
    return {"address": address}
