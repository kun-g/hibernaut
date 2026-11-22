from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, cast

# Reverse-engineered from Claude Code; verified against these releases. A session advertising any other
# `peerProtocol` is refused (see sender.check_protocol) until the format is re-verified.
SUPPORTED_PEER_PROTOCOLS: Final = frozenset({1})
VERIFIED_CLAUDE_CODE_VERSIONS: Final = ("2.1.316", "2.1.330")

PEER_PROTOCOL: Final = 1
FRAME_VERSION: Final = 1
# Claude Code only accepts these from-mode values. A sender whose declared mode differs from the receiving
# session's own mode has its message held for the operator's approval; Orca's `dontAsk` sessions report `prompting`.
PEER_MODES: Final = ("bypass", "prompting")
DEFAULT_MODE: Final = "prompting"

TAG: Final = "cross-session-message"
_ADDR_SAFE: Final = "A-Za-z0-9:_/.\\-"
_ADDR_RE: Final = re.compile(f"[{_ADDR_SAFE}%]{{1,300}}")
_ATTR_RE: Final = re.compile(r'([a-z][a-z-]*)="([^"]*)"')
# Claude Code parses the envelope with an anchored, greedy body, so the body needs no escaping: a closing tag
# inside it is not the end of the message.
_ENVELOPE_RE: Final = re.compile(
    rf'\A<{TAG}((?: [a-z][a-z-]*="[^"]*")*)>\n?([\s\S]*?)\n?</{TAG}>\s*\Z')


@dataclass(frozen=True, slots=True)
class Envelope:
    body: str
    from_addr: str | None
    from_name: str | None
    from_mode: str | None
    attrs: Mapping[str, str]


def uds_address(socket_path: str) -> str:
    # Same percent-encoding Claude Code applies to socket paths in `from` attributes.
    return "uds:" + urllib.parse.quote(socket_path, safe=":_/.-")


def clean_name(name: str) -> str:
    cleaned = re.sub(r'["<>]', "", name)
    if not cleaned or "\n" in cleaned or "\r" in cleaned:
        raise ValueError(f"invalid peer name: {name!r}")
    return cleaned


def encode_envelope(body: str, *, from_addr: str | None, from_name: str, from_mode: str = DEFAULT_MODE) -> str:
    if from_mode not in PEER_MODES:
        raise ValueError(f"from_mode must be one of {PEER_MODES}, got {from_mode!r}")
    attrs: list[str] = []
    if from_addr is not None:
        if not _ADDR_RE.fullmatch(from_addr):
            raise ValueError(f"invalid from address: {from_addr!r}")
        attrs.append(f'from="{from_addr}"')
    attrs.append(f'from-name="{clean_name(from_name)}"')
    attrs.append(f'from-mode="{from_mode}"')
    return f"<{TAG} {' '.join(attrs)}>\n{body}\n</{TAG}>"


def decode_envelope(content: str) -> Envelope | None:
    m = _ENVELOPE_RE.match(content)
    if m is None:
        return None
    attrs = dict(_ATTR_RE.findall(m.group(1)))
    return Envelope(body=m.group(2), from_addr=attrs.get("from"), from_name=attrs.get("from-name"),
                    from_mode=attrs.get("from-mode"), attrs=attrs)


def new_msg_id() -> str:
    return str(uuid.uuid4())


def user_frame(content: str, *, msg_id: str, from_addr: str | None) -> dict[str, Any]:
    frame: dict[str, Any] = {"msgV": FRAME_VERSION, "msg_id": msg_id, "type": "user",
                             "message": {"role": "user", "content": content}, "priority": "next"}
    if from_addr is not None:
        frame["from"] = from_addr
    return frame


def auth_line(token: str) -> bytes:
    return json.dumps({"type": "auth", "token": token}).encode() + b"\n"


def frame_line(frame: Mapping[str, Any]) -> bytes:
    # JSON escapes raw newlines, so one frame is always one line.
    return json.dumps(frame, ensure_ascii=False).encode("utf-8") + b"\n"


def key_filename(pid: int, socket_path: str) -> str:
    return f"{pid}.{hashlib.sha256(socket_path.encode()).hexdigest()}.key"


def _content_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts: list[str] = []
        for block in cast(list[object], content):
            if isinstance(block, dict):
                item = cast(dict[str, object], block)
                text = item.get("text")
                if item.get("type") == "text" and isinstance(text, str):
                    texts.append(text)
        if texts:
            return "\n".join(texts)
    return json.dumps(content, ensure_ascii=False)


def frame_to_record(frame: Mapping[str, Any], *, now: float) -> dict[str, Any] | None:
    kind = frame.get("type")
    msg_id = frame.get("msg_id")
    if not isinstance(msg_id, str) or not msg_id:
        msg_id = "local-" + new_msg_id()
    frame_from = frame.get("from")
    if kind == "user":
        message = frame.get("message")
        content = cast(Mapping[str, object], message).get("content", "") if isinstance(message, Mapping) else ""
        text = _content_text(content)
        env = decode_envelope(text)
        if env is None:
            return {"msg_id": msg_id, "ts": now, "type": "user", "from_addr": frame_from, "from_name": None,
                    "from_mode": None, "body": text, "unwrapped": True}
        return {"msg_id": msg_id, "ts": now, "type": "user", "from_addr": env.from_addr or frame_from,
                "from_name": env.from_name, "from_mode": env.from_mode, "body": env.body}
    if kind == "control":
        record: dict[str, Any] = {k: v for k, v in frame.items() if k not in ("msgV", "from")}
        record.update(msg_id=msg_id, ts=now, type="control", from_addr=frame_from)
        return record
    return None
