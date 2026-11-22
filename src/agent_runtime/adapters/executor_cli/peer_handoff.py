from __future__ import annotations

import json
import os
import re
import threading
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import ccpeer
from ccpeer import CursorError, Inbox, PeerError, ReceiverInfo, Session

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError
from agent_runtime.adapters.executor_cli.handoff_mcp import schema_errors
from agent_runtime.adapters.storage_sqlite.peer_mailbox import PeerRecord, SQLitePeerMailbox

TASK_HEADER = "hibernaut-task"
RESULT_IDS = ("task_id", "attempt_id")
DEFAULT_RECEIVER = "hibernaut"
# Orca's dontAsk sessions report `prompting`; a message declaring the same mode is never held.
SEND_MODE = "prompting"
_FENCE_RE = re.compile(r"\A```(?:json)?\s*\n([\s\S]*?)\n?```\Z")


def _home_claude() -> tuple[Path, ...]:
    return (Path.home() / ".claude",)


@dataclass(frozen=True, slots=True)
class PeerHandoffConfig:
    receiver: str = DEFAULT_RECEIVER
    # The receiver's state root (ccpeer default ~/.ccpeer when None).
    state_dir: Path | None = None
    # Where the launched sessions register. They start without CLAUDE_CONFIG_DIR, so ~/.claude by default.
    config_dirs: tuple[Path, ...] = field(default_factory=_home_claude)
    # Replies that are not a valid result get a "resend" this many times before the conversation fails.
    max_resends: int = 3
    register_timeout: float = 120.0
    exit_timeout: float = 30.0


def same_address(a: str | None, b: str | None) -> bool:
    return a is not None and b is not None and urllib.parse.unquote(a) == urllib.parse.unquote(b)


def task_message(prompt: str, *, task_id: str, attempt_id: str, reply_to: str, schema_name: str,
                 schema: Mapping[str, Any]) -> str:
    header = json.dumps({"task_id": task_id, "attempt_id": attempt_id, "reply_to": reply_to, "schema": schema_name},
                        ensure_ascii=False)
    shape = {"task_id": task_id, "attempt_id": attempt_id, "...": "the result fields"}
    return (
        f"{TASK_HEADER} {header}\n\n{prompt}\n\n## Result handoff\n"
        "A person may watch this terminal and answer questions here; keep progress visible. As the final action, "
        f"send your result with SendMessage to \"{reply_to}\": the message is exactly one JSON object and nothing "
        f"else, {json.dumps(shape, ensure_ascii=False)}, carrying task_id \"{task_id}\", attempt_id \"{attempt_id}\" "
        "and the fields of this JSON Schema. That message is the only way the result is read; if Hibernaut answers "
        "that it is malformed, fix it and send it again.\n"
        f"Result schema ({schema_name}):\n```json\n{json.dumps(schema, ensure_ascii=False)}\n```"
    )


def resend_message(error: str, *, task_id: str, attempt_id: str, reply_to: str, tries_left: int) -> str:
    return (
        f"{TASK_HEADER} {json.dumps({'task_id': task_id, 'attempt_id': attempt_id, 'reply_to': reply_to})}\n\n"
        f"Your result message was not accepted: {error}. Send it again with SendMessage to \"{reply_to}\" as exactly "
        f"one JSON object with task_id \"{task_id}\", attempt_id \"{attempt_id}\" and the result fields; "
        f"{tries_left} more malformed repl{'y' if tries_left == 1 else 'ies'} fail this turn."
    )


def parse_result(body: str, schema: Mapping[str, Any], *, task_id: str, attempt_id: str) -> tuple[dict[str, Any] | None, str]:
    # JSON syntax, then the schema, then that the reply is for this attempt.
    text = body.strip()
    fenced = _FENCE_RE.match(text)
    if fenced is not None:
        text = fenced.group(1).strip()
    try:
        raw: object = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"it is not valid JSON ({exc.msg} at line {exc.lineno} column {exc.colno})"
    if not isinstance(raw, dict):
        return None, "it is not a JSON object"
    data = cast(dict[str, Any], raw)
    full = dict(schema)
    full["properties"] = {**{k: {"type": "string"} for k in RESULT_IDS}, **cast(dict[str, Any], schema.get("properties", {}))}
    full["required"] = [*RESULT_IDS, *cast(list[str], schema.get("required", []))]
    errors = schema_errors(data, full)
    if errors:
        return None, "; ".join(errors)
    if data["task_id"] != task_id:
        return None, f"task_id is \"{data['task_id']}\", expected \"{task_id}\""
    if data["attempt_id"] != attempt_id:
        return None, f"attempt_id is \"{data['attempt_id']}\", expected \"{attempt_id}\" (the current attempt)"
    return {k: v for k, v in data.items() if k not in RESULT_IDS}, ""


def require_receiver(config: PeerHandoffConfig) -> ReceiverInfo:
    info = ccpeer.receiver_info(config.receiver, config.state_dir)
    if info is None:
        where = ccpeer.state_dir(config.receiver, config.state_dir)
        raise ClaudeCodeError(
            f"the ccpeer receiver '{config.receiver}' is not running (no live receiver.json in {where}); "
            "start it under launchd as deploy/ccpeer/README.md describes"
        )
    return info


class PeerHandoff:
    # Hibernaut's side of the peer channel: sends to sessions as the receiver's name, reads replies from the
    # receiver's inbox through a persisted cursor. The receiver itself runs under launchd, never under Hibernaut.
    def __init__(self, config: PeerHandoffConfig, mailbox: SQLitePeerMailbox) -> None:
        if mailbox.receiver != config.receiver:
            raise ValueError("the mailbox belongs to another receiver")
        self.config = config
        self.mailbox = mailbox
        self._inbox = Inbox(ccpeer.state_dir(config.receiver, config.state_dir))
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self.config.receiver

    def receiver(self) -> ReceiverInfo:
        return require_receiver(self.config)

    def ingest(self) -> None:
        with self._lock:
            for log, jsonl in (("user", self._inbox.messages), ("control", self._inbox.control)):
                offset = self.mailbox.cursor(log)
                try:
                    page = jsonl.read(offset)
                except CursorError:
                    # The log was replaced or truncated: read it again from the start; msg_id dedupes.
                    page = jsonl.read(0)
                if page.records or page.cursor != offset:
                    self.mailbox.store(log, page.records, page.cursor)

    def replies(self, session: Session, since: float) -> list[PeerRecord]:
        return self.mailbox.take("user", _addresses(session), since)

    def replies_at(self, addresses: Sequence[str], since: float) -> list[PeerRecord]:
        # Replies from a session that may no longer be running, by the addresses it had.
        return self.mailbox.take("user", list(addresses), since)

    @staticmethod
    def addresses(session: Session) -> list[str]:
        return _addresses(session)

    def receipts(self, sent: Sequence[str], session: Session, since: float) -> list[PeerRecord]:
        return self.mailbox.take_controls(sent, _addresses(session), since)

    def find(self, native: str, workdir: Path, *, name: str | None = None, since: float | None = None) -> Session | None:
        # The conversation started with --session-id/--resume <native>, so its registration carries that sessionId.
        # Failing that, the one session started here since `since` under the --name given (Claude Code may add a
        # "-<words>" suffix when another live session holds the name).
        here = os.path.realpath(workdir)
        live = [s for s in ccpeer.discover(self.config.config_dirs)
                if not s.standalone and (s.cwd is None or os.path.realpath(s.cwd) == here)]
        hits = [s for s in live if s.session_id == native]
        if not hits and name is not None and since is not None:
            hits = [s for s in live if s.name is not None and (s.name == name or s.name.startswith(f"{name}-"))
                    and _started_ms(s) >= since * 1000]
        return hits[0] if len(hits) == 1 else None

    def send(self, session: Session, body: str) -> str:
        info = self.receiver()
        try:
            return ccpeer.send(session, body, sender_name=self.config.receiver, reply_addr=info.address,
                               mode=SEND_MODE).msg_id
        except PeerError as exc:
            raise ClaudeCodeError(f"could not message {session.label()}: {type(exc).__name__} {exc}") from exc


def _started_ms(session: Session) -> float:
    started = session.raw.get("startedAt")
    return float(started) if isinstance(started, (int, float)) else 0.0


def _addresses(session: Session) -> list[str]:
    raw = f"uds:{session.socket_path}"
    return list(dict.fromkeys((session.address, raw)))
