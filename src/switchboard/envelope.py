from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Final, Literal, cast, get_args

Source = Literal["github", "feishu", "mcp", "timer", "taskboard", "hibernaut"]
EventType = Literal[
    "work.opened",
    "work.updated",
    "work.closed",
    "work.ready",
    "conversation.message",
    "approval.decided",
    "execution.reported",
]
OutcomeKind = Literal["emit", "suspend", "error", "done"]

SOURCES: Final[frozenset[str]] = frozenset(get_args(Source))
EVENT_TYPES: Final[frozenset[str]] = frozenset(get_args(EventType))
OUTCOME_KINDS: Final[frozenset[str]] = frozenset(get_args(OutcomeKind))
SCHEMA_VERSION: Final = 1

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def new_ulid() -> str:
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    chars: list[str] = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def new_event_id() -> str:
    return f"evt_{new_ulid()}"


def utc_now() -> datetime:
    return datetime.now(UTC)


# One fixed-width format so stored timestamps compare correctly as strings in SQL.
def format_utc(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime(_ISO_FORMAT)


def parse_utc(text: str) -> datetime:
    normalized = text.strip()
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    moment = datetime.fromisoformat(normalized)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def normalize_utc(text: str | None, fallback: datetime) -> str:
    if not text:
        return format_utc(fallback)
    try:
        return format_utc(parse_utc(text))
    except ValueError:
        return format_utc(fallback)


def _freeze_metadata(metadata: Mapping[str, Any]) -> MappingProxyType[str, Any]:
    plain = dict(metadata)
    try:
        json.dumps(plain, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"metadata must be JSON-serializable: {exc}") from exc
    return MappingProxyType(plain)


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    source: Source
    event_type: EventType
    external_id: str
    scope: str
    subject: str = ""
    body: str = ""
    correlation_key: str | None = None
    actor: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    occurred_at: str = field(default_factory=lambda: format_utc(utc_now()))
    event_id: str = field(default_factory=new_event_id)
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "schema_version": self.schema_version,
            "source": self.source,
            "event_type": self.event_type,
            "external_id": self.external_id,
            "occurred_at": self.occurred_at,
            "scope": self.scope,
            "correlation_key": self.correlation_key,
            "actor": self.actor,
            "subject": self.subject,
            "body": self.body,
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CanonicalEvent:
        source = str(data["source"])
        event_type = str(data["event_type"])
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r}")
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unknown event_type {event_type!r}")
        raw_metadata: Any = data.get("metadata") or {}
        if not isinstance(raw_metadata, Mapping):
            raise ValueError("metadata must be an object")
        metadata = cast(Mapping[str, Any], raw_metadata)
        return cls(
            event_id=str(data["event_id"]),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            source=cast(Source, source),
            event_type=cast(EventType, event_type),
            external_id=str(data["external_id"]),
            occurred_at=str(data["occurred_at"]),
            scope=str(data["scope"]),
            correlation_key=_optional_str(data.get("correlation_key")),
            actor=_optional_str(data.get("actor")),
            subject=str(data.get("subject") or ""),
            body=str(data.get("body") or ""),
            metadata=metadata,
        )

    @classmethod
    def from_json(cls, text: str) -> CanonicalEvent:
        return cls.from_dict(cast(Mapping[str, Any], json.loads(text)))


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


class EnvelopeError(ValueError):
    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


_TOP_LEVEL_REQUIRED: Final[Mapping[str, tuple[str, ...]]] = {
    "work.opened": ("correlation_key", "subject"),
    "work.updated": ("correlation_key", "subject"),
    "work.closed": ("correlation_key", "subject"),
    "work.ready": ("correlation_key", "subject"),
    "conversation.message": ("correlation_key", "body"),
    "approval.decided": ("actor",),
    "execution.reported": ("subject",),
}

_METADATA_REQUIRED: Final[Mapping[str, tuple[str, ...]]] = {
    "work.opened": ("work_kind", "state"),
    "work.updated": ("work_kind", "state"),
    "work.closed": ("work_kind", "state"),
    "work.ready": ("work_kind", "state", "task_id"),
    "conversation.message": ("channel",),
    "approval.decided": ("approval_request_id", "decision"),
    "execution.reported": ("run_id", "session_id", "outcome_kind"),
}


def validate_envelope(event: CanonicalEvent) -> None:
    problems: list[str] = []
    if not event.event_id.startswith("evt_") or len(event.event_id) != 30:
        problems.append("event_id must be 'evt_' + 26-char ULID")
    if event.schema_version != SCHEMA_VERSION:
        problems.append(f"unsupported schema_version {event.schema_version}")
    if event.source not in SOURCES:
        problems.append(f"unknown source {event.source!r}")
    if event.event_type not in EVENT_TYPES:
        problems.append(f"unknown event_type {event.event_type!r}")
    if not event.external_id:
        problems.append("missing external_id")
    if not event.scope:
        problems.append("missing scope")
    try:
        parse_utc(event.occurred_at)
    except ValueError:
        problems.append(f"occurred_at is not ISO-8601: {event.occurred_at!r}")

    for name in _TOP_LEVEL_REQUIRED.get(event.event_type, ()):
        if not getattr(event, name):
            problems.append(f"{event.event_type} requires {name}")
    for key in _METADATA_REQUIRED.get(event.event_type, ()):
        value = event.metadata.get(key)
        if value is None or value == "":
            problems.append(f"{event.event_type} requires metadata.{key}")
    if event.event_type == "execution.reported":
        outcome = event.metadata.get("outcome_kind")
        if not isinstance(outcome, str) or outcome not in OUTCOME_KINDS:
            problems.append(f"metadata.outcome_kind must be one of {sorted(OUTCOME_KINDS)}")
        if outcome == "suspend":
            for key in ("approval_request_id", "session_id"):
                identifier = event.metadata.get(key)
                if not isinstance(identifier, str) or not identifier.strip():
                    problems.append(f"suspend requires non-empty metadata.{key}")
        elif not event.correlation_key:
            problems.append("execution.reported requires correlation_key")
    if event.event_type == "approval.decided":
        if event.metadata.get("decision") not in ("approve", "reject"):
            problems.append("metadata.decision must be approve or reject")
        session_id = event.metadata.get("session_id")
        if not event.correlation_key and not (
            event.source == "feishu" and isinstance(session_id, str) and session_id.strip()
        ):
            problems.append("approval.decided requires correlation_key or a Feishu card session")
    if problems:
        raise EnvelopeError(problems)
