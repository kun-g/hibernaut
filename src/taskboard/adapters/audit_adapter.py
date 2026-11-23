from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from taskboard.ports import AuditSinkPort


class NoopAuditSink:
    def emit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        del event_name, task_id, detail


@dataclass(slots=True)
class RecordingAuditSink:
    records: list[dict[str, Any]] = field(default_factory=lambda: [])

    def emit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        self.records.append(
            {
                "event_name": event_name,
                "task_id": task_id,
                "detail": dict(detail or {}),
            }
        )


# Side-channel audit adapter: missing or failing sinks degrade to stderr silently.
class SafeAuditAdapter:
    def __init__(self, inner: AuditSinkPort | None = None) -> None:
        self._inner = inner

    def emit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        if self._inner is None:
            return
        try:
            self._inner.emit(event_name, task_id=task_id, detail=detail)
        except Exception as exc:  # noqa: BLE001
            print(f"[taskboard] audit sink failed on {event_name}: {exc!r}", file=sys.stderr)


# The service's default sink: one JSON line per audit record on stderr, next to the process log.
class StderrAuditSink:
    def emit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        record = {"audit": event_name, "task_id": task_id, "detail": dict(detail or {})}
        print(f"[taskboard] {json.dumps(record, ensure_ascii=False, default=str)}", file=sys.stderr, flush=True)
