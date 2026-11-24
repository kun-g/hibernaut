from __future__ import annotations

import json
import logging
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.executor_cli.quota import one_line
from agent_runtime.adapters.executor_cli.reviewer import Reviewer
from agent_runtime.adapters.storage_sqlite.reviewers import QuotaMark, ReviewerChoice, SQLiteReviewerStore

_TABLE_VERSION = 1
_MAX_TABLE_BYTES = 1024 * 1024
_NOT_CONFIGURED = "not configured any more"
_CHOSE = "chose "
_log = logging.getLogger(__name__)


def read_availability(path: Path) -> dict[str, str]:
    # Executors the table rules out, with its reasons. What cannot be read rules nothing out: reviews go on in the
    # configured order. The table's age is its writer's business.
    try:
        return _unavailable(path)
    except FileNotFoundError:
        problem = "is missing"
    except OSError as exc:
        problem = f"cannot be read ({exc.strerror or exc})"
    except (ValueError, RecursionError) as exc:
        problem = f"cannot be used ({one_line(str(exc))})"
    _log.warning("availability table %s %s: every review candidate counts as available", path, problem)
    return {}


def _unavailable(path: Path) -> dict[str, str]:
    # Opening a pipe would wait for its writer.
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("not a regular file")
    with path.open("rb") as stream:
        raw = stream.read(_MAX_TABLE_BYTES + 1)
    if len(raw) > _MAX_TABLE_BYTES:
        raise ValueError(f"larger than {_MAX_TABLE_BYTES} bytes")
    data: object = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("not a JSON object")
    table = cast(dict[str, object], data)
    version = table.get("version")
    if isinstance(version, bool) or version != _TABLE_VERSION:
        raise ValueError(f"version {version!r}, this version reads {_TABLE_VERSION}")
    executors = table.get("executors")
    if not isinstance(executors, dict):
        raise ValueError("executors is not an object")
    found: dict[str, str] = {}
    for name, item in cast(dict[str, object], executors).items():
        entry = cast(dict[str, object], item) if isinstance(item, dict) else {}
        available, reason = entry.get("available"), entry.get("reason")
        if not isinstance(available, bool):
            raise ValueError(f"executors.{name}.available is not true or false")
        if not available:
            found[name] = (one_line(reason) if isinstance(reason, str) else "") or "no reason given"
    return found


@dataclass(frozen=True, slots=True)
class SwitchNotice:
    # Where the notice of a switch goes and what its text starts with.
    inbox_id: str
    subject: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PickedReviewer:
    # None: no candidate is available, and refused says why each one is not.
    reviewer: Reviewer | None
    refused: tuple[tuple[str, str], ...] = ()


class ReviewerPool:
    # Who reviews a task. A single configured reviewer is used as it is; a list is gone through in order, once per
    # task, and the choice is kept in the database until that reviewer runs out of quota.
    def __init__(
        self,
        *,
        configured: str | tuple[str, ...],
        # Over every declared reviewer: one a task already has may no longer be in the list.
        lookup: Callable[[str], Reviewer | None],
        store: SQLiteReviewerStore,
        # The sde executor: its list is its own, so another one working on the same session chooses for itself.
        owner: str,
        table: str,
        cooldown_seconds: float,
        notify: Callable[[SwitchNotice, str], object],
    ) -> None:
        self._configured = configured
        self._lookup = lookup
        self._store = store
        self._owner = owner
        self._table = table
        self._cooldown = cooldown_seconds
        self._notify = notify

    @property
    def listed(self) -> bool:
        return not isinstance(self._configured, str)

    def applies(self, session_id: str) -> bool:
        # False: the task has a single reviewer, and nothing about choosing one is said or done.
        return self.listed or bool(self._store.choice(session_id, self._owner).candidates)

    def pick(
        self, session_id: str, *, round_no: int, tried: Mapping[str, str], notice: SwitchNotice
    ) -> PickedReviewer:
        # tried: reviewers that ran out of quota in this round, with the reason; not asked again in it.
        choice = self._store.choice(session_id, self._owner)
        if isinstance(self._configured, str) and not choice.candidates:
            # Recorded so that a list configured later leaves the task with the reviewer it has.
            if choice.reviewer != self._configured:
                self._store.save(session_id, self._owner, replace(choice, reviewer=self._configured))
            return PickedReviewer(self._lookup(self._configured))
        if choice.reviewer:
            kept = self._lookup(choice.reviewer)
            if kept is not None:
                return PickedReviewer(kept)
            choice = replace(choice, reviewer="", left_name=choice.reviewer,
                             left=f"round {round_no}: {choice.reviewer} is not available: {_NOT_CONFIGURED}")
        configured = (self._configured,) if isinstance(self._configured, str) else self._configured
        candidates = choice.candidates or configured
        unavailable = read_availability(Path(self._table)) if self._table else {}
        marks = self._store.marks(time.time())
        refused: list[tuple[str, str]] = []
        for name in candidates:
            reviewer = self._lookup(name)
            reason = self._refusal(name, reviewer, unavailable, marks, tried)
            if reviewer is not None and not reason:
                self._settle(session_id, choice, name, candidates, refused, notice)
                return PickedReviewer(reviewer)
            refused.append((name, reason))
        # Once a person has decided, the list configured by then counts.
        self._store.save(session_id, self._owner, replace(choice, candidates=()))
        return PickedReviewer(None, tuple(refused))

    @staticmethod
    def _refusal(
        name: str, reviewer: Reviewer | None, unavailable: Mapping[str, str], marks: Mapping[str, QuotaMark],
        tried: Mapping[str, str],
    ) -> str:
        if reviewer is None:
            return _NOT_CONFIGURED
        mark = marks.get(name)
        if mark is not None:
            return f"ran out of quota ({mark.reason}), not used until {_local_time(mark.until)}"
        if name in tried:
            return f"ran out of quota ({tried[name]})"
        return unavailable.get(name, "")

    def _settle(
        self, session_id: str, choice: ReviewerChoice, name: str, candidates: tuple[str, ...],
        refused: list[tuple[str, str]], notice: SwitchNotice,
    ) -> None:
        passed_over = "; ".join(f"{n} is not available: {why}" for n, why in refused if n != choice.left_name)
        if not choice.left:
            note = f"{_CHOSE}{name}"
        elif name == choice.left_name:
            note = f"{choice.left}, chosen again"
        else:
            note = f"{choice.left}, switched to {name}"
        note += f" ({passed_over})" if passed_over else ""
        settled = ReviewerChoice(reviewer=name, candidates=candidates, notes=(*choice.notes, note))
        switched = bool(choice.left) and name != choice.left_name
        self._store.save(session_id, self._owner, settled,
                         (lambda: self._notify(notice, f"{notice.subject}: {note}")) if switched else None)

    def exhausted(self, session_id: str, name: str, *, reason: str, round_no: int) -> None:
        self._store.mark(name, until=time.time() + self._cooldown, reason=reason)
        choice = self._store.choice(session_id, self._owner)
        if not self.listed and not choice.candidates:
            return
        self._store.save(session_id, self._owner, replace(choice, reviewer="", left_name=name,
                                             left=f"round {round_no}: {name} ran out of quota ({reason})"))

    def account(self, session_id: str) -> str:
        # Who was chosen and why, for the messages and the pull request; "" while nobody was.
        if not self.applies(session_id):
            return ""
        choice = self._store.choice(session_id, self._owner)
        parts = [*choice.notes, *([choice.left] if choice.left else [])]
        if len(choice.notes) == 1 and choice.notes[0].startswith(_CHOSE) and not choice.left:
            parts.append("not switched")
        return "; ".join(parts)


def _local_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%d %H:%M %z")
