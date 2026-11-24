from __future__ import annotations

import json
import re
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Protocol

from collaboration._files import ExclusiveLock, atomic_write_text
from collaboration.models import (
    CloseoutCard,
    CloseoutDecision,
    CloseoutMemoryDraft,
    CloseoutSyncResult,
    utc_now_iso,
)


_CARD_ID_RE = re.compile(r"closeout-[0-9a-f]{12}")


# Confirming again after a crash or a failed sink repeats every call, so each sink must take a repeated call for the
# same card as the one it already has.
class ChiefOfStaffSinkPort(Protocol):
    def record_closeout_summary(self, card: CloseoutCard) -> None: ...


class TaskboardSinkPort(Protocol):
    def create_follow_up_task(self, *, project_key: str, title: str, source_card_id: str) -> str: ...


class MemoryProposalSinkPort(Protocol):
    def submit_memory_proposal(
        self,
        *,
        target_uri: str,
        content: str,
        reason: str,
        proposed_by: str,
    ) -> str: ...


class CloseoutStore:
    def __init__(self, store_dir: Path | str) -> None:
        self._dir = Path(store_dir).expanduser().resolve()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = ExclusiveLock(self._dir / ".closeouts.lock")

    @contextmanager
    def exclusive(self) -> Generator[None]:
        with self._lock.hold():
            yield

    def save(self, card: CloseoutCard) -> None:
        text = json.dumps(card.to_dict(), ensure_ascii=False, indent=2)
        with self.exclusive():
            atomic_write_text(self._path(card.card_id), text)

    def get(self, card_id: str) -> CloseoutCard:
        path = self._path(card_id)
        if not path.is_file():
            raise KeyError(f"Unknown CloseoutCard: {card_id}")
        raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return CloseoutCard(
            schema_version=raw.get("schema_version", "1.0"),
            card_id=raw["card_id"],
            session_id=raw["session_id"],
            agent_id=raw["agent_id"],
            project_key=raw["project_key"],
            task_id=raw.get("task_id"),
            summary=raw["summary"],
            decisions=tuple(CloseoutDecision(**d) for d in raw.get("decisions", [])),
            follow_up_tasks=tuple(raw.get("follow_up_tasks", [])),
            memory_proposals=tuple(CloseoutMemoryDraft(**m) for m in raw.get("memory_proposals", [])),
            status=raw["status"],
            confirmed_by=raw.get("confirmed_by"),
            confirmed_at=raw.get("confirmed_at"),
            created_at=raw["created_at"],
        )

    def list_cards(self, *, status: str | None = None) -> list[CloseoutCard]:
        out: list[CloseoutCard] = []
        for path in sorted(self._dir.glob("closeout-*.json")):
            try:
                card = self.get(path.stem)
            except (KeyError, TypeError, ValueError):
                continue
            if status is None or card.status == status:
                out.append(card)
        return out

    def _path(self, card_id: str) -> Path:
        if not _CARD_ID_RE.fullmatch(card_id):
            raise KeyError(f"Unknown CloseoutCard: {card_id}")
        return self._dir / f"{card_id}.json"


def _merged(drafts: tuple[CloseoutMemoryDraft, ...]) -> tuple[CloseoutMemoryDraft, ...]:
    # Two proposals for one file would share a base, so approving one would turn the other into a conflict.
    by_uri: dict[str, list[CloseoutMemoryDraft]] = {}
    for draft in drafts:
        by_uri.setdefault(draft.target_uri, []).append(draft)
    return tuple(
        CloseoutMemoryDraft(
            target_uri=uri,
            content="\n".join(d.content.strip("\n") for d in group),
            reason="; ".join(d.reason for d in group),
        ) if len(group) > 1 else group[0]
        for uri, group in by_uri.items()
    )


class CloseoutCoordinator:
    def __init__(
        self,
        store: CloseoutStore,
        *,
        chief_sink: ChiefOfStaffSinkPort | None = None,
        taskboard_sink: TaskboardSinkPort | None = None,
        memory_sink: MemoryProposalSinkPort | None = None,
    ) -> None:
        self._store = store
        self._chief_sink = chief_sink
        self._taskboard_sink = taskboard_sink
        self._memory_sink = memory_sink

    def emit_draft(
        self,
        *,
        session_id: str,
        agent_id: str,
        project_key: str,
        summary: str,
        decisions: tuple[CloseoutDecision, ...],
        task_id: str | None = None,
        follow_up_tasks: tuple[str, ...] = (),
        memory_proposals: tuple[CloseoutMemoryDraft, ...] = (),
    ) -> CloseoutCard:
        if not summary.strip():
            raise ValueError("CloseoutCard summary must be non-empty")
        if not decisions:
            raise ValueError("CloseoutCard must include at least one explicit decision")
        card = CloseoutCard(
            card_id=f"closeout-{uuid.uuid4().hex[:12]}",
            session_id=session_id.strip(),
            agent_id=agent_id.strip(),
            project_key=project_key.strip(),
            task_id=task_id,
            summary=summary.strip(),
            decisions=decisions,
            follow_up_tasks=tuple(t.strip() for t in follow_up_tasks if t.strip()),
            memory_proposals=_merged(memory_proposals),
            status="draft",
            created_at=utc_now_iso(),
        )
        self._store.save(card)
        return card

    def confirm_and_sync(self, card_id: str, *, confirmed_by: str) -> CloseoutSyncResult:
        with self._store.exclusive():
            return self._confirm_and_sync(card_id, confirmed_by.strip())

    def _confirm_and_sync(self, card_id: str, confirmed_by: str) -> CloseoutSyncResult:
        card = self._store.get(card_id)
        if card.status == "synced":
            return CloseoutSyncResult(
                card_id=card.card_id,
                chief_of_staff_notified=False,
                tasks_created=(),
                proposals_queued=(),
                already_synced=True,
            )
        if card.status == "discarded":
            raise ValueError(f"Cannot confirm discarded CloseoutCard: {card_id}")
        if not confirmed_by:
            raise ValueError("confirmed_by must be non-empty for explicit closeout (D6-A)")
        memory_sink = self._memory_sink_for(card)
        if card.follow_up_tasks and self._taskboard_sink is None:
            raise ValueError(f"{card_id} has follow-up tasks, but no Taskboard sink is configured")
        if card.memory_proposals and memory_sink is None:
            raise ValueError(f"{card_id} has memory proposals, but no memory sink is configured")

        # A card that failed half-way keeps who confirmed it, and when.
        confirmed = card if card.status == "confirmed" else replace(
            card, status="confirmed", confirmed_by=confirmed_by, confirmed_at=utc_now_iso()
        )
        self._store.save(confirmed)

        queued_proposals: list[str] = []
        if memory_sink is not None:
            for draft in confirmed.memory_proposals:
                queued_proposals.append(
                    memory_sink.submit_memory_proposal(
                        target_uri=draft.target_uri,
                        content=draft.content,
                        reason=draft.reason,
                        proposed_by=confirmed.agent_id,
                    )
                )

        created_tasks: list[str] = []
        if self._taskboard_sink is not None:
            for title in confirmed.follow_up_tasks:
                created_tasks.append(
                    self._taskboard_sink.create_follow_up_task(
                        project_key=confirmed.project_key,
                        title=title,
                        source_card_id=confirmed.card_id,
                    )
                )

        # Last: the Chief of Staff hears of a closeout whose tasks and proposals are all there.
        chief_notified = False
        if self._chief_sink is not None:
            self._chief_sink.record_closeout_summary(confirmed)
            chief_notified = True

        synced = replace(confirmed, status="synced")
        self._store.save(synced)
        return CloseoutSyncResult(
            card_id=synced.card_id,
            chief_of_staff_notified=chief_notified,
            tasks_created=tuple(created_tasks),
            proposals_queued=tuple(queued_proposals),
            already_synced=False,
        )

    def _memory_sink_for(self, card: CloseoutCard) -> MemoryProposalSinkPort | None:
        # The port does not carry the card; a deployment overrides this to tie its proposals to it.
        return self._memory_sink

    def discard(self, card_id: str) -> CloseoutCard:
        with self._store.exclusive():
            card = self._store.get(card_id)
            if card.status == "synced":
                raise ValueError(f"Cannot discard already synced CloseoutCard: {card_id}")
            discarded = replace(card, status="discarded")
            self._store.save(discarded)
            return discarded
