from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Protocol

from collaboration.models import (
    CloseoutCard,
    CloseoutDecision,
    CloseoutMemoryDraft,
    CloseoutSyncResult,
    utc_now_iso,
)


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

    def save(self, card: CloseoutCard) -> None:
        path = self._dir / f"{card.card_id}.json"
        path.write_text(json.dumps(card.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, card_id: str) -> CloseoutCard:
        path = self._dir / f"{card_id}.json"
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
        for path in sorted(self._dir.glob("*.json")):
            card = self.get(path.stem)
            if status is None or card.status == status:
                out.append(card)
        return out


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
            memory_proposals=memory_proposals,
            status="draft",
            created_at=utc_now_iso(),
        )
        self._store.save(card)
        return card

    def confirm_and_sync(self, card_id: str, *, confirmed_by: str) -> CloseoutSyncResult:
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
        if not confirmed_by.strip():
            raise ValueError("confirmed_by must be non-empty for explicit closeout (D6-A)")

        confirmed = CloseoutCard(
            schema_version=card.schema_version,
            card_id=card.card_id,
            session_id=card.session_id,
            agent_id=card.agent_id,
            project_key=card.project_key,
            task_id=card.task_id,
            summary=card.summary,
            decisions=card.decisions,
            follow_up_tasks=card.follow_up_tasks,
            memory_proposals=card.memory_proposals,
            status="confirmed",
            confirmed_by=confirmed_by.strip(),
            confirmed_at=utc_now_iso(),
            created_at=card.created_at,
        )
        self._store.save(confirmed)

        chief_notified = False
        if self._chief_sink is not None:
            self._chief_sink.record_closeout_summary(confirmed)
            chief_notified = True

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

        queued_proposals: list[str] = []
        if self._memory_sink is not None:
            for draft in confirmed.memory_proposals:
                queued_proposals.append(
                    self._memory_sink.submit_memory_proposal(
                        target_uri=draft.target_uri,
                        content=draft.content,
                        reason=draft.reason,
                        proposed_by=confirmed.agent_id,
                    )
                )

        synced = CloseoutCard(
            schema_version=confirmed.schema_version,
            card_id=confirmed.card_id,
            session_id=confirmed.session_id,
            agent_id=confirmed.agent_id,
            project_key=confirmed.project_key,
            task_id=confirmed.task_id,
            summary=confirmed.summary,
            decisions=confirmed.decisions,
            follow_up_tasks=confirmed.follow_up_tasks,
            memory_proposals=confirmed.memory_proposals,
            status="synced",
            confirmed_by=confirmed.confirmed_by,
            confirmed_at=confirmed.confirmed_at,
            created_at=confirmed.created_at,
        )
        self._store.save(synced)
        return CloseoutSyncResult(
            card_id=synced.card_id,
            chief_of_staff_notified=chief_notified,
            tasks_created=tuple(created_tasks),
            proposals_queued=tuple(queued_proposals),
            already_synced=False,
        )

    def discard(self, card_id: str) -> CloseoutCard:
        card = self._store.get(card_id)
        if card.status == "synced":
            raise ValueError(f"Cannot discard already synced CloseoutCard: {card_id}")
        discarded = CloseoutCard(
            schema_version=card.schema_version,
            card_id=card.card_id,
            session_id=card.session_id,
            agent_id=card.agent_id,
            project_key=card.project_key,
            task_id=card.task_id,
            summary=card.summary,
            decisions=card.decisions,
            follow_up_tasks=card.follow_up_tasks,
            memory_proposals=card.memory_proposals,
            status="discarded",
            confirmed_by=card.confirmed_by,
            confirmed_at=card.confirmed_at,
            created_at=card.created_at,
        )
        self._store.save(discarded)
        return discarded
