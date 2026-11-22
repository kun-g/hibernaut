from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from switchboard.envelope import CanonicalEvent, validate_envelope
from switchboard.ports import ConsumerHandler, IngestResult
from taskboard.adapters.audit_adapter import RecordingAuditSink, SafeAuditAdapter
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.adapters.switchboard_adapter import SwitchboardAdapter
from taskboard.core.errors import SchemaValidationError
from taskboard.engine.ingestion import IngestionService


@dataclass
class _FlakyBus:
    published: list[CanonicalEvent] = field(default_factory=lambda: [])
    subscribers: dict[str, ConsumerHandler] = field(default_factory=lambda: {})
    fail_next: int = 0
    failure: Exception = field(default_factory=lambda: RuntimeError("switchboard unavailable"))

    def publish(self, event: CanonicalEvent) -> IngestResult:
        validate_envelope(event)
        if self.fail_next > 0:
            self.fail_next -= 1
            raise self.failure
        self.published.append(event)
        return IngestResult(status="accepted", event_id=event.event_id)

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        self.subscribers[consumer_id] = handler


def _build(tmp_path: Path) -> tuple[SQLiteTaskStore, _FlakyBus, IngestionService, SwitchboardAdapter]:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    bus = _FlakyBus()
    audit = SafeAuditAdapter(RecordingAuditSink())
    adapter = SwitchboardAdapter(bus, audit=audit)
    service = IngestionService(store, publisher=adapter, audit=audit)
    adapter.bind_ingestion(service)
    return store, bus, service, adapter


def _complete_event(task_id: str) -> CanonicalEvent:
    meta: dict[str, Any] = {
        "outcome_kind": "done",
        "run_id": "run_1",
        "session_id": "sess_1",
        "task_id": task_id,
        "task_action": "complete",
        "trace_id": "trc_complete",
        "hop_count": 1,
    }
    return CanonicalEvent(
        source="hibernaut",
        event_type="execution.reported",
        external_id=f"exec:complete:{task_id}",
        scope="proj:hibernaut",
        correlation_key=task_id,
        subject="complete",
        metadata=meta,
    )


def _ready_ids(bus: _FlakyBus, task_id: str) -> list[str]:
    return [
        ev.external_id
        for ev in bus.published
        if ev.metadata.get("task_id") == task_id
        and (ev.event_type == "work.ready" or ev.metadata.get("notify_kind") == "human_todo")
    ]


def test_completion_fanout_is_replayed_when_the_report_is_redelivered(tmp_path: Path) -> None:
    store, bus, service, adapter = _build(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Up", initial_status="ready")
        first = service.tb_create_task("proj:hibernaut", "First", priority=0)
        second = service.tb_create_task("proj:hibernaut", "Second", priority=1)
        service.tb_manage_dependency("add", up.task_id, first.task_id)
        service.tb_manage_dependency("add", up.task_id, second.task_id)
        service.tb_transition_task(up.task_id, "in_progress")
        bus.published.clear()

        bus.fail_next = 1
        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "nack"
        assert store.get_task(up.task_id).status == "done"  # type: ignore[union-attr]

        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "acked"
        # The claim made before the failed publish is republished, not claimed again.
        assert _ready_ids(bus, first.task_id) == [f"tb:{first.task_id}:d1"]
        assert _ready_ids(bus, second.task_id) == [f"tb:{second.task_id}:d1"]
        assert store.get_task(first.task_id).dispatch_seq == 1  # type: ignore[union-attr]

        # A further redelivery republishes the same external_ids, which Switchboard dedupes.
        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "acked"
        assert set(_ready_ids(bus, first.task_id)) == {f"tb:{first.task_id}:d1"}
        assert store.get_task(second.task_id).dispatch_seq == 1  # type: ignore[union-attr]
    finally:
        store.close()


def test_outbound_schema_failure_is_nacked_not_acked(tmp_path: Path) -> None:
    store, bus, service, adapter = _build(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Up", initial_status="ready")
        down = service.tb_create_task("proj:hibernaut", "Down")
        service.tb_manage_dependency("add", up.task_id, down.task_id)
        service.tb_transition_task(up.task_id, "in_progress")

        bus.fail_next = 1
        bus.failure = SchemaValidationError("work_ready.metadata.schema.json", ["$.x: broken"])
        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "nack"
    finally:
        store.close()


def test_work_closed_cancellation_fanout_is_replayed(tmp_path: Path) -> None:
    store, bus, service, adapter = _build(tmp_path)
    try:
        up = service.tb_create_task(
            "proj:hibernaut", "Up", source_ref="gh:issue:7", source="github"
        )
        d1 = service.tb_create_task("proj:hibernaut", "D1", priority=0)
        d2 = service.tb_create_task("proj:hibernaut", "D2", priority=1)
        service.tb_manage_dependency("add", up.task_id, d1.task_id)
        service.tb_manage_dependency("add", up.task_id, d2.task_id)
        bus.published.clear()

        closed = CanonicalEvent(
            source="github",
            event_type="work.closed",
            external_id="gh:issue:7:closed",
            scope="proj:hibernaut",
            correlation_key="gh:issue:7",
            subject="Up",
            metadata={"state": "cancelled", "trace_id": "trc_close", "hop_count": 0},
        )
        bus.fail_next = 1
        assert adapter.handle_canonical_event(closed) == "nack"
        assert store.get_task(up.task_id).status == "cancelled"  # type: ignore[union-attr]

        assert adapter.handle_canonical_event(closed) == "acked"

        def notified() -> dict[str, list[str]]:
            out: dict[str, list[str]] = {}
            for ev in bus.published:
                if ev.metadata.get("notify_kind") == "upstream_cancelled":
                    out.setdefault(str(ev.metadata["task_id"]), []).append(ev.external_id)
            return out

        after_retry = notified()
        assert set(after_retry) == {d1.task_id, d2.task_id}
        for task_id in (d1.task_id, d2.task_id):
            assert store.get_task(task_id).blocked_reason == "upstream_cancelled"  # type: ignore[union-attr]

        assert adapter.handle_canonical_event(closed) == "acked"
        for task_id, ids in notified().items():
            assert set(ids) == set(after_retry[task_id])
    finally:
        store.close()


def test_work_opened_dispatch_is_replayed_when_redelivered(tmp_path: Path) -> None:
    store, bus, _, adapter = _build(tmp_path)
    try:
        opened = CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="gh:issue:9",
            scope="proj:hibernaut",
            correlation_key="gh:issue:9",
            subject="Ready on arrival",
            metadata={
                "work_kind": "issue",
                "state": "open",
                "trace_id": "trc_9",
                "hop_count": 0,
                "initial_status": "ready",
                "assignee": "agent:coder",
            },
        )
        bus.fail_next = 1
        assert adapter.handle_canonical_event(opened) == "nack"
        task = store.find_by_source_ref("gh:issue:9")
        assert task is not None and task.status == "ready"

        assert adapter.handle_canonical_event(opened) == "acked"
        assert _ready_ids(bus, task.task_id) == [f"tb:{task.task_id}:d1"]
    finally:
        store.close()


def test_human_todo_republish_keeps_its_external_id_after_an_edit(tmp_path: Path) -> None:
    store, bus, service, adapter = _build(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Up", initial_status="ready")
        down = service.tb_create_task("proj:hibernaut", "Approve", assignee="human:kun")
        service.tb_manage_dependency("add", up.task_id, down.task_id)
        service.tb_transition_task(up.task_id, "in_progress")

        bus.fail_next = 1
        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "nack"
        service.tb_update_task(down.task_id, {"title": "Approve the plan"})
        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "acked"
        bus_ids = _ready_ids(bus, down.task_id)
        assert bus_ids == [f"tb:notify:human_todo:{down.task_id}:d1"]

        assert adapter.handle_canonical_event(_complete_event(up.task_id)) == "acked"
        assert set(_ready_ids(bus, down.task_id)) == set(bus_ids)
    finally:
        store.close()


def test_promotion_loses_to_a_blocker_committed_after_the_unblocked_scan(tmp_path: Path) -> None:
    store, bus, service, _ = _build(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Up", initial_status="ready")
        down = service.tb_create_task("proj:hibernaut", "Down")
        late = service.tb_create_task("proj:hibernaut", "Late blocker", initial_status="ready")
        service.tb_manage_dependency("add", up.task_id, down.task_id)
        service.tb_transition_task(up.task_id, "in_progress")
        # Commit the terminal state without the fan-out, as if the dispatcher had only just started.
        current = store.get_task(up.task_id)
        assert current is not None
        store.upsert_task(replace(current, status="done"), expected_version=current.version)

        candidates = store.list_unblocked_downstream(up.task_id)
        assert [c.task_id for c in candidates] == [down.task_id]
        # Another thread adds an incomplete blocker between the scan and the promotion.
        store.add_dependency(late.task_id, down.task_id)

        assert store.try_promote_to_ready(
            down.task_id, expected_version=candidates[0].version, ready_at="2026-11-23T00:00:00.000000Z"
        ) is None
        assert store.get_task(down.task_id).status == "backlog"  # type: ignore[union-attr]

        # The late blocker's completion is what unlocks it.
        bus.published.clear()
        service.tb_transition_task(late.task_id, "in_progress")
        service.tb_transition_task(late.task_id, "done")
        assert store.get_task(down.task_id).status == "ready"  # type: ignore[union-attr]
        assert _ready_ids(bus, down.task_id) == [f"tb:{down.task_id}:d1"]
    finally:
        store.close()


def test_each_cancelled_upstream_gets_its_own_notification_id(tmp_path: Path) -> None:
    store, bus, service, _ = _build(tmp_path)
    try:
        a = service.tb_create_task("proj:hibernaut", "A")
        b = service.tb_create_task("proj:hibernaut", "B")
        down = service.tb_create_task("proj:hibernaut", "Down")
        service.tb_manage_dependency("add", a.task_id, down.task_id)
        service.tb_manage_dependency("add", b.task_id, down.task_id)
        bus.published.clear()

        service.tb_transition_task(a.task_id, "cancelled")
        service.tb_transition_task(b.task_id, "cancelled")
        ids = [
            ev.external_id
            for ev in bus.published
            if ev.metadata.get("notify_kind") == "upstream_cancelled"
        ]
        assert len(ids) == 2
        assert len(set(ids)) == 2
    finally:
        store.close()
