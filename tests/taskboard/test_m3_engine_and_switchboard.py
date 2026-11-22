from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from switchboard.envelope import CanonicalEvent, validate_envelope
from switchboard.ports import ConsumerHandler, IngestResult
from taskboard.adapters.audit_adapter import RecordingAuditSink, SafeAuditAdapter
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.adapters.switchboard_adapter import SwitchboardAdapter
from taskboard.core.errors import (
    IllegalTransitionError,
    MaxHopsExceededError,
    SchemaValidationError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.models import FlowPlan, FlowStepSpec
from taskboard.engine.ingestion import IngestionService


@dataclass
class _FakeEventBus:
    published: list[CanonicalEvent] = field(default_factory=lambda: [])
    subscribers: dict[str, ConsumerHandler] = field(default_factory=lambda: {})

    def publish(self, event: CanonicalEvent) -> IngestResult:
        validate_envelope(event)
        self.published.append(event)
        return IngestResult(status="accepted", event_id=event.event_id)

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        self.subscribers[consumer_id] = handler


def _build_stack(tmp_path: Path) -> tuple[SQLiteTaskStore, _FakeEventBus, RecordingAuditSink, IngestionService, SwitchboardAdapter]:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    bus = _FakeEventBus()
    audit = RecordingAuditSink()
    safe_audit = SafeAuditAdapter(audit)
    adapter = SwitchboardAdapter(bus, audit=safe_audit)
    service = IngestionService(store, publisher=adapter, audit=safe_audit)
    adapter.bind_ingestion(service)
    adapter.attach("taskboard")
    return store, bus, audit, service, adapter


def test_work_opened_initial_status_and_idempotency_key(tmp_path: Path) -> None:
    store, bus, _, _, adapter = _build_stack(tmp_path)
    try:
        # Agent task with initial_status=ready emits work.ready immediately.
        ev_agent = CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="gh:issue:101",
            scope="proj:hibernaut",
            correlation_key="github://org/hibernaut/issues/101",
            subject="Implement Taskboard",
            body="Details",
            metadata={
                "work_kind": "issue",
                "state": "open",
                "trace_id": "trc_101",
                "hop_count": 0,
                "initial_status": "ready",
                "assignee": "agent:coder",
                "task_type": "coding",
                "priority": 1,
            },
        )
        task_agent = adapter.ingest_event(ev_agent)
        assert task_agent is not None
        assert len(bus.published) == 1
        ready_ev = bus.published[0]
        assert ready_ev.event_type == "work.ready"
        assert ready_ev.source == "taskboard"
        assert ready_ev.external_id == f"tb:{ready_ev.metadata['task_id']}:d1"
        assert ready_ev.metadata["dispatch_seq"] == 1
        assert ready_ev.metadata["assignee"] == "agent:coder"
        assert ready_ev.metadata["hop_count"] == 1

        # Human task with initial_status=ready emits execution.reported(human_todo), never work.ready.
        ev_human = CanonicalEvent(
            source="feishu",
            event_type="work.opened",
            external_id="fs:task:202",
            scope="proj:hibernaut",
            correlation_key="feishu://task/202",
            subject="Approve Architecture",
            metadata={
                "work_kind": "task",
                "state": "open",
                "trace_id": "trc_202",
                "hop_count": 0,
                "initial_status": "ready",
                "assignee": "human:kun",
                "priority": 0,
            },
        )
        adapter.ingest_event(ev_human)
        assert len(bus.published) == 2
        notify_ev = bus.published[1]
        assert notify_ev.event_type == "execution.reported"
        assert notify_ev.source == "taskboard"
        assert notify_ev.metadata["outcome_kind"] == "emit"
        assert notify_ev.metadata["notify_kind"] == "human_todo"
        assert notify_ev.metadata["target_assignee"] == "human:kun"

        # Missing depends_on strictly rejected without creating ghost task.
        ev_bad_dep = CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="gh:issue:103",
            scope="proj:hibernaut",
            correlation_key="github://org/hibernaut/issues/103",
            subject="Depends on ghost",
            metadata={
                "work_kind": "issue",
                "state": "open",
                "trace_id": "trc_103",
                "hop_count": 0,
                "depends_on": ["github://org/hibernaut/issues/999"],
            },
        )
        with pytest.raises(UpstreamTaskNotFoundError):
            adapter.ingest_event(ev_bad_dep)
        assert store.find_by_source_ref("github://org/hibernaut/issues/103") is None
    finally:
        store.close()


def test_execution_reported_actions_and_priority_ordering(tmp_path: Path) -> None:
    store, bus, _, service, adapter = _build_stack(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Upstream", initial_status="ready")
        low_prio = service.tb_create_task(
            "proj:hibernaut", "Low Priority", initial_status="backlog", priority=3
        )
        high_prio = service.tb_create_task(
            "proj:hibernaut", "High Priority", initial_status="backlog", priority=0
        )
        service.tb_manage_dependency("add", up.task_id, low_prio.task_id)
        service.tb_manage_dependency("add", up.task_id, high_prio.task_id)
        bus.published.clear()

        def _exec_ev(action: str, **extra: Any) -> CanonicalEvent:
            meta: dict[str, Any] = {
                "outcome_kind": "emit",
                "run_id": "run_1",
                "session_id": "sess_1",
                "task_id": up.task_id,
                "task_action": action,
                "trace_id": "trc_exec",
                "hop_count": 1,
                **extra,
            }
            return CanonicalEvent(
                source="hibernaut",
                event_type="execution.reported",
                external_id=f"exec:{action}:{extra.get('stage', '')}",
                scope="proj:hibernaut",
                correlation_key=up.task_id,
                subject=f"action {action}",
                metadata=meta,
            )

        # claim -> release -> claim -> update_stage -> complete
        adapter.ingest_event(_exec_ev("claim", assignee="agent:sde"))
        assert store.get_task(up.task_id).status == "in_progress"  # type: ignore[union-attr]

        adapter.ingest_event(_exec_ev("release"))
        assert store.get_task(up.task_id).status == "ready"  # type: ignore[union-attr]

        adapter.ingest_event(_exec_ev("claim", assignee="agent:sde"))
        adapter.ingest_event(_exec_ev("update_stage", stage="testing", artifact_ref="file://report.log"))
        mid = store.get_task(up.task_id)
        assert mid is not None and mid.stage == "testing" and mid.artifact_ref == "file://report.log"

        adapter.ingest_event(_exec_ev("complete"))
        assert store.get_task(up.task_id).status == "done"  # type: ignore[union-attr]

        # Downstream dispatched in (priority ASC, ready_at ASC) order: high_prio (0) before low_prio (3).
        assert [ev.metadata["task_id"] for ev in bus.published] == [high_prio.task_id, low_prio.task_id]

        # Terminal status rejects late update_stage.
        with pytest.raises(IllegalTransitionError):
            adapter.ingest_event(_exec_ev("update_stage", stage="late_stage"))
    finally:
        store.close()


def test_dag_anomaly_notifications_cancel_and_reopen(tmp_path: Path) -> None:
    store, bus, _, service, _ = _build_stack(tmp_path)
    try:
        up = service.tb_create_task("proj:hibernaut", "Up", initial_status="ready")
        down = service.tb_create_task("proj:hibernaut", "Down", initial_status="backlog")
        service.tb_manage_dependency("add", up.task_id, down.task_id)
        bus.published.clear()

        # Upstream cancelled: downstream stays backlog, gets blocked_reason & upstream_cancelled alert.
        service.tb_transition_task(up.task_id, "cancelled", reason="no longer needed")
        d = store.get_task(down.task_id)
        assert d is not None
        assert d.status == "backlog"
        assert d.blocked_reason == "upstream_cancelled"
        assert len(bus.published) == 1
        assert bus.published[0].event_type == "execution.reported"
        assert bus.published[0].metadata["notify_kind"] == "upstream_cancelled"

        # Now test reopen of a done upstream affecting both ready and in_progress downstreams.
        u2 = service.tb_create_task("proj:hibernaut", "Up2", initial_status="ready")
        d_ready = service.tb_create_task("proj:hibernaut", "DownReady", initial_status="backlog")
        d_running = service.tb_create_task("proj:hibernaut", "DownRunning", initial_status="backlog")
        service.tb_manage_dependency("add", u2.task_id, d_ready.task_id)
        service.tb_manage_dependency("add", u2.task_id, d_running.task_id)

        service.tb_transition_task(u2.task_id, "in_progress")
        service.tb_transition_task(u2.task_id, "done")
        service.tb_transition_task(d_running.task_id, "in_progress")
        bus.published.clear()

        service.tb_transition_task(u2.task_id, "in_progress", reason="rework", reopen=True)
        assert store.get_task(d_ready.task_id).status == "backlog"  # type: ignore[union-attr]
        assert store.get_task(d_running.task_id).status == "in_progress"  # type: ignore[union-attr]
        assert any(
            ev.event_type == "execution.reported"
            and ev.metadata.get("notify_kind") == "dependency_invalidated"
            and ev.metadata.get("task_id") == d_running.task_id
            for ev in bus.published
        )
    finally:
        store.close()


def test_hop_count_guard_and_tb_tools(tmp_path: Path) -> None:
    store, bus, audit, service, adapter = _build_stack(tmp_path)
    try:
        # Inbound hop_count >= 8 rejected & audited.
        ev_loop = CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="gh:loop:1",
            scope="proj:hibernaut",
            correlation_key="github://org/repo/issues/9999",
            subject="Looping event",
            metadata={
                "work_kind": "issue",
                "state": "open",
                "trace_id": "trc_loop",
                "hop_count": 8,
            },
        )
        with pytest.raises(MaxHopsExceededError):
            adapter.ingest_event(ev_loop)
        assert any(r["event_name"] == "max_hops_exceeded" for r in audit.records)

        # handle_canonical_event catches MaxHopsExceededError / SchemaValidationError and acks safely.
        assert adapter.handle_canonical_event(ev_loop) == "acked"

        ev_bad_schema = CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="gh:bad:1",
            scope="proj:hibernaut",
            correlation_key="github://org/repo/issues/8888",
            subject="Empty trace_id",
            # A missing trace_id gets a default; an explicit empty one is still a schema violation.
            metadata={"work_kind": "issue", "state": "open", "trace_id": ""},
        )
        with pytest.raises(SchemaValidationError):
            adapter.ingest_event(ev_bad_schema)
        assert adapter.handle_canonical_event(ev_bad_schema) == "acked"

        # Flow instantiation + tb_list_tasks derived incomplete_blocker_count.
        bus.published.clear()
        flow = service.tb_instantiate_flow(
            FlowPlan(
                title="Release Flow",
                scope="proj:hibernaut",
                initial_status="ready",
                steps=(
                    FlowStepSpec(id="build", title="Build", assignee="agent:builder"),
                    FlowStepSpec(id="verify", title="Verify", assignee="human:kun"),
                ),
            )
        )
        assert len(flow.tasks) == 2
        assert len(flow.edges) == 1
        assert len(bus.published) == 1
        assert bus.published[0].event_type == "work.ready"

        views = service.tb_list_tasks(parent_task_id=flow.parent_task_id)
        by_title = {v.task.title: v for v in views}
        assert by_title["Build"].incomplete_blocker_count == 0
        assert by_title["Verify"].incomplete_blocker_count == 1

        dispatchable = service.tb_list_tasks(
            parent_task_id=flow.parent_task_id,
            dispatchable_only=True,
        )
        assert [v.task.title for v in dispatchable] == ["Build"]
    finally:
        store.close()


def test_audit_adapter_silent_degradation() -> None:
    class _BrokenAudit:
        def emit(self, event_name: str, *, task_id: str | None = None, detail: Any = None) -> None:
            del event_name, task_id, detail
            raise RuntimeError("disk full")

    SafeAuditAdapter(None).emit("noop")
    SafeAuditAdapter(_BrokenAudit()).emit("broken", task_id="tb_tsk_1")
