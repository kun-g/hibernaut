from __future__ import annotations

import json
from collections.abc import Callable, Collection, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from switchboard.envelope import CanonicalEvent, validate_envelope
from switchboard.inbound.github import GitHubInboundAdapter
from switchboard.ports import ConsumerHandler, IngestResult
from switchboard.router import ScopeResolver
from taskboard.adapters.audit_adapter import RecordingAuditSink, SafeAuditAdapter
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.adapters.switchboard_adapter import SwitchboardAdapter
from taskboard.core.errors import SchemaValidationError, TaskNotFoundError
from taskboard.engine.ingestion import IngestionService
from tests.support.switchboard_support import StaticCredentials, github_headers, issue_payload, make_config

SCOPE = "proj:hibernaut"


@dataclass
class _Bus:
    published: list[CanonicalEvent] = field(default_factory=lambda: [])
    # The next N publishes fail, as when the hub's /mcp is unreachable.
    failures: int = 0

    def publish(self, event: CanonicalEvent) -> IngestResult:
        validate_envelope(event)
        if self.failures:
            self.failures -= 1
            raise ConnectionError("switchboard /mcp unreachable")
        self.published.append(event)
        return IngestResult(status="accepted", event_id=event.event_id)

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        del consumer_id, handler


@dataclass
class _Stack:
    store: SQLiteTaskStore
    bus: _Bus
    audit: RecordingAuditSink
    service: IngestionService
    adapter: SwitchboardAdapter


@pytest.fixture
def make_stack(tmp_path: Path) -> Iterator[Callable[..., _Stack]]:
    stores: list[SQLiteTaskStore] = []

    def build(auto_ready_sources: Collection[str] = ()) -> _Stack:
        store = SQLiteTaskStore(tmp_path / f"taskboard-{len(stores)}.db")
        stores.append(store)
        bus = _Bus()
        audit = RecordingAuditSink()
        adapter = SwitchboardAdapter(bus, audit=SafeAuditAdapter(audit), auto_ready_sources=auto_ready_sources)
        service = IngestionService(store, publisher=adapter, audit=SafeAuditAdapter(audit))
        adapter.bind_ingestion(service)
        return _Stack(store, bus, audit, service, adapter)

    yield build
    for store in stores:
        store.close()


def github_event(action: str = "opened", *, event: str = "issues", number: int = 42, **item: Any) -> CanonicalEvent:
    # The real Switchboard GitHub adapter produces the event, so the contract is checked against its output.
    payload = issue_payload(action, number=number)
    if event == "pull_request":
        payload["pull_request"] = payload.pop("issue")
    payload["issue" if event == "issues" else "pull_request"].update(item)
    adapter = GitHubInboundAdapter(StaticCredentials(), ScopeResolver(make_config("unused").routing))
    raw = json.dumps(payload).encode("utf-8")
    parsed = adapter.handle(github_headers(raw, event, "d-1"), raw)
    assert parsed.event is not None, parsed.response_body
    return parsed.event


def test_github_pull_request_work_opened_is_accepted(make_stack: Callable[..., _Stack]) -> None:
    stack = make_stack()
    event = github_event(event="pull_request", number=7)
    assert event.metadata["work_kind"] == "pull_request"
    assert "trace_id" not in event.metadata and "hop_count" not in event.metadata

    task = stack.adapter.ingest_event(event)
    assert task is not None
    created = stack.store.find_by_source_ref("github:kun-g/hibernaut#7")
    assert created is not None and created.status == "backlog"
    assert stack.adapter.handle_canonical_event(event) == "acked"


def test_missing_trace_fields_get_defaults_but_bad_values_are_rejected(make_stack: Callable[..., _Stack]) -> None:
    stack = make_stack(auto_ready_sources=["github"])
    event = github_event()
    stack.adapter.ingest_event(event)
    [ready] = stack.bus.published
    assert ready.metadata["trace_id"] == event.event_id
    assert ready.metadata["hop_count"] == 1
    # The adapter worked on a copy: the delivered event is unchanged.
    assert "trace_id" not in event.metadata

    for bad in ({"trace_id": ""}, {"trace_id": 5}, {"hop_count": "0"}, {"hop_count": -1}, {"hop_count": True}):
        metadata = {**event.metadata, **bad}
        with pytest.raises(SchemaValidationError):
            stack.adapter.ingest_event(
                CanonicalEvent(
                    source="github",
                    event_type="work.opened",
                    external_id=f"bad:{bad}",
                    scope=SCOPE,
                    correlation_key="github:kun-g/hibernaut#100",
                    subject="bad",
                    metadata=metadata,
                )
            )

    report = CanonicalEvent(
        source="hibernaut",
        event_type="execution.reported",
        external_id="outbox:1",
        scope=SCOPE,
        correlation_key="github:kun-g/hibernaut#42",
        subject="done",
        metadata={"run_id": "r", "session_id": "s", "outcome_kind": "emit"},
    )
    assert stack.adapter.ingest_event(report) is None
    with pytest.raises(SchemaValidationError):
        stack.adapter.ingest_event(
            CanonicalEvent(
                source="hibernaut",
                event_type="execution.reported",
                external_id="outbox:2",
                scope=SCOPE,
                correlation_key="k",
                subject="done",
                metadata={"run_id": "r", "session_id": "s", "outcome_kind": "emit", "hop_count": "1"},
            )
        )


def test_auto_ready_sources_decide_the_default_initial_status(make_stack: Callable[..., _Stack]) -> None:
    stack = make_stack(auto_ready_sources=["github"])
    stack.adapter.ingest_event(github_event(number=1))
    gh = stack.store.find_by_source_ref("github:kun-g/hibernaut#1")
    assert gh is not None and gh.status == "ready" and gh.dispatch_seq == 1
    assert [e.event_type for e in stack.bus.published] == ["work.ready"]

    explicit = github_event(number=2)
    stack.adapter.ingest_event(
        CanonicalEvent(
            source="github",
            event_type="work.opened",
            external_id="explicit",
            scope=SCOPE,
            correlation_key="github:kun-g/hibernaut#2",
            subject="stay in backlog",
            metadata={**explicit.metadata, "initial_status": "backlog"},
        )
    )
    held = stack.store.find_by_source_ref("github:kun-g/hibernaut#2")
    assert held is not None and held.status == "backlog"

    stack.adapter.ingest_event(
        CanonicalEvent(
            source="mcp",
            event_type="work.opened",
            external_id="mcp:1",
            scope=SCOPE,
            correlation_key="mcp:1",
            subject="from mcp",
            metadata={"work_kind": "task", "state": "open"},
        )
    )
    other = stack.store.find_by_source_ref("mcp:1")
    assert other is not None and other.status == "backlog"
    assert len(stack.bus.published) == 1

    # Without auto_ready_sources even GitHub stays in backlog (the previous behaviour).
    plain = make_stack()
    plain.adapter.ingest_event(github_event(number=3))
    kept = plain.store.find_by_source_ref("github:kun-g/hibernaut#3")
    assert kept is not None and kept.status == "backlog"


def test_labeled_old_issue_work_updated_creates_the_task_once(make_stack: Callable[..., _Stack]) -> None:
    stack = make_stack(auto_ready_sources=["github"])
    labeled = github_event("labeled", number=9, labels=[{"name": "hibernaut"}])
    assert labeled.event_type == "work.updated"

    task = stack.adapter.ingest_event(labeled)
    assert task is not None
    created = stack.store.find_by_source_ref("github:kun-g/hibernaut#9")
    assert created is not None and created.status == "ready" and created.title == "Switchboard phase 1"
    [ready] = stack.bus.published
    assert (ready.event_type, ready.metadata["task_id"]) == ("work.ready", created.task_id)

    # Redelivery and a later edit update the same task; nothing is created or newly dispatched. Updates
    # of a ready task replay the same work.ready id, which Switchboard dedupes on (source, external_id).
    assert stack.adapter.handle_canonical_event(labeled) == "acked"
    edited = github_event("edited", number=9, title="Renamed", updated_at="2026-11-22T05:00:00Z")
    assert stack.adapter.handle_canonical_event(edited) == "acked"
    tasks = stack.service.tb_list_tasks()
    assert [v.task.title for v in tasks] == ["Renamed"]
    assert tasks[0].task.dispatch_seq == 1
    assert {e.external_id for e in stack.bus.published} == {f"tb:{created.task_id}:d1"}


def test_work_updated_for_unknown_task_without_auto_ready_source_still_fails(
    make_stack: Callable[..., _Stack],
) -> None:
    stack = make_stack(auto_ready_sources=["github"])
    unknown = CanonicalEvent(
        source="feishu",
        event_type="work.updated",
        external_id="fs:1",
        scope=SCOPE,
        correlation_key="feishu:task:1",
        subject="unknown",
        metadata={"work_kind": "task", "state": "open"},
    )
    with pytest.raises(TaskNotFoundError):
        stack.adapter.ingest_event(unknown)
    assert stack.adapter.handle_canonical_event(unknown) == "nack"
    assert stack.service.tb_list_tasks() == []


def test_updates_and_closes_of_untracked_closed_or_untitled_items_are_acked(
    make_stack: Callable[..., _Stack],
) -> None:
    # Retrying these can never succeed (no work.opened will follow), so they are acked, not dead-lettered.
    stack = make_stack(auto_ready_sources=["github"])
    no_subject = CanonicalEvent(
        source="github",
        event_type="work.updated",
        external_id="gh:untitled",
        scope=SCOPE,
        correlation_key="github:kun-g/hibernaut#77",
        subject="",
        metadata={"work_kind": "issue", "state": "open"},
    )
    # Labelling an already closed issue, or editing a closed issue that carries the label.
    closed_labeled = github_event("labeled", number=78, state="closed")
    closed_edited = github_event("edited", number=79, state="closed")
    closed_issue = github_event("closed", number=80, state="closed")
    assert closed_issue.event_type == "work.closed"
    for event in (no_subject, closed_labeled, closed_edited, closed_issue):
        assert stack.adapter.ingest_event(event) is None, event.external_id
        assert stack.adapter.handle_canonical_event(event) == "acked", event.external_id
    assert stack.service.tb_list_tasks() == []
    assert stack.bus.published == []
    assert any(r["event_name"] == "untracked_work_ignored" for r in stack.audit.records)


def test_labeled_issue_dispatch_lost_to_a_publish_failure_is_replayed_on_redelivery(
    make_stack: Callable[..., _Stack],
) -> None:
    stack = make_stack(auto_ready_sources=["github"])
    labeled = github_event("labeled", number=11)
    stack.bus.failures = 1
    # The task commits and claims dispatch_seq 1, then the publish fails: nack for a redelivery.
    assert stack.adapter.handle_canonical_event(labeled) == "nack"
    task = stack.store.find_by_source_ref("github:kun-g/hibernaut#11")
    assert task is not None and task.status == "ready" and task.dispatch_seq == 1
    assert stack.bus.published == []

    # The redelivery finds the task and must still publish its work.ready, under the same external_id.
    assert stack.adapter.handle_canonical_event(labeled) == "acked"
    [ready] = stack.bus.published
    assert (ready.event_type, ready.external_id) == ("work.ready", f"tb:{task.task_id}:d1")
    after = stack.store.find_by_source_ref("github:kun-g/hibernaut#11")
    assert after is not None and after.dispatch_seq == 1

    # A later update republishes the same id (Switchboard dedupes it) and never claims a new dispatch.
    edited = github_event("edited", number=11, title="Renamed", updated_at="2026-11-22T05:00:00Z")
    assert stack.adapter.handle_canonical_event(edited) == "acked"
    assert {e.external_id for e in stack.bus.published} == {f"tb:{task.task_id}:d1"}
    final = stack.store.find_by_source_ref("github:kun-g/hibernaut#11")
    assert final is not None and final.dispatch_seq == 1 and final.title == "Renamed"
