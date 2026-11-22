from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.core.errors import (
    CycleDetectedError,
    OptimisticLockError,
    TaskNotFoundError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.models import NotifyKind, TaskItem, new_task_id
from taskboard.engine.ingestion import IngestionService


class _RecordingPublisher:
    def __init__(self) -> None:
        self.work_ready: list[tuple[str, int]] = []
        self.notifications: list[tuple[NotifyKind, str, str]] = []
        self._lock = threading.Lock()

    def publish_work_ready(
        self,
        task: TaskItem,
        *,
        trace_id: str,
        hop_count: int,
    ) -> None:
        del trace_id, hop_count
        with self._lock:
            self.work_ready.append((task.task_id, task.dispatch_seq))

    def publish_notification(
        self,
        *,
        notify_kind: NotifyKind,
        task: TaskItem,
        reason: str,
        trace_id: str,
        hop_count: int,
        target_assignee: str | None = None,
        dedupe_key: str | None = None,
    ) -> None:
        del trace_id, hop_count, target_assignee, dedupe_key
        with self._lock:
            self.notifications.append((notify_kind, task.task_id, reason))


def test_wal_mode_and_partial_unique_source_ref(tmp_path: Path) -> None:
    db_path = tmp_path / "taskboard.db"
    store = SQLiteTaskStore(db_path)
    try:
        conn = sqlite3.connect(db_path)
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        conn.close()
        assert str(mode).lower() == "wal"

        # Multiple tasks with source_ref=None are allowed by partial unique index.
        t1 = store.create_task(TaskItem(task_id=new_task_id(), title="Ad-hoc 1", source_ref=None))
        t2 = store.create_task(TaskItem(task_id=new_task_id(), title="Ad-hoc 2", source_ref=None))
        assert t1.task_id != t2.task_id

        # Duplicate non-empty source_ref violates uq_tasks_source_ref.
        store.create_task(
            TaskItem(task_id=new_task_id(), title="GH 1", source_ref="github://org/repo/issues/1"),
            source="github",
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.create_task(
                TaskItem(
                    task_id=new_task_id(),
                    title="GH 1 dup",
                    source_ref="github://org/repo/issues/1",
                ),
                source="github",
            )
    finally:
        store.close()


def test_1_to_n_source_links_merge(tmp_path: Path) -> None:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    try:
        task = store.create_task(
            TaskItem(
                task_id=new_task_id(),
                title="Cross-source task",
                source_ref="github://org/repo/issues/42",
            ),
            source="github",
        )
        store.link_source(
            task.task_id,
            "feishu://task/abc-123",
            "feishu",
            is_primary=False,
            last_synced_at=12345,
        )

        found_gh = store.find_by_source_ref("github://org/repo/issues/42")
        found_fs = store.find_by_source_ref("feishu://task/abc-123")
        assert found_gh is not None and found_gh.task_id == task.task_id
        assert found_fs is not None and found_fs.task_id == task.task_id

        links = store.list_source_links(task.task_id)
        assert len(links) == 2
        assert links[0].is_primary is True
        assert links[0].source == "github"
        assert links[1].source == "feishu"

        # Promote feishu link to primary.
        store.link_source(task.task_id, "feishu://task/abc-123", "feishu", is_primary=True)
        refreshed = store.get_task(task.task_id)
        assert refreshed is not None and refreshed.source_ref == "feishu://task/abc-123"
    finally:
        store.close()


def test_dependency_strict_existence_cycle_and_related_edges(tmp_path: Path) -> None:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    try:
        a = store.create_task(TaskItem(task_id=new_task_id(), title="A"))
        b = store.create_task(TaskItem(task_id=new_task_id(), title="B"))
        c = store.create_task(TaskItem(task_id=new_task_id(), title="C"))

        with pytest.raises(UpstreamTaskNotFoundError):
            store.add_dependency("tb_tsk_missing", b.task_id)
        with pytest.raises(TaskNotFoundError):
            store.add_dependency(a.task_id, "tb_tsk_missing")

        store.add_dependency(a.task_id, b.task_id, kind="blocks")
        store.add_dependency(b.task_id, c.task_id, kind="blocks")
        with pytest.raises(CycleDetectedError) as exc:
            store.add_dependency(c.task_id, a.task_id, kind="blocks")
        assert exc.value.cycle_path == (c.task_id, a.task_id, b.task_id, c.task_id)

        # Related edges allow bidirectional links and do not block.
        store.add_dependency(a.task_id, c.task_id, kind="related")
        store.add_dependency(c.task_id, a.task_id, kind="related")
        assert store.count_incomplete_blockers(a.task_id) == 0
    finally:
        store.close()


def test_concurrent_dual_upstream_done_unlocks_at_most_once(tmp_path: Path) -> None:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    pub = _RecordingPublisher()
    svc = IngestionService(store, publisher=pub)
    try:
        up1 = svc.tb_create_task("proj:hibernaut", "Up 1", initial_status="ready")
        up2 = svc.tb_create_task("proj:hibernaut", "Up 2", initial_status="ready")
        down = svc.tb_create_task("proj:hibernaut", "Downstream", initial_status="backlog")
        svc.tb_manage_dependency("add", up1.task_id, down.task_id)
        svc.tb_manage_dependency("add", up2.task_id, down.task_id)

        svc.tb_transition_task(up1.task_id, "in_progress")
        svc.tb_transition_task(up2.task_id, "in_progress")
        pub.work_ready.clear()

        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def _complete(tid: str) -> None:
            try:
                barrier.wait()
                svc.tb_transition_task(tid, "done")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=_complete, args=(up1.task_id,))
        t2 = threading.Thread(target=_complete, args=(up2.task_id,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == []
        final_down = store.get_task(down.task_id)
        assert final_down is not None
        assert final_down.status == "ready"
        assert final_down.dispatch_seq == 1
        assert pub.work_ready == [(down.task_id, 1)]
    finally:
        store.close()


def test_remove_dependency_unlocks_immediately_and_cas_conflict(tmp_path: Path) -> None:
    store = SQLiteTaskStore(tmp_path / "taskboard.db")
    pub = _RecordingPublisher()
    svc = IngestionService(store, publisher=pub)
    try:
        up = svc.tb_create_task("proj:hibernaut", "Upstream", initial_status="backlog")
        down = svc.tb_create_task("proj:hibernaut", "Downstream", initial_status="ready")
        pub.work_ready.clear()

        # Adding an incomplete blocker demotes ready downstream back to backlog.
        svc.tb_manage_dependency("add", up.task_id, down.task_id)
        demoted = store.get_task(down.task_id)
        assert demoted is not None and demoted.status == "backlog"

        # Removing the blocker immediately unlocks and dispatches downstream.
        removed = svc.tb_manage_dependency("remove", up.task_id, down.task_id)
        assert removed is True
        unlocked = store.get_task(down.task_id)
        assert unlocked is not None and unlocked.status == "ready"
        assert len(pub.work_ready) == 1
        assert pub.work_ready[0][0] == down.task_id

        # Optimistic lock conflict on stale version.
        with pytest.raises(OptimisticLockError):
            store.upsert_task(replace(unlocked, title="Stale"), expected_version=unlocked.version - 1)
    finally:
        store.close()
