from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.memory_fs import FileSystemMemoryAdapter, WorkspacePartitionRegistry
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.models import Event, SessionRef
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemorySessionStore,
    InMemoryTimerStore,
)


class InboxContractMixin:
    def make_inbox(self) -> Any:
        raise NotImplementedError

    def test_append_dedup_fetch_and_mark_done(self) -> None:
        inbox = self.make_inbox()
        e1 = Event(inbox_id="u1", source="webhook", external_id="ext-1", payload={"v": 1})
        stored1, dup1 = inbox.append(e1)
        self.assertFalse(dup1)
        self.assertEqual(stored1.status, "pending")

        e1_dup = Event(inbox_id="u1", source="webhook", external_id="ext-1", payload={"v": 1})
        stored_dup, dup2 = inbox.append(e1_dup)
        self.assertTrue(dup2)
        self.assertEqual(stored_dup.status, "dropped")

        e2 = Event(inbox_id="u1", source="webhook", external_id="ext-2", payload={"v": 2})
        stored2, dup3 = inbox.append(e2)
        self.assertFalse(dup3)

        pending = inbox.fetch_pending("u1")
        self.assertEqual([e.event_id for e in pending], [stored1.event_id, stored2.event_id])

        inbox.mark_done([stored1.event_id])
        remaining = inbox.fetch_pending("u1")
        self.assertEqual([e.event_id for e in remaining], [stored2.event_id])

    def test_has_open_event_is_true_only_while_pending_or_processing(self) -> None:
        inbox = self.make_inbox()
        self.assertFalse(inbox.has_open_event("u3", "drive", "f:1"))
        stored, _ = inbox.append(Event(inbox_id="u3", source="drive", external_id="f:1", payload={}))
        self.assertTrue(inbox.has_open_event("u3", "drive", "f:1"))
        self.assertFalse(inbox.has_open_event("other", "drive", "f:1"))
        self.assertFalse(inbox.has_open_event("u3", "other", "f:1"))
        inbox.claim_pending("u3")
        self.assertTrue(inbox.has_open_event("u3", "drive", "f:1"))
        inbox.mark_done([stored.event_id])
        self.assertFalse(inbox.has_open_event("u3", "drive", "f:1"))
        # A dropped duplicate is never open, whatever its row says.
        inbox.append(Event(inbox_id="u3", source="drive", external_id="f:1", payload={}))
        self.assertFalse(inbox.has_open_event("u3", "drive", "f:1"))

        failed, _ = inbox.append(Event(inbox_id="u3", source="drive", external_id="f:2", payload={}))
        inbox.claim_pending("u3")
        inbox.mark_failed([failed.event_id], "boom")
        self.assertFalse(inbox.has_open_event("u3", "drive", "f:2"))

    def test_claim_bind_requeue_and_mark_failed(self) -> None:
        inbox = self.make_inbox()
        e1, _ = inbox.append(Event(inbox_id="u2", source="im", external_id="c1", payload={}))
        e2, _ = inbox.append(Event(inbox_id="u2", source="im", external_id="c2", payload={}))

        claimed = inbox.claim_pending("u2")
        self.assertEqual([e.event_id for e in claimed], [e1.event_id, e2.event_id])
        self.assertTrue(all(e.status == "processing" for e in claimed))
        self.assertFalse(hasattr(claimed[0], "attempts"))
        self.assertEqual(inbox.diagnostics([e1.event_id])[e1.event_id].attempts, 1)
        self.assertEqual(inbox.fetch_pending("u2"), [])
        self.assertEqual(len(inbox.fetch_processing("u2")), 2)

        inbox.bind_session([e1.event_id], "sess_bound")
        proc_after_bind = {e.event_id: e for e in inbox.fetch_processing("u2")}
        self.assertEqual(proc_after_bind[e1.event_id].session_id, "sess_bound")

        inbox.requeue_processing([e1.event_id])
        self.assertEqual([e.event_id for e in inbox.fetch_pending("u2")], [e1.event_id])

        inbox.mark_failed([e2.event_id], "boom")
        self.assertEqual(inbox.fetch_processing("u2"), [])
        self.assertEqual(inbox.diagnostics([e2.event_id])[e2.event_id].last_error, "boom")

        # Claim count is storage-internal and keeps growing across re-claims (poison-event stop-loss input).
        inbox.claim_pending("u2")
        self.assertEqual(inbox.diagnostics([e1.event_id])[e1.event_id].attempts, 2)
        self.assertEqual(inbox.diagnostics(["missing"]), {})


class TimerContractMixin:
    def make_timers(self) -> Any:
        raise NotImplementedError

    def test_follow_up_and_recurring_schedule(self) -> None:
        timers = self.make_timers()
        t1 = timers.schedule(
            "follow_up",
            "2026-11-21T10:00:00+00:00",
            {"task": "check_pr"},
            inbox_id="u1",
        )
        t2 = timers.schedule(
            "schedule",
            "2026-11-21T10:00:00+00:00",
            {"task": "daily_digest"},
            recurrence_seconds=3600,
            inbox_id="u1",
        )
        timers.schedule(
            "follow_up",
            "2026-11-21T12:00:00+00:00",
            {"task": "future"},
            inbox_id="u1",
        )

        due = timers.fetch_due("2026-11-21T10:05:00+00:00")
        self.assertEqual({t.timer_id for t in due}, {t1, t2})

        timers.mark_triggered(t1, next_due_at=None)
        timers.mark_triggered(t2, next_due_at="2026-11-21T11:00:00+00:00")

        self.assertEqual(timers.fetch_due("2026-11-21T10:30:00+00:00"), [])
        next_due = timers.fetch_due("2026-11-21T11:00:00+00:00")
        self.assertEqual([t.timer_id for t in next_due], [t2])


class MemoryContractMixin:
    def make_memory(self) -> Any:
        raise NotImplementedError

    def test_baseline_and_on_demand_read_write(self) -> None:
        memory = self.make_memory()
        baseline = memory.load_baseline()
        self.assertEqual(set(baseline.keys()), {"instructions.md", "index.md", "reported.md"})

        memory.write("instructions.md", "Be concise.")
        memory.write("notes/user_prefs.md", "Prefers UTC+8")
        memory.write("progress/task_1.md", "Step 1 done")

        self.assertEqual(memory.load_baseline()["instructions.md"], "Be concise.")
        self.assertEqual(memory.read_on_demand("notes/user_prefs.md"), "Prefers UTC+8")
        self.assertEqual(memory.read_on_demand("progress/task_1.md"), "Step 1 done")

        with self.assertRaises(FileNotFoundError):
            memory.read_on_demand("notes/nonexistent.md")

        with self.assertRaises(ValueError):
            memory.write("../escape.txt", "bad")


class OutboxContractMixin:
    def make_outbox(self) -> Any:
        raise NotImplementedError

    def test_enqueue_retry_and_mark_sent(self) -> None:
        outbox = self.make_outbox()
        m1 = outbox.enqueue("u1", "hello", {"kind": "emit"})
        m2 = outbox.enqueue("u1", "world", {"kind": "emit"})

        pending = outbox.fetch_pending(limit=10)
        self.assertEqual([m.message_id for m in pending], [m1.message_id, m2.message_id])

        outbox.schedule_retry(m1.message_id, "timeout", "2026-11-22T08:00:05.000000+00:00")
        pending_after_fail = outbox.fetch_pending(limit=10, now_iso="2026-11-22T08:00:05.000000+00:00")
        self.assertEqual(pending_after_fail[0].attempts, 1)
        self.assertEqual(pending_after_fail[0].next_retry_at, "2026-11-22T08:00:05.000000+00:00")

        outbox.mark_sent(m1.message_id)
        remaining = outbox.fetch_pending(limit=10)
        self.assertEqual([m.message_id for m in remaining], [m2.message_id])

    def test_backoff_hides_a_message_until_next_retry_at(self) -> None:
        outbox = self.make_outbox()
        m1 = outbox.enqueue("u1", "first")
        m2 = outbox.enqueue("u1", "second")
        self.assertIsNone(m1.next_retry_at)

        outbox.schedule_retry(m1.message_id, "connection refused", "2026-11-22T08:00:05.000000+00:00")
        before = outbox.fetch_pending(limit=10, now_iso="2026-11-22T08:00:04.999999+00:00")
        self.assertEqual([m.message_id for m in before], [m2.message_id])

        due = outbox.fetch_pending(limit=10, now_iso="2026-11-22T08:00:05.000000+00:00")
        self.assertEqual([m.message_id for m in due], [m1.message_id, m2.message_id])
        self.assertEqual((due[0].status, due[0].attempts), ("pending", 1))

        outbox.schedule_retry(m1.message_id, "again", "2026-11-22T08:00:20.000000+00:00")
        (again,) = [m for m in outbox.fetch_pending(limit=10, now_iso="2026-11-22T09:00:00.000000+00:00")
                    if m.message_id == m1.message_id]
        self.assertEqual((again.attempts, again.next_retry_at), (2, "2026-11-22T08:00:20.000000+00:00"))

    def test_mark_failed_is_terminal(self) -> None:
        outbox = self.make_outbox()
        m1 = outbox.enqueue("u1", "doomed")
        outbox.schedule_retry(m1.message_id, "first", "2026-11-22T08:00:05.000000+00:00")
        outbox.mark_failed(m1.message_id, "exhausted")
        self.assertEqual(outbox.fetch_pending(limit=10, now_iso="2999-01-01T00:00:00.000000+00:00"), [])
        self.assertEqual(self.outbox_row(outbox, m1.message_id), ("failed", 2, "exhausted"))

    def outbox_row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        raise NotImplementedError


class LockContractMixin:
    def make_lock_and_clock(self) -> tuple[Any, list[float]]:
        raise NotImplementedError

    def test_single_active_worker_and_lease_expiry_takeover(self) -> None:
        lock, now_box = self.make_lock_and_clock()

        self.assertTrue(lock.acquire("u1", "w1", ttl_seconds=10.0))
        self.assertFalse(lock.acquire("u1", "w2", ttl_seconds=10.0))

        now_box[0] += 5.0
        self.assertTrue(lock.renew("u1", "w1", ttl_seconds=10.0))
        self.assertFalse(lock.acquire("u1", "w2", ttl_seconds=10.0))

        now_box[0] += 11.0
        self.assertTrue(lock.acquire("u1", "w2", ttl_seconds=10.0))

        lock.release("u1", "w2")
        self.assertTrue(lock.acquire("u1", "w1", ttl_seconds=10.0))


class SessionContractMixin:
    def make_sessions(self) -> Any:
        raise NotImplementedError

    def test_parent_session_id_persisted_and_filtered(self) -> None:
        store = self.make_sessions()
        store.save(SessionRef(session_id="main", inbox_id="proj:hibernaut", work_key="wk", executor_id="claude"))
        store.save(
            SessionRef(
                session_id="child",
                inbox_id="proj:hibernaut",
                work_key="wk",
                executor_id="grok",
                parent_session_id="main",
            )
        )
        store.save(SessionRef(session_id="other", inbox_id="proj:hibernaut", parent_session_id="elsewhere"))

        child = store.get("child")
        assert child is not None
        self.assertEqual(child.parent_session_id, "main")
        main = store.get("main")
        assert main is not None
        self.assertIsNone(main.parent_session_id)

        self.assertEqual([s.session_id for s in store.list_sessions(parent_session_id="main")], ["child"])
        self.assertEqual(store.list_sessions(parent_session_id="main", status="closed"), [])
        self.assertEqual(len(store.list_sessions(inbox_id="proj:hibernaut")), 3)
        # The child counts toward the shared worktree reference until it closes.
        self.assertEqual(store.count_active_by_work_key("wk"), 2)
        store.close_session("main")
        self.assertEqual(store.count_active_by_work_key("wk"), 1)
        store.close_session("child")
        self.assertEqual(store.count_active_by_work_key("wk"), 0)
        closed_child = store.get("child")
        assert closed_child is not None
        self.assertEqual(closed_child.parent_session_id, "main")

    def test_save_get_and_find_by_request_id(self) -> None:
        store = self.make_sessions()
        s = SessionRef(
            session_id="sess_1",
            inbox_id="u1",
            checkpoint='{"step": 1}',
            pending_request_id="req_42",
        )
        store.save(s)

        loaded = store.get("sess_1")
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.checkpoint, '{"step": 1}')

        by_req = store.find_by_request_id("req_42")
        self.assertIsNotNone(by_req)
        assert by_req is not None
        self.assertEqual(by_req.session_id, "sess_1")

    def test_find_by_correlation_key_and_session_rotation(self) -> None:
        store = self.make_sessions()
        key = "github:kun-g/hibernaut#42"

        store.save(SessionRef(session_id="sess_old", inbox_id="proj:hibernaut", correlation_key=key))
        found = store.find_by_correlation_key("proj:hibernaut", key)
        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found.session_id, "sess_old")
        self.assertIsNone(store.find_by_correlation_key("proj:other", key))

        # Rotating to a fresh session_id rebinds the correlation_key cleanly.
        store.save(SessionRef(session_id="sess_new", inbox_id="proj:hibernaut", correlation_key=key))
        rotated = store.find_by_correlation_key("proj:hibernaut", key)
        self.assertIsNotNone(rotated)
        assert rotated is not None
        self.assertEqual(rotated.session_id, "sess_new")

        # Old session remains accessible by its own session_id with correlation_key unbound.
        old_loaded = store.get("sess_old")
        self.assertIsNotNone(old_loaded)
        assert old_loaded is not None
        self.assertIsNone(old_loaded.correlation_key)

    def test_executor_binding_persisted_and_updated(self) -> None:
        store = self.make_sessions()
        ref = SessionRef(
            session_id="sess_exec",
            inbox_id="proj:hibernaut",
            correlation_key="github:kun-g/hibernaut#99",
            executor_id="claude",
            executor_session_id="claude-native-123",
        )
        store.save(ref)
        loaded = store.get("sess_exec")
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.executor_id, "claude")
        self.assertEqual(loaded.executor_session_id, "claude-native-123")

        by_corr = store.find_by_correlation_key("proj:hibernaut", "github:kun-g/hibernaut#99")
        self.assertIsNotNone(by_corr)
        assert by_corr is not None
        self.assertEqual(by_corr.executor_id, "claude")
        self.assertEqual(by_corr.executor_session_id, "claude-native-123")

        # Switching executor clears/replaces the prior native session id.
        store.save(
            SessionRef(
                session_id="sess_exec",
                inbox_id="proj:hibernaut",
                correlation_key="github:kun-g/hibernaut#99",
                executor_id="cursor-agent",
                executor_session_id=None,
            )
        )
        switched = store.get("sess_exec")
        self.assertIsNotNone(switched)
        assert switched is not None
        self.assertEqual(switched.executor_id, "cursor-agent")
        self.assertIsNone(switched.executor_session_id)

    def test_session_lifecycle_archive_close_and_work_key_ref_count(self) -> None:
        store = self.make_sessions()
        wk = "github:kun-g/hibernaut#209"
        store.save(
            SessionRef(
                session_id="s_active",
                inbox_id="proj:hibernaut",
                correlation_key=wk,
                work_key=wk,
                status="active",
            )
        )
        store.save(
            SessionRef(
                session_id="s_susp",
                inbox_id="proj:hibernaut",
                work_key=wk,
                status="suspended",
                pending_request_id="req_209",
            )
        )
        self.assertEqual(store.count_active_by_work_key(wk), 2)

        store.archive_session("s_active")
        self.assertEqual(store.count_active_by_work_key(wk), 1)
        self.assertIsNone(store.find_by_correlation_key("proj:hibernaut", wk))
        archived = store.get("s_active")
        assert archived is not None
        self.assertEqual(archived.status, "archived")

        store.close_session("s_susp")
        self.assertEqual(store.count_active_by_work_key(wk), 0)
        closed = store.get("s_susp")
        assert closed is not None
        self.assertEqual(closed.status, "closed")


class TestInMemoryContracts(
    InboxContractMixin,
    TimerContractMixin,
    MemoryContractMixin,
    OutboxContractMixin,
    LockContractMixin,
    SessionContractMixin,
    unittest.TestCase,
):
    def make_inbox(self) -> Any:
        return InMemoryInbox()

    def make_timers(self) -> Any:
        return InMemoryTimerStore()

    def make_memory(self) -> Any:
        return InMemoryMemoryStore()

    def make_outbox(self) -> Any:
        return InMemoryOutbox()

    def outbox_row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        msg = outbox.get(message_id)
        return msg.status, msg.attempts, outbox.last_errors.get(message_id)

    def make_lock_and_clock(self) -> tuple[Any, list[float]]:
        now_box = [1000.0]
        return InMemoryLock(clock=lambda: now_box[0]), now_box

    def make_sessions(self) -> Any:
        return InMemorySessionStore()


class TestSQLiteAndFileSystemContracts(
    InboxContractMixin,
    TimerContractMixin,
    MemoryContractMixin,
    OutboxContractMixin,
    LockContractMixin,
    SessionContractMixin,
    unittest.TestCase,
):
    def setUp(self) -> None:
        self._db = SQLiteDatabase(":memory:")
        self._tmp = tempfile.TemporaryDirectory(prefix="mem_contract_")

    def tearDown(self) -> None:
        self._db.close()
        self._tmp.cleanup()

    def make_inbox(self) -> Any:
        return SQLiteInboxAdapter(self._db)

    def make_timers(self) -> Any:
        return SQLiteTimerAdapter(self._db)

    def make_memory(self) -> Any:
        return FileSystemMemoryAdapter(Path(self._tmp.name))

    def make_outbox(self) -> Any:
        return SQLiteOutboxAdapter(self._db)

    def outbox_row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        row = self._db.fetchone("SELECT status, attempts, last_error FROM outbox WHERE message_id = ?", (message_id,))
        assert row is not None
        return str(row["status"]), int(row["attempts"]), row["last_error"]

    def make_lock_and_clock(self) -> tuple[Any, list[float]]:
        now_box = [1000.0]
        return SQLiteLeaseLockAdapter(self._db, clock=lambda: now_box[0]), now_box

    def make_sessions(self) -> Any:
        return SQLiteSessionStoreAdapter(self._db)


class TestPartitionedMemoryContract(MemoryContractMixin, unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="part_mem_contract_")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def make_memory(self) -> Any:
        registry = WorkspacePartitionRegistry(Path(self._tmp.name))
        return registry.memory_for("proj:contract-test")


if __name__ == "__main__":
    unittest.main()
