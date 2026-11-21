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

        outbox.mark_failed(m1.message_id, "timeout")
        pending_after_fail = outbox.fetch_pending(limit=10)
        self.assertEqual(pending_after_fail[0].attempts, 1)

        outbox.mark_sent(m1.message_id)
        remaining = outbox.fetch_pending(limit=10)
        self.assertEqual([m.message_id for m in remaining], [m2.message_id])


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
