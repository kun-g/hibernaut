from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_runtime.adapters.executor_cli.worktree import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteWorkspaceStore,
)
from agent_runtime.core.models import Event

INBOX = "proj:concurrency"


def _event(external_id: str) -> Event:
    return Event(inbox_id=INBOX, source="stress", external_id=external_id, payload={"k": external_id})


class TestSQLiteThreadSafety(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="sqlite_conc_")
        self.db_path = Path(self._tmp.name) / "conc.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _databases(self) -> list[tuple[str, SQLiteDatabase]]:
        return [("memory", SQLiteDatabase(":memory:")), ("file", SQLiteDatabase(self.db_path))]

    def test_concurrent_append_dedups_exactly_once_per_key(self) -> None:
        for label, db in self._databases():
            with self.subTest(db=label):
                inbox = SQLiteInboxAdapter(db)
                keys = [f"ext-{idx % 50}" for idx in range(400)]
                with ThreadPoolExecutor(max_workers=32) as pool:
                    results = list(pool.map(lambda key: inbox.append(_event(key)), keys))

                fresh = [stored.external_id for stored, dup in results if not dup]
                self.assertEqual(sorted(fresh), sorted({*keys}))
                self.assertEqual(len(inbox.fetch_pending(INBOX)), 50)
                db.close()

    def test_mixed_readers_and_writers_do_not_interfere(self) -> None:
        db = SQLiteDatabase(self.db_path)
        inbox = SQLiteInboxAdapter(db)
        outbox = SQLiteOutboxAdapter(db)
        errors: list[BaseException] = []

        def writer(worker: int) -> None:
            try:
                for idx in range(50):
                    stored, _ = inbox.append(_event(f"w{worker}-{idx}"))
                    outbox.enqueue(INBOX, f"msg-{worker}-{idx}")
                    if idx % 2 == 0:
                        inbox.mark_done([stored.event_id])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def reader() -> None:
            try:
                for _ in range(100):
                    inbox.fetch_pending(INBOX)
                    outbox.fetch_pending(limit=10)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(w,)) for w in range(8)]
        threads += [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(inbox.fetch_pending(INBOX)), 8 * 25)
        self.assertEqual(len(outbox.fetch_pending(limit=1000)), 8 * 50)
        db.close()

    def test_other_thread_cannot_commit_an_open_transaction(self) -> None:
        db = SQLiteDatabase(self.db_path)
        inbox = SQLiteInboxAdapter(db)
        inserted = threading.Event()
        other_done = threading.Event()

        def other_writer() -> None:
            inbox.append(_event("other"))
            other_done.set()

        other = threading.Thread(target=other_writer)
        with self.assertRaises(RuntimeError):
            with db.transaction() as conn:
                conn.execute(
                    "INSERT INTO worker_leases (inbox_id, worker_id, expires_at) VALUES ('x', 'w', 0)"
                )
                inserted.set()
                other.start()
                # A shared implicit transaction would let the other thread's commit publish this row.
                self.assertFalse(other_done.wait(timeout=0.2))
                raise RuntimeError("abort")
        other.join(timeout=5)

        self.assertTrue(other_done.is_set())
        self.assertIsNone(db.fetchone("SELECT 1 FROM worker_leases WHERE inbox_id = 'x'"))
        self.assertEqual([e.external_id for e in inbox.fetch_pending(INBOX)], ["other"])
        db.close()

    def test_lease_acquire_has_single_winner_across_connections(self) -> None:
        # Separate SQLiteDatabase objects stand in for separate processes sharing the file.
        dbs = [SQLiteDatabase(self.db_path) for _ in range(4)]
        locks = [SQLiteLeaseLockAdapter(db) for db in dbs]
        barrier = threading.Barrier(16)

        def contend(idx: int) -> bool:
            barrier.wait()
            return locks[idx % 4].acquire(INBOX, f"worker-{idx}", 60.0)

        with ThreadPoolExecutor(max_workers=16) as pool:
            wins = list(pool.map(contend, range(16)))

        self.assertEqual(wins.count(True), 1)
        for db in dbs:
            db.close()

    def test_append_dedups_across_connections(self) -> None:
        dbs = [SQLiteDatabase(self.db_path) for _ in range(4)]
        inboxes = [SQLiteInboxAdapter(db) for db in dbs]
        keys = [f"ext-{idx % 20}" for idx in range(200)]

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(
                pool.map(lambda pair: inboxes[pair[0] % 4].append(_event(pair[1])), enumerate(keys))
            )

        self.assertEqual(sum(1 for _, dup in results if not dup), 20)
        self.assertEqual(len(inboxes[0].fetch_pending(INBOX)), 20)
        for db in dbs:
            db.close()

    def test_nested_transaction_commits_once_and_rolls_back_together(self) -> None:
        db = SQLiteDatabase(":memory:")
        with self.assertRaises(ValueError):
            with db.transaction() as outer:
                outer.execute("INSERT INTO worker_leases VALUES ('a', 'w', 0)")
                with db.transaction() as inner:
                    inner.execute("INSERT INTO worker_leases VALUES ('b', 'w', 0)")
                raise ValueError
        self.assertEqual(db.fetchall("SELECT inbox_id FROM worker_leases"), [])

        with db.transaction() as outer:
            outer.execute("INSERT INTO worker_leases VALUES ('a', 'w', 0)")
            with db.transaction() as inner:
                inner.execute("INSERT INTO worker_leases VALUES ('b', 'w', 0)")
        rows = db.fetchall("SELECT inbox_id FROM worker_leases ORDER BY inbox_id")
        self.assertEqual([r["inbox_id"] for r in rows], ["a", "b"])
        db.close()


    def test_lease_ttl_starts_after_write_lock_is_acquired(self) -> None:
        db = SQLiteDatabase(self.db_path)
        advanced = threading.Event()
        lease = SQLiteLeaseLockAdapter(db, clock=lambda: 100.0 + (2.0 if advanced.is_set() else 0.0))
        blocker = sqlite3.connect(self.db_path)
        blocker.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(lease.acquire, INBOX, "A", 1.0)
            # Time passes while A waits on another process's write lock.
            advanced.set()
            blocker.commit()
            self.assertTrue(first.result(timeout=5))
        self.assertFalse(lease.acquire(INBOX, "B", 1.0))
        blocker.close()
        db.close()

    def test_mark_done_splits_ids_beyond_variable_limit(self) -> None:
        db = SQLiteDatabase(":memory:")
        inbox = SQLiteInboxAdapter(db)
        ids = [inbox.append(_event(f"e{idx}"))[0].event_id for idx in range(10)]
        with db.transaction() as conn:
            conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 3)
        inbox.mark_done(ids)
        self.assertEqual(inbox.fetch_pending(INBOX), [])
        db.close()

    def test_concurrent_reclaim_removes_worktree_once(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        folder = self.db_path.parent / "hb-fixture"
        folder.mkdir()
        store.reserve_creating(work_key="x", repo_checkout=str(folder.parent), workdir=str(folder), branch="b")
        store.mark_ready("x", workdir=str(folder), branch="b")
        both_past_gates = threading.Barrier(2, timeout=5)
        removals: list[tuple[str, ...]] = []
        waited: set[int] = set()

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            # Both callers pass the first gates together; the winner's recheck after its claim runs alone.
            if list(argv[:2]) == ["gh", "pr"] and threading.get_ident() not in waited:
                waited.add(threading.get_ident())
                both_past_gates.wait()
            if "remove" in argv:
                removals.append(tuple(argv))
            return CommandResult(0, "", "")

        manager = OrcaWorktreeManager(workspace_store=store, backend="git", runner=runner)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(manager.reclaim, ["x", "x"]))

        self.assertEqual(len(removals), 1)
        self.assertEqual(sorted(r.reclaimed for r in results), [False, True])
        self.assertIsNone(store.get("x"))
        db.close()


    def _ready_workspace(self, store: SQLiteWorkspaceStore) -> Path:
        folder = self.db_path.parent / "hb-fixture"
        folder.mkdir(exist_ok=True)
        store.reserve_creating(work_key="x", repo_checkout=str(folder.parent), workdir=str(folder), branch="b")
        store.mark_ready("x", workdir=str(folder), branch="b")
        return folder

    def test_reclaim_started_during_another_removal_is_refused(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        self._ready_workspace(store)
        in_remove = threading.Event()
        finish_remove = threading.Event()
        removals: list[tuple[str, ...]] = []

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            if "remove" in argv:
                removals.append(tuple(argv))
                in_remove.set()
                finish_remove.wait(timeout=5)
            return CommandResult(0, "", "")

        manager = OrcaWorktreeManager(workspace_store=store, backend="git", runner=runner)
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(manager.reclaim, "x")
            self.assertTrue(in_remove.wait(timeout=5))
            second = manager.reclaim("x")
            finish_remove.set()
            self.assertTrue(first.result(timeout=5).reclaimed)

        self.assertFalse(second.reclaimed)
        self.assertEqual(len(removals), 1)
        db.close()

    def test_slow_removal_keeps_claim_fresh_against_takeover(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        self._ready_workspace(store)
        in_remove = threading.Event()
        finish_remove = threading.Event()
        removals: list[tuple[str, ...]] = []

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            if "remove" in argv:
                removals.append(tuple(argv))
                in_remove.set()
                finish_remove.wait(timeout=5)
            return CommandResult(0, "", "")

        manager = OrcaWorktreeManager(
            workspace_store=store,
            backend="git",
            runner=runner,
            reclaim_stale_after_seconds=0.3,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(manager.reclaim, "x")
            self.assertTrue(in_remove.wait(timeout=5))
            threading.Event().wait(0.6)  # past the stale window; heartbeat must have renewed
            second = manager.reclaim("x")
            finish_remove.set()
            self.assertTrue(first.result(timeout=5).reclaimed)

        self.assertFalse(second.reclaimed)
        self.assertEqual(len(removals), 1)
        db.close()

    def test_heartbeat_survives_a_transient_lock_on_renewal(self) -> None:
        db = SQLiteDatabase(self.db_path, busy_timeout_ms=20)
        store = SQLiteWorkspaceStore(db)
        self._ready_workspace(store)
        # Stands in for another process; used from the remove and heartbeat threads.
        blocker = sqlite3.connect(self.db_path, check_same_thread=False)
        renew_errors: list[BaseException] = []
        real_renew = store.renew_reclaim

        def spy_renew(work_key: str, *, claim: str) -> str | None:
            try:
                return real_renew(work_key, claim=claim)
            except BaseException as exc:
                renew_errors.append(exc)
                if len(renew_errors) == 1:
                    blocker.commit()  # the lock is held only across the first renewal
                raise

        store.renew_reclaim = spy_renew  # type: ignore[method-assign]
        in_remove = threading.Event()
        finish_remove = threading.Event()
        removals: list[tuple[str, ...]] = []

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            if "remove" in argv:
                removals.append(tuple(argv))
                if len(removals) == 1:
                    blocker.execute("BEGIN IMMEDIATE")
                    in_remove.set()
                    finish_remove.wait(timeout=5)
            return CommandResult(0, "", "")

        manager = OrcaWorktreeManager(
            workspace_store=store,
            backend="git",
            runner=runner,
            reclaim_stale_after_seconds=0.45,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(manager.reclaim, "x")
            self.assertTrue(in_remove.wait(timeout=5))
            threading.Event().wait(0.9)  # two stale windows after the failed renewal
            second = manager.reclaim("x")
            finish_remove.set()
            self.assertTrue(first.result(timeout=5).reclaimed)

        self.assertEqual(len(renew_errors), 1)
        self.assertFalse(second.reclaimed)
        self.assertEqual(len(removals), 1)
        blocker.close()
        db.close()

    def test_reclaim_that_lost_its_claim_keeps_the_new_record(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        folder = self._ready_workspace(store)

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            if "remove" in argv:
                # A takeover finished and the key was registered again meanwhile.
                store.delete("x")
                store.reserve_creating(
                    work_key="x", repo_checkout=str(folder.parent), workdir=str(folder), branch="b2"
                )
            return CommandResult(0, "", "")

        manager = OrcaWorktreeManager(workspace_store=store, backend="git", runner=runner)
        result = manager.reclaim("x")

        self.assertFalse(result.reclaimed)
        record = store.get("x")
        assert record is not None
        self.assertEqual(record.branch, "b2")
        db.close()

    def test_stale_reclaim_claim_can_be_taken_over(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        self._ready_workspace(store)
        with db.transaction() as conn:
            conn.execute(
                "UPDATE workspaces SET status = 'reclaiming', updated_at = ? WHERE work_key = 'x'",
                ("2000-01-01T00:00:00+00:00",),
            )
        manager = OrcaWorktreeManager(
            workspace_store=store,
            backend="git",
            runner=lambda argv, cwd: CommandResult(0, "", ""),
        )
        self.assertTrue(manager.reclaim("x").reclaimed)
        self.assertIsNone(store.get("x"))
        db.close()

    def test_failed_removal_releases_claim_for_retry(self) -> None:
        db = SQLiteDatabase(self.db_path)
        store = SQLiteWorkspaceStore(db)
        self._ready_workspace(store)
        outcomes = iter([CommandResult(1, "", "busy"), CommandResult(0, "", "")])

        def runner(argv: Sequence[str], cwd: Path | None) -> CommandResult:
            _ = cwd
            return next(outcomes) if "remove" in argv else CommandResult(0, "", "")

        manager = OrcaWorktreeManager(workspace_store=store, backend="git", runner=runner)
        self.assertFalse(manager.reclaim("x").reclaimed)
        record = store.get("x")
        assert record is not None
        self.assertEqual(record.status, "ready")
        self.assertTrue(manager.reclaim("x").reclaimed)
        db.close()


if __name__ == "__main__":
    unittest.main()
