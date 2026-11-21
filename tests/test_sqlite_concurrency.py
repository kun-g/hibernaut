from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
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


if __name__ == "__main__":
    unittest.main()
