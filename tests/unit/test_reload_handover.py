import asyncio
import unittest

from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.models import Emit
from agent_runtime.engine import AgentWorker, EventHandler
from tests.support.fakes import InMemoryMemoryStore, InMemoryPermission


class _SlowExecutor:
    executor_id = "slow"

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.started = asyncio.Event()

    async def execute(self, events, session, caps):
        self.started.set()
        await self.release.wait()
        return Emit(messages=("finished",))


class TestLeaseHeldThroughLongRun(unittest.IsolatedAsyncioTestCase):
    async def test_a_run_longer_than_the_lease_ttl_keeps_its_lease(self):
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        inbox = SQLiteInboxAdapter(db)
        # The lease's clock moves only here, so a stall of the machine cannot expire it between two renewals.
        now = 1000.0
        lock = SQLiteLeaseLockAdapter(db, clock=lambda: now)
        executor = _SlowExecutor()
        worker = AgentWorker(
            worker_id="old-pool-worker-0", inbox=inbox, timers=SQLiteTimerAdapter(db), memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db), lock=lock,
            sessions=SQLiteSessionStoreAdapter(db), executor=executor, lease_ttl_seconds=0.15,
        )
        EventHandler(inbox=inbox).ingest(inbox_id="i", source="t", external_id="e1", payload={"text": "x"})

        def renewed_at(moment: float) -> bool:
            [row] = db.fetchall("SELECT expires_at FROM worker_leases")
            return float(row["expires_at"]) == moment + 0.15

        run = asyncio.create_task(worker.process_inbox("i"))
        await asyncio.wait_for(executor.started.wait(), timeout=30)
        for _ in range(3):
            now += 0.1
            async with asyncio.timeout(30):
                while not renewed_at(now):
                    await asyncio.sleep(0.01)
            self.assertFalse(lock.acquire("i", "new-pool-worker-0", 0.15))
        executor.release.set()
        reports = await run
        self.assertEqual(len(reports), 1)
        self.assertTrue(lock.acquire("i", "new-pool-worker-0", 0.15))


if __name__ == "__main__":
    unittest.main()
