import asyncio
import json
import os
import time
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.run_state import STATE_FILE
from agent_runtime.app.main import AsyncWorkerPool, drain_pools, startup_inboxes
from agent_runtime.core.models import Emit, RunDetached
from agent_runtime.engine.worker import ExecutorFailed
from tests.support.resume_runs import INBOX, ParkingExecutor, RuntimeCase


class TestDetachAndResume(RuntimeCase):
    async def test_a_parked_run_is_detached_kept_registered_and_resumed_once_by_the_next_process(self):
        parker = ParkingExecutor()
        evt = self.append()
        first = self.worker("old", {"parker": parker})
        running = asyncio.create_task(first.process_inbox(INBOX))
        await asyncio.wait_for(parker.parked.wait(), 5)

        first.detach()
        reports = await asyncio.wait_for(running, 5)

        # The old process returns at once: nothing written, the events stay 'processing' under the registration,
        # and the run directory with the executor's state stays.
        self.assertEqual(reports, [])
        self.assertEqual(self.status(evt), "processing")
        [open_run] = self.runs.list_open(INBOX)
        self.assertEqual((open_run.executor_id, open_run.event_ids, open_run.code_version),
                         ("parker", (evt.event_id,), "9.9.9"))
        self.assertTrue(Path(open_run.run_dir, STATE_FILE).exists())
        self.assertEqual(self.messages(), [])
        self.assertIsNone(self.db.fetchone("SELECT 1 FROM worker_leases WHERE inbox_id = ?", (INBOX,)))
        self.assertIn(INBOX, startup_inboxes(self.inbox, self.runs)())

        # A new process resumes it (no second start), then the result, events and registration are settled.
        second = self.worker("new", {"parker": ParkingExecutor()})
        reports = await second.process_inbox(INBOX)
        self.assertEqual(parker.calls, ["start"])
        [report] = reports
        self.assertEqual(report.outcome.messages, ("resumed from waiting (1 event(s))",))
        self.assertEqual(self.status(evt), "done")
        self.assertEqual(self.runs.list_open(INBOX), [])
        self.assertEqual(self.run_dirs(), [])
        self.assertEqual(self.messages(), ["resumed from waiting (1 event(s))"])

    async def test_a_run_that_finishes_normally_leaves_no_directory_or_registration(self):
        parker = ParkingExecutor()
        evt = self.append()
        running = asyncio.create_task(self.worker("w", {"parker": parker}).process_inbox(INBOX))
        await asyncio.wait_for(parker.parked.wait(), 5)
        self.assertEqual(len(self.run_dirs()), 1)
        parker.release.set()
        [report] = await asyncio.wait_for(running, 5)
        self.assertEqual(report.outcome.messages, ("done by start (1 event(s))",))
        self.assertEqual(self.status(evt), "done")
        self.assertEqual(self.runs.list_open(INBOX), [])
        self.assertEqual(self.run_dirs(), [])

    async def test_a_stop_waits_for_a_run_that_is_not_parked(self):
        parker = ParkingExecutor(park=False)
        evt = self.append()
        worker = self.worker("w", {"parker": parker})
        running = asyncio.create_task(worker.process_inbox(INBOX))
        await asyncio.wait_for(parker.parked.wait(), 5)
        worker.detach()
        await asyncio.sleep(0.1)
        self.assertFalse(running.done())
        parker.release.set()
        [report] = await asyncio.wait_for(running, 5)
        self.assertIsInstance(report.outcome, Emit)
        self.assertEqual(self.status(evt), "done")
        self.assertEqual(self.runs.list_open(INBOX), [])

    async def test_an_executor_that_detaches_at_a_checkpoint_is_resumed(self):
        class Checkpointing(ParkingExecutor):
            async def execute_resumable(self, events, session, caps, run):
                Path(run.run_dir, STATE_FILE).write_text(json.dumps({"step": "checkpoint"}))
                self.parked.set()
                while not run.should_detach():
                    await asyncio.sleep(0.01)
                raise RunDetached("stopped at a checkpoint")

        executor = Checkpointing()
        evt = self.append()
        worker = self.worker("w", {"parker": executor})
        running = asyncio.create_task(worker.process_inbox(INBOX))
        await asyncio.wait_for(executor.parked.wait(), 5)
        worker.detach()
        self.assertEqual(await asyncio.wait_for(running, 5), [])
        self.assertEqual(self.status(evt), "processing")
        [report] = await self.worker("new", {"parker": Checkpointing()}).process_inbox(INBOX)
        self.assertEqual(report.outcome.messages, ("resumed from checkpoint (1 event(s))",))

    async def test_new_events_wait_for_the_resumed_run_and_orphans_are_requeued(self):
        parker = ParkingExecutor()
        evt = self.append()
        first = self.worker("old", {"parker": parker})
        running = asyncio.create_task(first.process_inbox(INBOX))
        await asyncio.wait_for(parker.parked.wait(), 5)
        first.detach()
        await asyncio.wait_for(running, 5)
        later = self.append("second task")

        follower = ParkingExecutor()
        follower.release.set()
        reports = await self.worker("new", {"parker": follower}).process_inbox(INBOX)
        self.assertEqual([r.outcome.messages[0] for r in reports],
                         ["resumed from waiting (1 event(s))", "done by start (1 event(s))"])
        self.assertEqual((self.status(evt), self.status(later)), ("done", "done"))

    async def test_an_unregistered_executor_requeues_the_run_and_tells_a_person(self):
        parker = ParkingExecutor()
        evt = self.append()
        first = self.worker("old", {"parker": parker})
        running = asyncio.create_task(first.process_inbox(INBOX))
        await asyncio.wait_for(parker.parked.wait(), 5)
        first.detach()
        await asyncio.wait_for(running, 5)
        [open_run] = self.runs.list_open(INBOX)

        other = ParkingExecutor()
        other.executor_id = "other"
        other.release.set()
        worker = self.worker("new", {"other": other})
        with self.assertRaisesRegex(ExecutorFailed, "'parker' is not registered"):
            # Sticky routing still names the missing executor, so the batch stays pending for a person.
            await worker.process_inbox(INBOX)
        self.assertEqual(self.status(evt), "pending")
        self.assertEqual(self.runs.list_open(INBOX), [])
        self.assertFalse(Path(open_run.run_dir).exists())
        [notice] = self.messages()
        self.assertIn("executor 'parker' is no longer registered", notice)

    async def test_a_failed_run_drops_its_registration(self):
        class Failing(ParkingExecutor):
            async def execute_resumable(self, events, session, caps, run):
                raise RuntimeError("boom")

        evt = self.append()
        with self.assertRaises(RuntimeError):
            await self.worker("w", {"parker": Failing()}).process_inbox(INBOX)
        self.assertEqual(self.status(evt), "failed")
        self.assertEqual(self.runs.list_open(INBOX), [])
        self.assertEqual(self.run_dirs(), [])

    async def test_pool_drain_detaches_parked_runs_and_the_stop_returns(self):
        parker = ParkingExecutor()
        evt = self.append()
        pool = AsyncWorkerPool(worker_factory=lambda inbox_id, wid: self.worker(wid, {"parker": parker}))
        stop = asyncio.Event()
        runs = asyncio.create_task(pool.run_forever(stop))
        pool.wake(INBOX)
        await asyncio.wait_for(parker.parked.wait(), 5)
        await asyncio.wait_for(drain_pools([pool], [runs], None), 5)
        stop.set()
        self.assertEqual(self.status(evt), "processing")
        self.assertEqual(len(self.runs.list_open(INBOX)), 1)


class TestOpenRunStore(RuntimeCase):
    def test_finish_marks_events_done_and_drops_the_registration_together(self):
        evt = self.append()
        self.inbox.claim_pending(INBOX)
        run = self.runs.open(session_id="sess/1", inbox_id=INBOX, executor_id="sde", event_ids=[evt.event_id],
                             code_version="1.0.0")
        path = Path(run.run_dir)
        self.assertEqual(path.parent.parent, self.root / "runs")
        self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        self.runs.add_events("sess/1", ["evt_folded"])
        self.assertEqual(self.runs.list_open(INBOX)[0].event_ids, (evt.event_id, "evt_folded"))
        self.runs.finish("sess/1", [evt.event_id])
        self.assertEqual(self.status(evt), "done")
        self.assertEqual(self.runs.list_open(INBOX), [])
        self.assertFalse(path.exists())

    def test_sweep_removes_only_old_orphans_and_never_leaves_the_root(self):
        kept = self.runs.open(session_id="live", inbox_id=INBOX, executor_id="sde", event_ids=[], code_version="")
        orphan_old = self.root / "runs" / "gone" / "run_old"
        orphan_new = self.root / "runs" / "gone" / "run_new"
        for path in (orphan_old, orphan_new):
            path.mkdir(parents=True)
        past = time.time() - 3600
        os.utime(orphan_old, (past, past))
        os.utime(kept.run_dir, (past, past))
        removed = self.runs.sweep(retention_seconds=60)
        self.assertEqual(removed, [str(orphan_old)])
        self.assertTrue(Path(kept.run_dir).exists())
        self.assertTrue(orphan_new.exists())
        outside = self.root / "elsewhere"
        outside.mkdir()
        with self.db.transaction() as conn:
            conn.execute("UPDATE open_runs SET run_dir = ? WHERE session_id = 'live'", (str(outside),))
        self.runs.discard("live")
        self.assertTrue(outside.exists())


if __name__ == "__main__":
    unittest.main()
