import asyncio
import contextlib
import json
import os
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import Mock

from tests.fakes import InMemoryPermission
from tests.test_cli_reviewer import PASS, TerminalCase
from tests.test_coder_reviewer import git
from tests.test_peer_handoff import RECEIVER, FakeClaude, Incoming, PeerCase, result
from tests.test_pr_delivery import DeliveryCase
from tests.test_sde_executor import make_base_repo

from agent_runtime.adapters.delivery import DeliveryDetached
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.run_state import STATE_FILE, TurnSlot, read_state
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA, STATE_FORMAT, SDEExecutor, SDEOptions
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOpenRunStore,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.main import AsyncWorkerPool, drain_pools, startup_inboxes
from agent_runtime.core.models import Emit, Event, RunDetached, RunOutcome, SessionRef
from agent_runtime.core.ports import RunCapabilities, RunContext
from agent_runtime.engine import AgentWorker

INBOX = "proj:x"


async def until(check, timeout: float = 45.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while True:
        value = await asyncio.to_thread(check)
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


class FakeRun:
    # RunContext as the worker gives it: stop() is a process stop, which cancels the Run only while it is parked.
    def __init__(self, run_dir: Path) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        self._run_dir = str(run_dir)
        self.task: asyncio.Task | None = None
        self.parked = 0
        self.stopping = False
        self._detached = False

    @property
    def run_dir(self) -> str:
        return self._run_dir

    @contextlib.contextmanager
    def detachable(self):
        self.parked += 1
        try:
            if self.stopping:
                self._cancel()
            yield
        finally:
            self.parked -= 1

    def stop(self) -> None:
        self.stopping = True
        if self.parked:
            self._cancel()

    def _cancel(self) -> None:
        self._detached = True
        assert self.task is not None
        self.task.cancel()

    def should_detach(self) -> bool:
        if self.stopping:
            self._detached = True
        return self.stopping

    def detached(self) -> bool:
        return self._detached


class Recorder:
    # What an executor persists through a TurnSlot: the last recorded turn survives the "restart".
    def __init__(self) -> None:
        self.turn: dict | None = None

    def __call__(self, turn: dict | None) -> None:
        self.turn = turn


# --- the runtime: registry, detach and resume with a stand-in executor ---


class ParkingExecutor:
    # Parks every Run on an asyncio.Event inside run.detachable(), as an executor waiting on an Orca terminal does.
    executor_id = "parker"

    def __init__(self, *, park: bool = True) -> None:
        self.release = asyncio.Event()
        self.parked = asyncio.Event()
        self.park = park
        self.calls: list[str] = []

    async def execute(self, events, session, caps) -> RunOutcome:
        raise AssertionError("a resumable executor gets execute_resumable when a registry is configured")

    async def execute_resumable(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities,
                                run: RunContext) -> RunOutcome:
        self.calls.append("start")
        Path(run.run_dir, STATE_FILE).write_text(json.dumps({"step": "waiting"}))
        if self.park:
            with run.detachable():
                self.parked.set()
                await self.release.wait()
        else:
            self.parked.set()
            await self.release.wait()
        return Emit(messages=(f"done by start ({len(events)} event(s))",))

    async def resume(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities,
                     run: RunContext) -> RunOutcome:
        self.calls.append("resume")
        state = json.loads(Path(run.run_dir, STATE_FILE).read_text())
        return Emit(messages=(f"resumed from {state['step']} ({len(events)} event(s))",))


class RuntimeCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.db = SQLiteDatabase(self.root / "hb.db")
        self.addCleanup(self.db.close)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.runs = SQLiteOpenRunStore(self.db, self.root / "runs")

    def worker(self, name: str, executors: dict) -> AgentWorker:
        memory = Mock()
        memory.load_baseline.return_value = {}
        return AgentWorker(worker_id=name, inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=memory,
                           permissions=InMemoryPermission(), outbox=self.outbox, lock=SQLiteLeaseLockAdapter(self.db),
                           sessions=self.sessions, executors=executors, global_default_executor=next(iter(executors)),
                           open_runs=self.runs, code_version="9.9.9")

    def append(self, text: str = "task", executor_id: str = "parker") -> Event:
        stored, _ = self.inbox.append(Event(inbox_id=INBOX, source="test", external_id=f"e-{time.monotonic_ns()}",
                                            payload={"text": text, "executor_id": executor_id},
                                            correlation_key="task:1"))
        return stored

    def status(self, event: Event) -> str:
        return self.db.fetchone("SELECT status FROM event_inbox WHERE event_id = ?", (event.event_id,))["status"]

    def run_dirs(self) -> list[Path]:
        root = self.root / "runs"
        return [p for p in root.glob("*/*")] if root.exists() else []

    def messages(self) -> list[str]:
        return [m.content for m in self.outbox.fetch_pending(limit=50)]


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
        with self.assertRaises(Exception):
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
        with self.assertRaises(Exception):
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


# --- the Orca runner: a peer turn and a CLI review command survive a detach ---


class TestPeerTurnSurvivesDetach(PeerCase):
    async def converse_with(self, runner, slot, *, session_id="sess-1"):
        return await runner.converse(
            self.profile(), title="sde · task:1", workdir=self.workdir, prompt="## Task requirements\nfix divide",
            schema=SDE_SCHEMA, role_args=("--allowedTools", "Read"), is_interrupted=lambda: False,
            session_id=session_id, schema_name="sde-report", inbox_id=INBOX, slot=slot, slot_label="develop",
        )

    async def detach_waiting_turn(self) -> tuple[FakeClaude, Incoming, Recorder, FakeRun]:
        self.orca.scripts.append([None])
        run = FakeRun(self.root / "run")
        recorder = Recorder()
        task = asyncio.ensure_future(self.converse_with(self.runner, TurnSlot(run, None, recorder)))
        run.task = task
        # bool(): the live list can fill after the check returned it empty, and `until` would return the list itself.
        await until(lambda: bool(self.orca.terminals) and bool(self.orca.session().received), what="the task")
        session = self.orca.session()
        await until(lambda: recorder.turn and recorder.turn["sent"] and run.parked, what="the parked wait")
        run.stop()
        with self.assertRaises(asyncio.CancelledError):
            await task
        [msg] = session.received
        return session, msg, recorder, run

    def restarted_runner(self) -> OrcaVisibleRunner:
        db = SQLiteDatabase(self.db_path)
        self.addCleanup(db.close)
        return self.make_runner(db)

    async def test_the_terminal_stays_and_the_next_process_waits_for_the_same_attempt(self):
        session, msg, recorder, _ = await self.detach_waiting_turn()
        self.assertNotIn("terminal close", self.orca.verbs())
        self.assertIsNone(session.proc.poll())
        binding = self.bindings.get("sess-1")
        assert binding is not None and binding.handle
        turn = recorder.turn
        self.assertEqual((turn["label"], turn["task_id"], turn["attempt_id"]),
                         ("develop", "sess-1", msg.header["attempt_id"]))

        runner = self.restarted_runner()
        run = FakeRun(self.root / "run")
        resumed = asyncio.ensure_future(self.converse_with(runner, TurnSlot(run, turn, Recorder())))
        run.task = resumed
        await asyncio.sleep(0.2)
        result()(session, msg)
        reply = await asyncio.wait_for(resumed, 10)
        self.assertEqual(reply.data["summary"], "fixed")
        self.assertEqual(reply.handle, binding.handle)
        # The agent got the task once; nothing was sent again.
        self.assertEqual(len(session.received), 1)
        self.assertEqual(self.orca.verbs().count("terminal create"), 1)

    async def test_a_result_sent_before_the_terminal_ended_is_used(self):
        session, msg, recorder, _ = await self.detach_waiting_turn()
        result()(session, msg)
        await asyncio.to_thread(self.wait_inbox, 1)
        handle = next(iter(self.orca.terminals))
        self.orca.end(handle)

        notes: list[str] = []
        reply = await self.converse_with(self.restarted_runner(),
                                         TurnSlot(FakeRun(self.root / "run"), recorder.turn, Recorder(), notes))
        self.assertEqual(reply.data["summary"], "fixed")
        self.assertEqual(self.orca.verbs().count("terminal create"), 1)
        self.assertEqual(notes, [])

    async def test_a_terminal_gone_without_a_result_runs_the_turn_again(self):
        _, _, recorder, _ = await self.detach_waiting_turn()
        self.orca.end(next(iter(self.orca.terminals)))
        self.orca.scripts.append([result()])
        notes: list[str] = []
        reply = await self.converse_with(self.restarted_runner(),
                                         TurnSlot(FakeRun(self.root / "run"), recorder.turn, Recorder(), notes))
        self.assertEqual(reply.data["summary"], "fixed")
        self.assertEqual(self.orca.verbs().count("terminal create"), 2)
        [note] = notes
        self.assertIn("ended without a result", note)

    def wait_inbox(self, count: int) -> None:
        from ccpeer.inbox import JsonlLog
        log = JsonlLog(self.state / RECEIVER / "inbox.jsonl")
        deadline = time.monotonic() + 45
        while len(log.read().records) < count:
            if time.monotonic() > deadline:
                raise AssertionError("no reply in the receiver's inbox")
            time.sleep(0.02)


class TestCLIReviewSurvivesDetach(TerminalCase):
    async def test_a_cli_review_in_an_orca_terminal_is_picked_up_after_a_restart(self):
        self.cli.set({"stdout": json.dumps(PASS), "sleep": 1.5})
        reviewer = self.reviewer()
        run = FakeRun(self.root / "run")
        recorder = Recorder()
        task = asyncio.ensure_future(reviewer.review(self.request(), self.caps, slot=TurnSlot(run, None, recorder)))
        run.task = task
        await until(lambda: recorder.turn and run.parked, what="the parked review")
        run.stop()
        with self.assertRaises(asyncio.CancelledError):
            await task
        (handle,) = self.handles()
        self.assertTrue(self.alive(handle))
        self.assertNotIn("terminal close", self.orca.verbs())
        turn_dir = Path(recorder.turn["dir"])
        self.assertTrue(turn_dir.is_relative_to(self.root / "run"))

        restarted = OrcaCommandTerminal(root=self.runs, poll_interval=0.02, liveness_interval=0.2)
        resumed = self.reviewer(runner=restarted)
        review = await resumed.review(self.request(), self.caps,
                                      slot=TurnSlot(FakeRun(self.root / "run"), recorder.turn, Recorder()))
        self.assertEqual(review["verdict"], "pass")
        self.assertEqual(review["cli_terminals"], [handle])
        self.assertEqual(len(self.cli.calls()), 1)
        self.assertFalse(turn_dir.exists())
        # The detach left the terminal open; the process that read the result closes it.
        self.assertEqual(self.closes(), [handle])

    async def test_a_subprocess_review_is_not_detached(self):
        self.cli.set({"stdout": json.dumps(PASS), "sleep": 0.5})
        reviewer = self.reviewer(terminal="")
        run = FakeRun(self.root / "run")
        task = asyncio.ensure_future(reviewer.review(self.request(), self.caps, slot=TurnSlot(run, None, Recorder())))
        run.task = task
        await asyncio.sleep(0.1)
        run.stop()
        review = await asyncio.wait_for(task, 10)
        self.assertEqual(review["verdict"], "pass")
        self.assertFalse(run.detached())


# --- delivery: a stop while waiting for CI ---


class TestDeliveryDetachesWhileWaitingForCI(DeliveryCase):
    def test_the_wait_resumes_without_pushing_again_and_keeps_its_deadline(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        state = self.gh_state()
        state["checks"] = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        self.state_file.write_text(json.dumps(state))
        now = [1000.0]
        stop = [False]

        def sleep(seconds: float) -> None:
            now[0] += seconds
            stop[0] = True

        delivery = self.delivery(auto_merge=True, ci_timeout_seconds=100, ci_poll_seconds=10, sleep=sleep)
        delivery._clock = lambda: now[0]
        with self.assertRaises(DeliveryDetached):
            delivery.deliver(self.request(wt), permissions=InMemoryPermission(), should_detach=lambda: stop[0])
        rec = delivery.record(f"task:{wt.name}")
        self.assertEqual((rec.state, rec.ci_head, rec.ci_since), ("pr_open", rec.head_sha, 1000.0))
        pushed = self.remote_head("hb/task-a")
        self.assertEqual(len(self.gh_calls("create")), 1)

        # The next process: the same head keeps the wait it began, so the deadline counts from 1000.
        now[0] = 1150.0
        waited: list[float] = []

        def sleep_again(seconds: float) -> None:
            waited.append(seconds)
            now[0] += seconds

        again = self.delivery(auto_merge=True, ci_timeout_seconds=100, ci_poll_seconds=10, sleep=sleep_again)
        again._clock = lambda: now[0]
        rec = again.deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertIn("CI has not finished for PR #1 after 100s", rec.problem)
        self.assertEqual(waited, [])
        self.assertEqual(len(self.gh_calls("create")), 1)
        self.assertEqual(self.remote_head("hb/task-a"), pushed)
        self.assertEqual(self.check_log.read_text(), "ran\n")


# --- the SDE end to end: develop, review and a broken state.json across a restart ---


def committed(repo: Path, text: str):
    def step(session: FakeClaude, msg: Incoming) -> None:
        (repo / "calc.py").write_text(text)
        git(repo, "commit", "-qam", "divide rejects zero")
        result()(session, msg)
    return step


def verdict_pass(session: FakeClaude, msg: Incoming) -> None:
    session.reply(msg, json.dumps({"task_id": msg.header["task_id"], "attempt_id": msg.header["attempt_id"],
                                   "verdict": "pass", "summary": "zero is rejected", "blocking": []}))


class SDERestartCase(PeerCase):
    # What each process hands its SDE on top of the rest.
    sde_extra: dict = {}

    def setUp(self):
        super().setUp()
        self.repo = Path(os.path.realpath(make_base_repo(self.root)))
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.runs_root = self.root / "runs"

    def process(self, db: SQLiteDatabase, name: str) -> tuple[AgentWorker, SQLiteOpenRunStore]:
        runner = self.make_runner(db)
        sessions = SQLiteSessionStoreAdapter(db)
        workspaces = SQLiteWorkspaceStore(db)
        launcher = ClaudeCodeLauncher(name="fake", command="claude")
        reviewer = CoderReviewerExecutor(profile=ClaudeCodeProfile(name="code-reviewer", launcher=launcher,
                                                                   timeout_seconds=30),
                                         workspaces=workspaces, tasks=SQLiteTaskMetadataStore(db), terminal=runner)
        sde = SDEExecutor(profile=ClaudeCodeProfile(name="sde", launcher=launcher, timeout_seconds=30),
                          options=SDEOptions(), reviewer=reviewer,
                          worktree_manager=OrcaWorktreeManager(workspace_store=workspaces, session_store=sessions,
                                                               backend="git"),
                          inbox_checkout_map={}, tasks=SQLiteTaskMetadataStore(db), terminal=runner, **self.sde_extra)
        memory = Mock()
        memory.load_baseline.return_value = {}
        runs = SQLiteOpenRunStore(db, self.runs_root)
        worker = AgentWorker(worker_id=name, inbox=SQLiteInboxAdapter(db), timers=SQLiteTimerAdapter(db),
                             memory=memory, permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db),
                             lock=SQLiteLeaseLockAdapter(db), sessions=sessions, executors={"sde": sde},
                             global_default_executor="sde", open_runs=runs)
        return worker, runs

    def restarted(self) -> tuple[AgentWorker, SQLiteOpenRunStore, SQLiteDatabase]:
        db = SQLiteDatabase(self.db_path)
        self.addCleanup(db.close)
        worker, runs = self.process(db, "new")
        return worker, runs, db

    def submit(self) -> Event:
        stored, _ = SQLiteInboxAdapter(self.db).append(Event(
            inbox_id=INBOX, source="test", external_id="task-1", correlation_key="task:1",
            payload={"executor_id": "sde", "text": "divide must reject a zero divisor",
                     "explicit_workdir": str(self.repo), "base_ref": "main"}))
        return stored

    def saved_state(self, runs: SQLiteOpenRunStore) -> dict | None:
        open_runs = runs.list_open(INBOX)
        if not open_runs:
            return None
        data, _ = read_state(open_runs[0].run_dir, STATE_FORMAT)
        return data

    async def stop_when(self, worker: AgentWorker, runs: SQLiteOpenRunStore, phase: str) -> None:
        running = asyncio.create_task(worker.process_inbox(INBOX))
        def sent_handle():
            state = self.saved_state(runs)
            if state and state["phase"] == phase and state["turn"] and state["turn"]["sent"]:
                return state["turn"]["handle"]
            return None

        handle = await until(sent_handle, what=f"the {phase} turn")
        peer = self.orca.terminals[handle]["session"]
        await asyncio.to_thread(peer.wait_received, 1)
        worker.detach()
        self.assertEqual(await asyncio.wait_for(running, 5), [])

    def outbox(self, db: SQLiteDatabase) -> list[str]:
        return [m.content for m in SQLiteOutboxAdapter(db).fetch_pending(limit=50)]

    def event_status(self, db: SQLiteDatabase, evt: Event) -> str:
        return db.fetchone("SELECT status FROM event_inbox WHERE event_id = ?", (evt.event_id,))["status"]


class TestSDEResumesAfterRestart(SDERestartCase):
    async def test_a_stop_during_development_resumes_into_review_without_sending_the_task_again(self):
        self.orca.scripts += [[None], [verdict_pass]]
        evt = self.submit()
        worker, runs = self.process(self.db, "old")
        await self.stop_when(worker, runs, "develop")

        developer = self.orca.session(0)
        self.assertNotIn("terminal close", self.orca.verbs())
        self.assertIsNone(developer.proc.poll())
        self.assertEqual(self.event_status(self.db, evt), "processing")
        # The agent keeps working while Hibernaut is down, commits and sends its result.
        [task] = self.tasks(developer)
        committed(self.repo, "def divide(a, b):\n    if b == 0:\n        raise ValueError\n    return a / b\n")(
            developer, task)

        worker, runs, db = self.restarted()
        [report] = await worker.process_inbox(INBOX)
        self.assertIsInstance(report.outcome, Emit)
        text = report.outcome.messages[0]
        self.assertIn("[sde] delivered (review=pass after 1 round(s)", text)
        self.assertNotIn("Restart:", text)
        self.assertEqual(len(self.tasks(developer)), 1)
        self.assertEqual(len(self.tasks(self.orca.session(1))), 1)
        self.assertEqual(self.event_status(db, evt), "done")
        self.assertEqual(runs.list_open(INBOX), [])
        self.assertEqual(list(self.runs_root.glob("*/*")), [])

    async def test_a_stop_during_review_resumes_the_same_review(self):
        self.orca.scripts += [[committed(self.repo, "def divide(a, b):\n    return a / b if b else 0\n")], [None]]
        evt = self.submit()
        worker, runs = self.process(self.db, "old")
        await self.stop_when(worker, runs, "review")
        reviewer = self.orca.session(1)
        [review_task] = self.tasks(reviewer)
        self.assertIsNone(reviewer.proc.poll())
        verdict_pass(reviewer, review_task)

        worker, runs, db = self.restarted()
        [report] = await worker.process_inbox(INBOX)
        self.assertIn("review=pass after 1 round(s)", report.outcome.messages[0])
        self.assertEqual(len(self.tasks(reviewer)), 1)
        self.assertEqual(len(self.orca.terminals), 2)
        self.assertEqual(self.event_status(db, evt), "done")

    async def test_a_broken_state_file_starts_development_again_and_says_why(self):
        self.orca.scripts += [
            [None, committed(self.repo, "def divide(a, b):\n    return a / b if b else 0\n")], [verdict_pass],
        ]
        self.submit()
        worker, runs = self.process(self.db, "old")
        await self.stop_when(worker, runs, "develop")
        [open_run] = runs.list_open(INBOX)
        Path(open_run.run_dir, STATE_FILE).write_text("{not json")

        worker, _, db = self.restarted()
        [report] = await worker.process_inbox(INBOX)
        text = report.outcome.messages[0]
        self.assertIn("delivered (review=pass", text)
        self.assertIn("Restart: Resumed after a restart without a usable state", text)
        self.assertIn("is not valid JSON", text)
        self.assertEqual(len(self.tasks(self.orca.session(0))), 2)
        self.assertEqual(self.outbox(db), [text])


if __name__ == "__main__":
    unittest.main()
