import asyncio
import contextlib
import json
import time
import unittest
from pathlib import Path

from tests.support.fakes import InMemoryPermission
from tests.support.cli_reviewer import PASS, TerminalCase
from tests.support.peer_handoff import RECEIVER, FakeClaude, Incoming, PeerCase, result
from tests.support.pr_delivery import DeliveryCase
from agent_runtime.adapters.delivery import DeliveryDetached
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.run_state import STATE_FILE, TurnSlot
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.core.models import Emit
from tests.support.resume_runs import INBOX, SDERestartCase, committed, until, verdict_pass


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
