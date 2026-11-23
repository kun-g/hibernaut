import asyncio
import contextlib
import io
import json
import socket
import tempfile
import time
import unittest
from collections import Counter, defaultdict
from pathlib import Path
from unittest.mock import patch

from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config, concurrency_limits, serve_configured
from agent_runtime.app.projects import AmbiguousProjectMatch
from agent_runtime.core.models import (
    CLOSE_SOURCE,
    FORWARD_SOURCE,
    RETURN_CANCELLED_SOURCE,
    Emit,
    Event,
    ForwardSpec,
    HeldReturn,
    SessionRef,
    Silent,
)
from agent_runtime.engine import AgentWorker

MAIN_LOGGER = "agent_runtime.app.main"


def project(checkout="/tmp/x", **extra):
    return {"repo": "o/r", "checkout": str(checkout), "worktree_backend": "git", **extra}


class TestConfig(unittest.TestCase):
    def parse(self, **sections):
        return parse_config(sections)

    def test_limits_are_read_from_cli_and_role_executors_and_projects(self):
        cfg = self.parse(
            executors={"cli": {"command": "x", "max_concurrency": 3},
                       "code-reviewer": {"role": "code-reviewer", "max_concurrency": 2},
                       "sde": {"role": "sde", "max_concurrency": 1}},
            projects={"p": project(max_concurrency=2)},
        )
        self.assertEqual(cfg.executors["cli"].max_concurrency, 3)
        self.assertEqual(cfg.roles["code-reviewer"].max_concurrency, 2)
        self.assertEqual(cfg.roles["sde"].max_concurrency, 1)
        self.assertEqual(cfg.projects["p"].max_concurrency, 2)

    def test_limits_default_to_none(self):
        cfg = self.parse(executors={"cli": {"command": "x"}, "code-reviewer": {"role": "code-reviewer"},
                                    "sde": {"role": "sde"}},
                         projects={"p": project()})
        self.assertIsNone(cfg.executors["cli"].max_concurrency)
        self.assertIsNone(cfg.roles["sde"].max_concurrency)
        self.assertIsNone(cfg.projects["p"].max_concurrency)

    def test_invalid_limits_are_rejected_with_the_config_path(self):
        for bad in (0, -1, True, False, "2", 1.5, [1]):
            cases = {
                "executors.cli.max_concurrency": {"executors": {"cli": {"command": "x", "max_concurrency": bad}}},
                "executors.sde.max_concurrency": {
                    "executors": {"sde": {"role": "sde", "review": False, "max_concurrency": bad}}},
                "projects.p.max_concurrency": {"projects": {"p": project(max_concurrency=bad)}},
            }
            for where, raw in cases.items():
                with self.subTest(where=where, bad=bad), self.assertRaisesRegex(
                        ValueError, f"^{where} must be a positive integer"):
                    parse_config(raw)

    def test_a_limit_above_the_global_one_loads_with_a_warning(self):
        with self.assertLogs("agent_runtime.app.config", level="WARNING") as logs:
            cfg = self.parse(runtime={"max_concurrency": 2},
                             executors={"cli": {"command": "x", "max_concurrency": 3},
                                        "sde": {"role": "sde", "review": False, "max_concurrency": 2}},
                             projects={"p": project(max_concurrency=5)})
        self.assertEqual(cfg.executors["cli"].max_concurrency, 3)
        messages = [r.getMessage() for r in logs.records]
        self.assertEqual(len(messages), 2, messages)
        self.assertIn("executors.cli.max_concurrency = 3", messages[0])
        self.assertIn("projects.p.max_concurrency = 5", messages[1])
        self.assertTrue(all("runtime.max_concurrency = 2" in m for m in messages))


class TestLimiter(unittest.TestCase):
    def limiter(self, max_concurrency=4, projects=None, executors=None):
        from agent_runtime.app.concurrency import ConcurrencyLimiter, ConcurrencyLimits

        return ConcurrencyLimiter(max_concurrency, ConcurrencyLimits(projects=projects or {}, executors=executors or {}))

    def test_each_dimension_limits_on_its_own(self):
        lim = self.limiter(3, {"a": 1}, {"sde": 1})
        self.assertTrue(lim.try_acquire("i1", project_key="a", executor_key="claude"))
        self.assertFalse(lim.try_acquire("i2", project_key="a", executor_key="claude"))
        self.assertTrue(lim.try_acquire("i3", project_key="b", executor_key="sde"))
        self.assertFalse(lim.try_acquire("i4", project_key="b", executor_key="sde"))
        self.assertTrue(lim.try_acquire("i5", project_key=None, executor_key=None))
        self.assertFalse(lim.try_acquire("i6", project_key=None, executor_key=None))
        lim.release("i5")
        self.assertTrue(lim.try_acquire("i6", project_key=None, executor_key=None))

    def test_a_refused_acquire_takes_nothing(self):
        lim = self.limiter(4, {"a": 1, "b": 1}, {"sde": 1})
        self.assertTrue(lim.try_acquire("i1", project_key="a", executor_key="sde"))
        # Refused on the executor: project b and the global count stay free.
        self.assertFalse(lim.try_acquire("i2", project_key="b", executor_key="sde"))
        self.assertTrue(lim.try_acquire("i3", project_key="b", executor_key=None))
        lim.release("i1")
        lim.release("i3")
        self.assertEqual(lim.active, 0)
        self.assertTrue(lim.try_acquire("i2", project_key="b", executor_key="sde"))

    def test_release_is_idempotent_and_returns_only_what_was_taken(self):
        lim = self.limiter(2, {"a": 2})
        self.assertTrue(lim.try_acquire("i1", project_key="a", executor_key=None))
        self.assertTrue(lim.try_acquire("i2", project_key="a", executor_key=None))
        for _ in range(3):
            lim.release("i1")
        lim.release("never")
        self.assertEqual(lim.active, 1)
        self.assertTrue(lim.try_acquire("i3", project_key="a", executor_key=None))
        self.assertFalse(lim.try_acquire("i4", project_key="a", executor_key=None))

    def test_an_inbox_holds_at_most_one_slot(self):
        lim = self.limiter(4)
        self.assertTrue(lim.try_acquire("i1", project_key=None, executor_key=None))
        self.assertFalse(lim.try_acquire("i1", project_key=None, executor_key=None))
        self.assertEqual(lim.active, 1)

    def test_reconfigure_keeps_held_slots_and_notifies_listeners(self):
        from agent_runtime.app.concurrency import ConcurrencyLimits

        lim = self.limiter(3, {"a": 2}, {"sde": 2})
        events = []
        unsub = lim.subscribe(lambda: events.append("changed"))
        self.assertTrue(lim.try_acquire("i1", project_key="a", executor_key="sde"))
        self.assertTrue(lim.try_acquire("i2", project_key="a", executor_key="sde"))
        # Lowering global cap below active count saturates until held slots drop below the new cap.
        lim.reconfigure(1, ConcurrencyLimits(projects={"a": 1}, executors={"sde": 1}))
        self.assertEqual(len(events), 1)
        self.assertTrue(lim.saturated())
        self.assertFalse(lim.available("i3", project_key="b", executor_key=None))
        lim.release("i1")
        self.assertEqual(len(events), 2)
        self.assertTrue(lim.saturated())
        lim.release("i2")
        self.assertEqual(len(events), 3)
        self.assertFalse(lim.saturated())
        self.assertTrue(lim.try_acquire("i3", project_key="a", executor_key="sde"))
        unsub()
        lim.release("i3")
        self.assertEqual(len(events), 3)

    def test_reconfigure_counts_agents_held_under_the_old_limits(self):
        from agent_runtime.app.concurrency import ConcurrencyLimits

        lim = self.limiter(4)
        self.assertTrue(lim.try_acquire("i1", project_key=None, executor_key="cc", agent_key="sde"))
        lim.reconfigure(4, ConcurrencyLimits(agents={"sde": 1}))
        self.assertFalse(lim.try_acquire("i2", project_key=None, executor_key="cc", agent_key="sde"))
        self.assertTrue(lim.try_acquire("i3", project_key=None, executor_key="cc", agent_key="steward"))
        lim.release("i1")
        self.assertTrue(lim.try_acquire("i2", project_key=None, executor_key="cc", agent_key="sde"))


class Probe:
    # Stands in for every configured CLI executor of the daemon under test; runs on text "block..." wait for release().
    def __init__(self):
        self.active: list[tuple[str, str]] = []
        self.peaks: dict[tuple[str, str], int] = defaultdict(int)
        self.started: list[str] = []
        self.finished: list[str] = []
        self.cancelled: list[str] = []
        self.broken: set[str] = set()
        self.gates: dict[str, asyncio.Event] = defaultdict(asyncio.Event)

    def release(self, *texts):
        for text in texts:
            self.gates[text].set()

    def active_texts(self):
        return sorted(text for _, text in self.active)

    def peak(self, kind, name):
        return self.peaks[kind, name]

    def executor(self, executor_id):
        probe = self

        class _Executor:
            def __init__(self):
                self.executor_id = executor_id

            async def execute(self, events, session, caps):
                text = events[-1].payload["text"]
                inbox_id = events[0].inbox_id
                entry = (executor_id, text)
                probe.active.append(entry)
                probe.started.append(text)
                probe._measure("executor", executor_id, lambda: sum(e == executor_id for e, _ in probe.active))
                probe._inboxes.append(inbox_id)
                owner = ":".join(inbox_id.split(":")[:2])
                probe._measure("project", owner, lambda: sum(i.startswith(owner) for i in probe._inboxes))
                probe._measure("all", "", lambda: len(probe.active))
                try:
                    if text.startswith("block"):
                        await probe.gates[text].wait()
                    else:
                        await asyncio.sleep(0.02)
                    if text in probe.broken:
                        raise RuntimeError(f"{text} is broken")
                except asyncio.CancelledError:
                    probe.cancelled.append(text)
                    raise
                finally:
                    probe.active.remove(entry)
                    probe._inboxes.remove(inbox_id)
                probe.finished.append(text)
                return Emit(messages=(f"{text} by {executor_id}",))

        return _Executor()

    _inboxes: list[str]

    def _measure(self, kind, name, value):
        self.peaks[kind, name] = max(self.peaks[kind, name], value())


class PoolCase(unittest.IsolatedAsyncioTestCase):
    policy = "defer"

    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.probe = Probe()
        self.probe._inboxes = []
        # Real configuration and composition root; only the CLI subprocess is replaced.
        self.enterContext(patch(
            "agent_runtime.app.main.LocalCLIExecutor",
            lambda *, default_executor, **_: self.probe.executor(default_executor),
        ))

    def raw(self, *, max_concurrency=4, executors=None, projects=None):
        return {
            "runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem"),
                        "max_concurrency": max_concurrency, "default_executor": "claude",
                        "new_event_policy": self.policy, "worktree_sweep_interval_seconds": 0},
            "executors": {name: {"command": "unused", **extra}
                          for name, extra in {"claude": {}, "sde": {}, **(executors or {})}.items()},
            "projects": {name: project(self.root, **extra) for name, extra in (projects or {}).items()},
        }

    @contextlib.asynccontextmanager
    async def daemon(self, **limits):
        daemon = build_daemon_from_config(parse_config(self.raw(**limits)))
        stop = asyncio.Event()
        runs = asyncio.create_task(daemon.pool.run_forever(stop))
        self._seq = 0
        try:
            yield daemon
        finally:
            self.probe.release(*self.probe.gates)
            stop.set()
            await asyncio.wait_for(runs, 10)
            daemon.close()

    def post(self, daemon, inbox_id, text, *, executor=None, key=None, **payload):
        self._seq += 1
        if executor is not None:
            payload["executor_id"] = executor
        return daemon.handler.ingest(inbox_id=inbox_id, source="test", external_id=f"{text}-{self._seq}",
                                     payload={"text": text, **payload}, correlation_key=key)

    async def until(self, check, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not check():
            if time.monotonic() > deadline:
                self.fail(f"timed out: started={self.probe.started} active={self.probe.active}")
            await asyncio.sleep(0.005)

    async def settle(self):
        await asyncio.sleep(0.1)

    async def idle(self, daemon):
        await daemon.pool.wait_until_idle(timeout_seconds=5)


TASKS = {"inbox_id": ["proj:a", "proj:a:.+"]}


class TestProjectLimit(PoolCase):
    async def test_a_projects_inboxes_never_exceed_its_limit_and_all_finish(self):
        async with self.daemon(projects={"a": {**TASKS, "max_concurrency": 2}, "b": {"inbox_id": "proj:b"}}) as daemon:
            texts = [f"block-{n}" for n in range(4)]
            for n, text in enumerate(texts):
                self.post(daemon, f"proj:a:issue-{n}", text)
            await self.until(lambda: len(self.probe.active) == 2)
            await self.settle()
            self.assertEqual(self.probe.active_texts(), texts[:2])
            self.assertFalse(daemon.pool._idle_event.is_set())
            # Another project is not affected.
            self.post(daemon, "proj:b", "b")
            await self.until(lambda: "b" in self.probe.finished)

            # Waiting inboxes start in the order they arrived as slots come back.
            self.probe.release(texts[1])
            await self.until(lambda: self.probe.active_texts() == [texts[0], texts[2]])
            self.probe.release(texts[0])
            await self.until(lambda: self.probe.active_texts() == texts[2:])
            self.probe.release(*texts)
            await self.idle(daemon)
            self.assertEqual(sorted(self.probe.finished), sorted([*texts, "b"]))
            self.assertEqual(self.probe.peak("project", "proj:a"), 2)

    async def test_without_limits_only_the_global_one_applies(self):
        async with self.daemon(max_concurrency=3, projects={"a": TASKS}) as daemon:
            texts = [f"block-{n}" for n in range(4)]
            for n, text in enumerate(texts):
                self.post(daemon, f"proj:a:issue-{n}", text, executor="sde")
            await self.until(lambda: len(self.probe.active) == 3)
            await self.settle()
            self.assertEqual(self.probe.active_texts(), texts[:3])
            self.probe.release(*texts)
            await self.idle(daemon)
            self.assertEqual(self.probe.peak("all", ""), 3)

    async def test_a_limit_above_the_global_one_is_capped_by_it(self):
        with self.assertLogs("agent_runtime.app.config", level="WARNING"):
            config = self.daemon(max_concurrency=1, projects={"a": {**TASKS, "max_concurrency": 5}})
            async with config as daemon:
                for n in range(3):
                    self.post(daemon, f"proj:a:issue-{n}", f"t{n}")
                await self.idle(daemon)
        self.assertEqual(self.probe.started, ["t0", "t1", "t2"])
        self.assertEqual(self.probe.peak("all", ""), 1)


class TestExecutorLimit(PoolCase):
    async def test_one_executor_runs_at_most_its_limit_across_projects(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}},
                               projects={"a": TASKS, "b": {"inbox_id": "proj:b", "executor": "sde"}}) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-1", executor="sde")
            # The project default and the deployment default count as well as an explicit executor_id.
            self.post(daemon, "proj:b", "block-2")
            self.post(daemon, "proj:a:issue-3", "block-3", executor="sde")
            for n in (1, 2, 3):
                await self.until(lambda: self.probe.active_texts() == [f"block-{n}"])
                await self.settle()
                self.assertEqual(self.probe.active_texts(), [f"block-{n}"])
                self.probe.release(f"block-{n}")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished, ["block-1", "block-2", "block-3"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_the_global_default_executor_counts(self):
        async with self.daemon(executors={"claude": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            for n in range(3):
                self.post(daemon, f"proj:a:issue-{n}", f"t{n}")
            await self.idle(daemon)
        self.assertEqual(self.probe.started, ["t0", "t1", "t2"])
        self.assertEqual(self.probe.peak("executor", "claude"), 1)

    async def test_a_follow_up_without_executor_id_counts_for_its_sessions_executor(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:issue-1", "first", executor="sde", key="task:1")
            await self.idle(daemon)
            self.post(daemon, "proj:a:issue-2", "block-other", executor="sde", key="task:2")
            await self.until(lambda: self.probe.active_texts() == ["block-other"])
            self.post(daemon, "proj:a:issue-1", "follow-up", key="task:1")
            await self.settle()
            self.assertNotIn("follow-up", self.probe.started)
            self.probe.release("block-other")
            await self.idle(daemon)
        self.assertEqual(self.probe.started, ["first", "block-other", "follow-up"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_waiting_inboxes_do_not_block_an_inbox_whose_quota_is_free(self):
        async with self.daemon(max_concurrency=3, executors={"sde": {"max_concurrency": 1}},
                               projects={"hibernaut": {"inbox_id": ["proj:hibernaut", "proj:hibernaut:.+"]}}) as daemon:
            for n in (1, 2, 3):
                self.post(daemon, f"proj:hibernaut:sde:issue-{n}", f"block-{n}", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-1"])
            self.post(daemon, "proj:hibernaut", "light")
            await self.until(lambda: "light" in self.probe.finished)
            self.assertEqual(self.probe.active_texts(), ["block-1"])
            self.assertEqual(self.probe.started, ["block-1", "light"])
            self.probe.release("block-1", "block-2", "block-3")
            await self.idle(daemon)
        self.assertEqual(self.probe.started, ["block-1", "light", "block-2", "block-3"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_one_release_starts_every_inbox_it_unblocks(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={
            "a": {**TASKS, "max_concurrency": 1}, "b": {"inbox_id": "proj:b"},
        }) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-both", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-both"])
            self.post(daemon, "proj:a:issue-2", "block-project")
            self.post(daemon, "proj:b", "block-sde", executor="sde")
            await self.settle()
            self.assertEqual(self.probe.active_texts(), ["block-both"])
            # The slot it gives back is one of project a's and one of sde's: both waiting inboxes run at once.
            self.probe.release("block-both")
            await self.until(lambda: self.probe.active_texts() == ["block-project", "block-sde"])

    async def test_waiting_for_quota_does_not_poll(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            for n in (1, 2, 3):
                self.post(daemon, f"proj:a:issue-{n}", f"block-{n}", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-1"])
            await self.settle()
            with patch.object(daemon.pool._limiter, "try_acquire", wraps=daemon.pool._limiter.try_acquire) as attempts:
                await asyncio.sleep(0.3)
                self.assertEqual(attempts.call_count, 0)
                self.probe.release("block-1", "block-2", "block-3")
                await self.idle(daemon)
                self.assertGreater(attempts.call_count, 0)


class TestOneInboxWithSeveralExecutors(PoolCase):
    # An inbox runs many batches per wake; the slot it was admitted with covers only its own executor.
    async def test_an_event_for_a_full_executor_waits_even_behind_another_executors_batch(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-sde", executor="sde")
            self.post(daemon, "proj:a", "block-chat", key="chat")
            await self.until(lambda: self.probe.active_texts() == ["block-chat", "block-sde"])
            # Arrives while the inbox is running on claude's slot.
            self.post(daemon, "proj:a", "chat-sde", executor="sde", key="task")
            self.post(daemon, "proj:a", "chat-2", key="chat")
            self.probe.release("block-chat")
            await self.until(lambda: "block-chat" in self.probe.finished)
            await self.settle()
            # Inbox order is kept: nothing queued behind the waiting event overtakes it.
            self.assertEqual(self.probe.active_texts(), ["block-sde"])
            self.assertNotIn("chat-2", self.probe.started)
            self.assertFalse(daemon.pool._idle_event.is_set())
            self.probe.release("block-sde")
            await self.idle(daemon)
        self.assertEqual([t for t in self.probe.started if t.startswith(("chat", "block-chat"))],
                         ["block-chat", "chat-sde", "chat-2"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_mixed_batches_queued_together_run_in_order_and_each_once(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}, "claude": {"max_concurrency": 1}},
                               projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:hold", "block-hold", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-hold"])
            for n, executor in enumerate(("claude", "sde", "claude", "sde")):
                self.post(daemon, "proj:a", f"t{n}", executor=executor)
            await self.settle()
            self.assertEqual(self.probe.started, ["block-hold", "t0"])
            self.probe.release("block-hold")
            await self.idle(daemon)
            rows = daemon.db.fetchall("SELECT status, attempts FROM event_inbox WHERE inbox_id = 'proj:a'")
            self.assertEqual([(r["status"], r["attempts"]) for r in rows], [("done", 1)] * 4)
        self.assertEqual(self.probe.started, ["block-hold", "t0", "t1", "t2", "t3"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_events_left_behind_run_after_the_batch_ahead_of_them_fails(self):
        self.probe.broken.add("bad")
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:s", "block-sde", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-sde"])
            with self.assertLogs(MAIN_LOGGER, level="ERROR") as logs:
                self.post(daemon, "proj:a", "bad", executor="claude")
                self.post(daemon, "proj:a", "later", executor="sde")
                await self.until(lambda: len(daemon.inbox.list_events(inbox_id="proj:a", status="failed", limit=5)) == 1)
                await self.until(lambda: daemon.pool.limiter.active == 1)
            self.assertEqual(len(logs.records), 1, logs.output)
            self.assertFalse(daemon.pool._idle_event.is_set())
            self.probe.release("block-sde")
            # No further wake: the failed event is out of the way, so the inbox was queued again for sde.
            await self.until(lambda: "later" in self.probe.finished)
            await self.idle(daemon)
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_a_slot_freed_during_the_dispatch_is_used_without_another_wake(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:s", "block-sde", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-sde"])
            for text, executor in (("block-chat", "claude"), ("task", "sde"), ("chat", "claude")):
                self.post(daemon, "proj:a", text, executor=executor)
            await self.until(lambda: self.probe.active_texts() == ["block-chat", "block-sde"])
            self.probe.release("block-sde")
            await self.until(lambda: "block-sde" in self.probe.finished)
            self.probe.release("block-chat")
            await self.until(lambda: "chat" in self.probe.finished)
            await self.idle(daemon)
        # In inbox order: what follows the event that had to wait does not overtake it.
        self.assertEqual(self.probe.started, ["block-sde", "block-chat", "task", "chat"])

    async def test_an_event_that_becomes_covered_during_the_dispatch_continues_in_it(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}, "claude": {"max_concurrency": 1}},
                               projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a", "first", executor="claude", key="task")
            await self.idle(daemon)
            # Claimed together: the follow-up's session is on claude until the sde batch ahead of it rebinds it.
            self.post(daemon, "proj:a", "block-task", executor="sde", key="task")
            self.post(daemon, "proj:a", "follow-up", key="task")
            await self.until(lambda: self.probe.active_texts() == ["block-task"])
            self.post(daemon, "proj:a:other", "other-sde", executor="sde")
            await self.settle()
            self.probe.release("block-task")
            await self.idle(daemon)
        self.assertEqual(self.probe.started, ["first", "block-task", "follow-up", "other-sde"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_an_unrunnable_event_ahead_of_a_waiting_one_does_not_spin(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            with self.assertLogs(MAIN_LOGGER, level="ERROR") as logs:
                # Registered nowhere: its batch stays pending for an operator, as without limits.
                self.post(daemon, "proj:a", "lost", executor="nobody")
                self.post(daemon, "proj:a", "behind", executor="sde")
                await self.idle(daemon)
                await self.settle()
            self.assertLessEqual(len(logs.records), 2, logs.output)
            self.assertEqual([e.payload["text"] for e in daemon.inbox.fetch_pending("proj:a")], ["lost", "behind"])
            self.assertEqual(daemon.pool._limiter.active, 0)


class TestReplay(PoolCase):
    # hibernaut_replay_event puts an already handled event back to pending, optionally for another executor.
    def replay(self, daemon, event_id, **arguments):
        assert daemon.mcp_server is not None
        reply = daemon.mcp_server.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "hibernaut_replay_event", "arguments": {"event_id": event_id, **arguments}}})
        assert reply is not None
        self.assertIn("result", reply, reply)
        self.assertFalse(reply["result"].get("isError", False), reply)

    async def test_an_event_replayed_for_a_full_executor_mid_dispatch_waits_for_its_slot(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-sde", executor="sde")
            first = self.post(daemon, "proj:a", "first", executor="claude")
            self.post(daemon, "proj:a", "block-chat", executor="claude")
            await self.until(lambda: self.probe.active_texts() == ["block-chat", "block-sde"])
            # This dispatch already ran the event on claude; its slot does not cover the replay on sde.
            self.replay(daemon, first.event_id, executor_id="sde", force=True)
            self.probe.release("block-chat")
            await self.until(lambda: "block-chat" in self.probe.finished)
            await self.settle()
            self.assertEqual(self.probe.active_texts(), ["block-sde"])
            self.assertEqual(self.probe.started.count("first"), 1)
            self.probe.release("block-sde")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished.count("first"), 2)
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_a_failed_event_replayed_on_another_executor_takes_that_executors_quota(self):
        self.probe.broken.add("bad")
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-sde", executor="sde")
            with self.assertLogs(MAIN_LOGGER, level="ERROR"):
                bad = self.post(daemon, "proj:a:issue-2", "bad", executor="claude")
                await self.until(lambda: len(daemon.inbox.list_events(inbox_id="proj:a:issue-2", status="failed",
                                                                      limit=5)) == 1)
                await self.until(lambda: daemon.pool.limiter.active == 1)
            self.probe.broken.clear()
            self.replay(daemon, bad.event_id, executor_id="sde")
            await self.settle()
            self.assertEqual(self.probe.active_texts(), ["block-sde"])
            self.assertFalse(daemon.pool._idle_event.is_set())
            self.probe.release("block-sde")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished, ["block-sde", "bad"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    LIMITS = {"executors": {"claude": {"max_concurrency": 1}, "sde": {"max_concurrency": 1}}, "projects": {"a": TASKS}}

    async def queued_for_claude(self, daemon):
        # proj:a:x has run "first" on claude and now waits for the claude slot, its limit keys cached; sde is full too.
        first = self.post(daemon, "proj:a:x", "first", executor="claude")
        await self.idle(daemon)
        self.post(daemon, "proj:a:c", "block-claude", executor="claude")
        self.post(daemon, "proj:a:s", "block-sde", executor="sde")
        await self.until(lambda: self.probe.active_texts() == ["block-claude", "block-sde"])
        self.post(daemon, "proj:a:x", "second", executor="claude")
        await self.settle()
        self.assertEqual(daemon.pool._queued_keys["proj:a:x"].executor, "claude")
        return first

    async def test_a_replay_re_keys_an_inbox_that_is_already_queued(self):
        for threaded in (False, True):
            with self.subTest(threaded=threaded):
                self.probe.__init__()
                self.probe._inboxes = []
                async with self.daemon(**self.LIMITS) as daemon:
                    first = await self.queued_for_claude(daemon)
                    if threaded:
                        # As from the HTTP server: the wake reaches the pool through call_soon_threadsafe.
                        await asyncio.to_thread(self.replay, daemon, first.event_id, executor_id="sde", force=True)
                    else:
                        self.replay(daemon, first.event_id, executor_id="sde", force=True)
                    self.probe.release("block-claude")
                    await self.until(lambda: "block-claude" in self.probe.finished)
                    await self.settle()
                    self.assertEqual(self.probe.active_texts(), ["block-sde"])
                    self.assertEqual(daemon.pool.limiter.active, 1)
                    self.probe.release("block-sde")
                    await self.idle(daemon)
                    self.assertEqual(self.probe.peak("executor", "sde"), 1)
                    self.assertEqual(self.probe.started[-2:], ["first", "second"])
                for path in self.root.glob("hb.db*"):
                    path.unlink()

    async def test_a_replay_stored_before_its_wake_arrives_is_not_dispatched_under_the_old_key(self):
        async with self.daemon(**self.LIMITS) as daemon:
            first = await self.queued_for_claude(daemon)
            self.post(daemon, "proj:a:y", "later-sde", executor="sde")
            await self.settle()
            # An HTTP thread stores the replay before its wake runs on the loop; a scan can come in between.
            daemon.inbox.replay(first.event_id, executor_id_override="sde", force=True)
            admitted = []
            with patch.object(daemon.pool.limiter, "try_acquire", wraps=daemon.pool.limiter.try_acquire) as acquire:
                self.probe.release("block-claude")
                await self.until(lambda: "block-claude" in self.probe.finished)
                await self.settle()
                admitted = [c.kwargs["executor_key"] for c in acquire.call_args_list if c.args == ("proj:a:x",)]
            self.assertEqual(self.probe.peak("executor", "sde"), 1)
            self.assertEqual(list(daemon.pool._queue), ["proj:a:x", "proj:a:y"])
            self.assertNotIn("claude", admitted)
            self.probe.release("block-sde")
            await self.idle(daemon)
            # It kept its place in the queue, ahead of the inbox that asked for sde after it.
            replayed = len(self.probe.started) - 1 - self.probe.started[::-1].index("first")
            self.assertLess(replayed, self.probe.started.index("later-sde"))
            self.assertEqual(self.probe.started.count("first"), 2)
            self.assertEqual(self.probe.peak("executor", "sde"), 1)


class TestInterruptPolicy(PoolCase):
    policy = "interrupt"

    async def test_an_event_that_cannot_run_yet_does_not_interrupt_the_run(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:issue-1", "block-sde", executor="sde")
            self.post(daemon, "proj:a", "block-chat")
            await self.until(lambda: self.probe.active_texts() == ["block-chat", "block-sde"])
            self.post(daemon, "proj:a", "chat-sde", executor="sde")
            await self.settle()
            self.assertEqual(self.probe.cancelled, [])
            self.probe.release("block-chat", "block-sde")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished.count("block-chat"), 1)
        self.assertIn("chat-sde", self.probe.finished)
        self.assertEqual(self.probe.peak("executor", "sde"), 1)


class TestFailuresReturnQuota(PoolCase):
    async def test_a_failed_run_returns_its_slots(self):
        self.probe.broken.add("bad")
        async with self.daemon(executors={"sde": {"max_concurrency": 1}},
                               projects={"a": {**TASKS, "max_concurrency": 1}}) as daemon:
            with self.assertLogs(MAIN_LOGGER, level="ERROR") as logs:
                self.post(daemon, "proj:a:issue-1", "bad", executor="sde")
                self.post(daemon, "proj:a:issue-2", "good", executor="sde")
                await self.idle(daemon)
            self.assertIn("proj:a:issue-1", logs.output[0])
            self.assertEqual(self.probe.finished, ["good"])
            self.assertEqual(daemon.pool._limiter.active, 0)
            self.post(daemon, "proj:a:issue-1", "again", executor="sde")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished, ["good", "again"])

    async def test_an_ambiguous_inbox_is_still_refused_and_returns_its_slot(self):
        # Loading rejects these patterns; dispatch still checks on its own.
        self.enterContext(patch("agent_runtime.app.projects._validate_disjoint_patterns"))
        async with self.daemon(max_concurrency=1, executors={"claude": {"max_concurrency": 1}}, projects={
            "a": {"inbox_id": ["proj:a", "shared:.*"], "max_concurrency": 1},
            "b": {"inbox_id": ["proj:b", ".*:x"], "max_concurrency": 1},
        }) as daemon:
            with self.assertLogs(MAIN_LOGGER, level="ERROR") as logs:
                self.post(daemon, "shared:x", "ambiguous")
                for inbox_id in ("proj:a", "proj:b"):
                    self.post(daemon, inbox_id, inbox_id)
                await self.idle(daemon)
            (record,) = logs.records
            assert record.exc_info is not None
            self.assertIs(record.exc_info[0], AmbiguousProjectMatch)
            self.assertEqual(self.probe.finished, ["proj:a", "proj:b"])
            self.assertEqual(len(daemon.inbox.fetch_pending("shared:x")), 1)
            self.assertEqual(daemon.pool._limiter.active, 0)

    async def test_an_executor_key_that_cannot_be_resolved_falls_back_to_the_other_limits(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            with (patch("agent_runtime.app.concurrency.AgentWorker.expected_executor_id", side_effect=RuntimeError("no")),
                  self.assertLogs("agent_runtime.app.concurrency", level="WARNING")):
                self.post(daemon, "proj:a", "t", executor="sde")
                await self.idle(daemon)
            self.assertEqual(self.probe.finished, ["t"])
            self.assertEqual(daemon.pool._limiter.active, 0)

    async def test_an_unmatched_inbox_waits_for_its_project_without_taking_executor_quota(self):
        async with self.daemon(executors={"claude": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a", "block-a")
            await self.until(lambda: self.probe.active_texts() == ["block-a"])
            with self.assertLogs("agent_runtime.app.projects", level="WARNING") as logs:
                self.post(daemon, "proj:orphan", "orphan")
                await self.until(lambda: not daemon.pool._queued)
            self.assertEqual(len(logs.records), 1, logs.output)
            self.assertEqual(daemon.pool._limiter.active, 1)
            # One that names its own workdir runs, so it does count.
            self.post(daemon, "proj:orphan-2", "explicit", explicit_workdir=str(self.root))
            await self.settle()
            self.assertNotIn("explicit", self.probe.started)
            self.probe.release("block-a")
            await self.idle(daemon)
            self.assertEqual(self.probe.finished, ["block-a", "explicit"])
            (held,) = daemon.inbox.fetch_pending("proj:orphan")
            self.assertEqual(held.status, "pending")


class TestLifecycle(PoolCase):
    LIMITS = {"executors": {"sde": {"max_concurrency": 1}}, "projects": {"a": TASKS}}

    async def three_sde(self, daemon):
        for n in (1, 2, 3):
            self.post(daemon, f"proj:a:issue-{n}", f"block-{n}", executor="sde")
        await self.until(lambda: self.probe.active_texts() == ["block-1"])

    async def test_retire_hands_over_every_waiting_inbox_once(self):
        async with self.daemon(**self.LIMITS) as daemon:
            await self.three_sde(daemon)
            handed: list[str] = []
            daemon.pool.retire(handed.append)
            self.assertEqual(handed, ["proj:a:issue-2", "proj:a:issue-3"])
            daemon.pool.wake("proj:a:issue-4")
            self.assertEqual(handed[2:], ["proj:a:issue-4"])
            self.assertFalse(daemon.pool._idle_event.is_set())
            self.probe.release("block-1")
            await self.idle(daemon)
            await self.settle()
            # The finished inbox goes too, once its lease is released; nothing waiting ran here.
            self.assertEqual(handed[3:], ["proj:a:issue-1"])
            self.assertEqual(self.probe.started, ["block-1"])
            self.assertEqual(len(daemon.inbox.fetch_pending("proj:a:issue-2")), 1)
            self.assertEqual(daemon.pool._limiter.active, 0)

    async def test_the_next_pool_runs_what_was_handed_over(self):
        async with self.daemon(**self.LIMITS) as old:
            await self.three_sde(old)
            replacement = build_daemon_from_config(parse_config(self.raw(**self.LIMITS)), database=old.db)
            stop = asyncio.Event()
            runs = asyncio.create_task(replacement.pool.run_forever(stop))
            old.pool.retire(replacement.pool.wake)
            # The new generation has its own quota: the old one's last Run is not counted against it.
            await self.until(lambda: self.probe.active_texts() == ["block-1", "block-2"])
            self.probe.release("block-1", "block-2", "block-3")
            await self.idle(old)
            await self.idle(replacement)
            stop.set()
            await asyncio.wait_for(runs, 5)
        self.assertEqual(Counter(self.probe.finished), Counter(["block-1", "block-2", "block-3"]))

    async def test_drain_drops_waiting_inboxes_and_leaves_their_events_pending(self):
        async with self.daemon(**self.LIMITS) as daemon:
            await self.three_sde(daemon)
            daemon.pool.drain()
            self.assertEqual(len(daemon.pool._queue), 0)
            self.probe.release("block-1")
            await self.idle(daemon)
            await self.settle()
            self.assertEqual(self.probe.started, ["block-1"])
            for n in (2, 3):
                (event,) = daemon.inbox.fetch_pending(f"proj:a:issue-{n}")
                self.assertEqual(event.status, "pending")

    async def test_stop_runs_what_is_already_waiting(self):
        daemon = build_daemon_from_config(parse_config(self.raw(**self.LIMITS)))
        self.addCleanup(daemon.close)
        self._seq = 0
        stop = asyncio.Event()
        runs = asyncio.create_task(daemon.pool.run_forever(stop))
        await self.three_sde(daemon)
        stop.set()
        await self.settle()
        self.assertFalse(runs.done())
        self.probe.release("block-1", "block-2", "block-3")
        await asyncio.wait_for(runs, 5)
        self.assertEqual(self.probe.finished, ["block-1", "block-2", "block-3"])
        self.assertTrue(daemon.pool._idle_event.is_set())

    async def test_an_idle_pool_stops_promptly(self):
        daemon = build_daemon_from_config(parse_config(self.raw(**self.LIMITS)))
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        runs = asyncio.create_task(daemon.pool.run_forever(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(runs, 1)

    async def test_wait_until_idle_waits_for_inboxes_held_back_by_a_limit(self):
        async with self.daemon(**self.LIMITS) as daemon:
            await self.three_sde(daemon)
            with self.assertRaises(TimeoutError):
                await daemon.pool.wait_until_idle(timeout_seconds=0.1)
            self.probe.release("block-1", "block-2", "block-3")
            await self.idle(daemon)
            self.assertEqual(len(self.probe.finished), 3)

    async def test_a_wake_while_waiting_neither_duplicates_nor_moves_the_inbox(self):
        async with self.daemon(**self.LIMITS) as daemon:
            await self.three_sde(daemon)
            self.post(daemon, "proj:a:issue-2", "more", executor="sde")
            self.assertEqual(list(daemon.pool._queue), ["proj:a:issue-2", "proj:a:issue-3"])
            self.probe.release("block-1", "block-2", "block-3")
            await self.idle(daemon)
        self.assertEqual(self.probe.started, ["block-1", "block-2", "more", "block-3"])


class TestReload(PoolCase):
    def write(self, **limits):
        raw = self.raw(**limits)
        raw["channels"] = {"host": "127.0.0.1", "port": self.port}
        lines = []
        for section, table in raw.items():
            tables = {section: table} if section in ("runtime", "channels") else {
                f"{section}.{name}": item for name, item in table.items()}
            for name, item in tables.items():
                lines += [f"[{name}]", *(f"{k} = {json.dumps(v)}" for k, v in item.items())]
        self.path.write_text("\n".join(lines) + "\n")

    async def test_a_reload_applies_new_limits_and_keeps_waiting_inboxes(self):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        self.write(projects={"a": {**TASKS, "max_concurrency": 1}})
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        with contextlib.redirect_stderr(stderr):
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                for n in range(4):
                    self.post(daemon, f"proj:a:issue-{n}", f"block-{n}")
                await self.until(lambda: self.probe.active_texts() == ["block-0"])
                await self.settle()
                self.assertEqual(self.probe.active_texts(), ["block-0"])
                self.write(projects={"a": {**TASKS, "max_concurrency": 2}})
                # block-0 still occupies one project slot under the old generation, so the new one admits only block-1.
                await self.until(lambda: len(self.probe.active) == 2)
                await self.settle()
                self.assertEqual(self.probe.active_texts(), ["block-0", "block-1"])
                # Releasing the old generation's run frees its slot and wakes the new generation for block-2.
                self.probe.release("block-0")
                await self.until(lambda: self.probe.active_texts() == ["block-1", "block-2"])
                await self.settle()
                self.assertEqual(self.probe.active_texts(), ["block-1", "block-2"])
                # An unusable limit keeps the current generation.
                self.write(projects={"a": {**TASKS, "max_concurrency": 0}})
                await self.until(lambda: "projects.a.max_concurrency must be a positive integer" in stderr.getvalue())
                self.probe.release(*(f"block-{n}" for n in range(4)))
                await self.until(lambda: len(self.probe.finished) == 4)
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)
        self.assertEqual(Counter(self.probe.finished), Counter(f"block-{n}" for n in range(4)))
        self.assertEqual(self.probe.peak("project", "proj:a"), 2)

    async def test_a_reload_never_exceeds_global_max_concurrency(self):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        self.write(max_concurrency=2, projects={"a": TASKS})
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        with contextlib.redirect_stderr(stderr):
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                self.post(daemon, "proj:a:issue-0", "block-0")
                self.post(daemon, "proj:a:issue-1", "block-1")
                await self.until(lambda: len(self.probe.active) == 2)
                # Trigger an unrelated config reload while global concurrency is full (2/2).
                self.write(max_concurrency=2, projects={"a": TASKS, "b": {"inbox_id": "proj:b"}})
                await self.until(lambda: "[config] Reloaded" in stderr.getvalue())
                # New work arriving after reload must wait for an old-generation slot instead of running 3/2.
                self.post(daemon, "proj:b", "block-2")
                await self.settle()
                self.assertEqual(self.probe.active_texts(), ["block-0", "block-1"])
                self.probe.release("block-0")
                await self.until(lambda: self.probe.active_texts() == ["block-1", "block-2"])
                self.probe.release("block-1", "block-2")
                await self.until(lambda: len(self.probe.finished) == 3)
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)
        self.assertEqual(self.probe.peak("all", ""), 2)

    async def test_a_reload_counts_an_agents_old_runs_against_its_new_limit(self):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        base = self.raw
        self.raw = lambda writer, **limits: {**base(**limits), "agents": {
            "writer": {"executor": "sde", "max_concurrency": writer}, "other": {"executor": "sde"}}}
        self.write(writer=1, projects={"a": TASKS})
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        with contextlib.redirect_stderr(stderr):
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                for n in range(3):
                    self.post(daemon, f"proj:a:{n}", f"block-{n}", executor="writer")
                await self.until(lambda: self.probe.active_texts() == ["block-0"])
                self.write(writer=2, projects={"a": TASKS})
                await self.until(lambda: len(self.probe.active) == 2)
                await self.settle()
                self.assertEqual(self.probe.active_texts(), ["block-0", "block-1"])
                # Another agent of the same driver has a slot, and the old Run's goes to the writer's next inbox.
                self.post(daemon, "proj:a:9", "other", executor="other")
                await self.until(lambda: "other" in self.probe.finished)
                self.probe.release("block-0")
                await self.until(lambda: self.probe.active_texts() == ["block-1", "block-2"])
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)

    @contextlib.asynccontextmanager
    async def reloading(self, agents, executors=None):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        base = self.raw
        raw = {"agents": agents, "executors": executors or {}}
        self.raw = lambda: {**base(projects={"a": TASKS}, executors=raw["executors"]), "agents": raw["agents"]}
        self.write()
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()

        async def reload(**changed):
            raw.update(changed)
            self.write()
            await self.until(lambda: "[config] Reloaded" in stderr.getvalue())

        with contextlib.redirect_stderr(stderr):
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                yield daemon, reload
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)
                self.raw = base

    async def test_a_reload_that_brings_the_first_limit_counts_the_runs_in_progress(self):
        plain = {"writer": {"executor": "sde"}, "other": {"executor": "sde"}}
        for name, changed, free in (
            ("agent", {"agents": {**plain, "writer": {"executor": "sde", "max_concurrency": 1}}}, "other"),
            ("driver", {"executors": {"sde": {"max_concurrency": 1}}}, "claude"),
        ):
            with self.subTest(name):
                self.setUp()
                async with self.reloading(plain) as (daemon, reload):
                    self.post(daemon, "proj:a:1", "block-1", executor="writer")
                    await self.until(lambda: self.probe.active_texts() == ["block-1"])
                    await reload(**changed)
                    self.post(daemon, "proj:a:2", "block-2", executor="writer")
                    self.post(daemon, "proj:a:3", "free", executor=free)
                    await self.until(lambda: "free" in self.probe.finished)
                    await self.settle()
                    self.assertEqual(self.probe.active_texts(), ["block-1"])
                    self.probe.release("block-1")
                    await self.until(lambda: self.probe.active_texts() == ["block-2"])

    async def test_the_second_agent_of_a_dispatch_counts_too(self):
        # Neither is limited when they run, so both use the slot taken for the first.
        plain = {"writer": {"executor": "sde"}, "other": {"executor": "claude"}}
        for name, changed in (
            ("agent", {"agents": {**plain, "writer": {"executor": "sde", "max_concurrency": 1}}}),
            ("driver", {"executors": {"sde": {"max_concurrency": 1}}}),
        ):
            with self.subTest(name):
                self.setUp()
                async with self.reloading(plain) as (daemon, reload):
                    self.post(daemon, "proj:a:1", "block-0", executor="other", key="k0")
                    await self.until(lambda: self.probe.active_texts() == ["block-0"])
                    self.post(daemon, "proj:a:1", "block-1", executor="writer", key="k1")
                    self.probe.release("block-0")
                    await self.until(lambda: self.probe.active_texts() == ["block-1"])
                    await reload(**changed)
                    self.post(daemon, "proj:a:2", "block-2", executor="writer")
                    self.post(daemon, "proj:a:3", "free", executor="other")
                    await self.until(lambda: "free" in self.probe.finished)
                    await self.settle()
                    self.assertEqual(self.probe.active_texts(), ["block-1"])
                    self.probe.release("block-1")
                    await self.until(lambda: self.probe.active_texts() == ["block-2"])

    async def test_lowering_global_limit_on_reload_waits_for_old_runs_to_drain(self):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        self.write(max_concurrency=3, projects={"a": TASKS})
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        with contextlib.redirect_stderr(stderr):
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                for n in range(3):
                    self.post(daemon, f"proj:a:issue-{n}", f"block-{n}")
                await self.until(lambda: len(self.probe.active) == 3)
                self.write(max_concurrency=1, projects={"a": TASKS})
                await self.until(lambda: "[config] Reloaded" in stderr.getvalue())
                self.post(daemon, "proj:a:issue-3", "block-3")
                self.probe.release("block-0", "block-1")
                await self.until(lambda: self.probe.active_texts() == ["block-2"])
                await self.settle()
                # Still 1 active (block-2 >= new limit 1), so block-3 must keep waiting.
                self.assertEqual(self.probe.active_texts(), ["block-2"])
                self.probe.release("block-2")
                await self.until(lambda: self.probe.active_texts() == ["block-3"])
                self.probe.release("block-3")
                await self.until(lambda: len(self.probe.finished) == 4)
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)

    async def test_a_failed_reload_keeps_the_current_limiter_limits(self):
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stderr = io.StringIO()
        self._seq = 0
        self.write(max_concurrency=1, projects={"a": TASKS})
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        with contextlib.redirect_stderr(stderr), socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            serving = asyncio.create_task(serve_configured(self.path, config, daemon, stop, reload_interval=0.02,
                                                           outbox_interval_seconds=0.02))
            try:
                self.post(daemon, "proj:a:issue-0", "block-0")
                self.post(daemon, "proj:a:issue-1", "block-1")
                await self.until(lambda: self.probe.active_texts() == ["block-0"])
                old_port = self.port
                self.port = occupied.getsockname()[1]
                self.write(max_concurrency=4, projects={"a": TASKS})
                await self.until(lambda: "Cannot activate settings" in stderr.getvalue())
                self.port = old_port
                await self.settle()
                # Limit stayed at 1 because the reload failed to bind the new port.
                self.assertEqual(self.probe.active_texts(), ["block-0"])
                self.probe.release("block-0", "block-1")
                await self.until(lambda: len(self.probe.finished) == 2)
            finally:
                self.probe.release(*self.probe.gates)
                stop.set()
                await asyncio.wait_for(serving, 10)
        self.assertEqual(self.probe.peak("all", ""), 1)


class TestBuildDaemon(PoolCase):
    async def test_build_daemon_takes_the_same_limits(self):
        from agent_runtime.app.concurrency import ConcurrencyLimits

        executors = {name: self.probe.executor(name) for name in ("claude", "sde")}
        daemon = build_daemon(
            db_path=":memory:", memory_root=self.root / "mem", executors_factory=lambda _inbox: executors,
            global_default_executor="claude", limits=ConcurrencyLimits(projects={"a": 1}, executors={"sde": 1}),
            project_for=lambda inbox_id: "a" if inbox_id.startswith("proj:a") else None,
        )
        self.addCleanup(daemon.close)
        self._seq = 0
        stop = asyncio.Event()
        runs = asyncio.create_task(daemon.pool.run_forever(stop))
        self.post(daemon, "proj:a:1", "block-a1")
        self.post(daemon, "proj:a:2", "a2")
        self.post(daemon, "x", "block-x", executor="sde")
        self.post(daemon, "y", "y", executor="sde")
        self.post(daemon, "z", "z")
        await self.until(lambda: "z" in self.probe.finished)
        await self.settle()
        self.assertEqual(self.probe.active_texts(), ["block-a1", "block-x"])
        self.probe.release("block-a1", "block-x")
        await self.idle(daemon)
        stop.set()
        await asyncio.wait_for(runs, 5)
        self.assertEqual(sorted(self.probe.finished), ["a2", "block-a1", "block-x", "y", "z"])
        self.assertEqual(self.probe.peak("executor", "sde"), 1)


class TestLimitKeys(PoolCase):
    async def test_a_detached_run_counts_for_the_executor_that_resumes_it(self):
        daemon = build_daemon_from_config(parse_config(
            self.raw(executors={"sde": {"max_concurrency": 1}}, projects={"a": {**TASKS, "max_concurrency": 1}})))
        self.addCleanup(daemon.close)
        self._seq = 0
        assert daemon.open_runs is not None
        daemon.open_runs.open(session_id="s", inbox_id="proj:a:1", executor_id="sde", event_ids=[], code_version="")
        self.post(daemon, "proj:a:1", "new")
        keys = daemon.pool._limit_keys("proj:a:1")
        self.assertEqual((keys.project, keys.executor), ("a", "sde"))

    async def test_a_wake_re_reads_which_executor_a_waiting_inbox_needs(self):
        async with self.daemon(executors={"sde": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            assert daemon.open_runs is not None
            self.post(daemon, "proj:a:1", "block-sde", executor="sde")
            await self.until(lambda: self.probe.active_texts() == ["block-sde"])
            # As after a reload: the previous generation still runs proj:a:2 on sde, then hands the inbox over.
            daemon.open_runs.open(session_id="s", inbox_id="proj:a:2", executor_id="sde", event_ids=[],
                                  code_version="")
            self.post(daemon, "proj:a:2", "light")
            await self.settle()
            self.assertEqual(self.probe.active_texts(), ["block-sde"])
            daemon.open_runs.discard("s")
            daemon.pool.wake("proj:a:2")
            await self.until(lambda: "light" in self.probe.finished)
            self.assertEqual(self.probe.active_texts(), ["block-sde"])

    async def test_a_close_request_needs_no_executor_quota(self):
        async with self.daemon(executors={"claude": {"max_concurrency": 1}}, projects={"a": TASKS}) as daemon:
            self.post(daemon, "proj:a:1", "first", key="task:1")
            await self.idle(daemon)
            self.post(daemon, "proj:a:2", "block-2")
            await self.until(lambda: self.probe.active_texts() == ["block-2"])
            self.post(daemon, "proj:a:1", "close", key="task:1", action="close_session")
            await self.until(lambda: len(daemon.inbox.list_events(inbox_id="proj:a:1", status="done", limit=5)) == 2)
            self.assertEqual(self.probe.active_texts(), ["block-2"])


class TestAgents(PoolCase):
    # Agents are picked by the inbox bindings and counted on their own; a limit on the [executors.<id>] they share caps
    # them together.
    BINDINGS = [
        {"pattern": "proj:a", "default_agent": "claude"},
        {"pattern": "proj:a:.+", "default_agent": "writer", "agents": ["writer", "fixer", "claude"],
         "rules": [{"tags": ["fix"], "agent": "fixer"}]},
    ]

    def raw(self, **limits):
        raw = super().raw(executors={"sde": {"max_concurrency": 1}},
                          projects={"a": {**TASKS, "inbox_bindings": self.BINDINGS}}, **limits)
        raw["agents"] = {"writer": {"executor": "sde"}, "fixer": {"executor": "sde"}}
        return raw

    async def test_agents_on_one_executor_share_its_limit(self):
        async with self.daemon() as daemon:
            # One by the binding's default, one by its rules, one by name.
            self.post(daemon, "proj:a:1", "block-1")
            self.post(daemon, "proj:a:2", "block-2", tags=["fix"])
            self.post(daemon, "proj:a:3", "block-3", executor="writer")
            self.post(daemon, "proj:a", "main")
            self.post(daemon, "proj:a:4", "other", executor="claude")
            await self.until(lambda: {"main", "other"} <= set(self.probe.finished))
            for n, agent in ((1, "writer"), (2, "fixer"), (3, "writer")):
                await self.until(lambda: self.probe.active == [(agent, f"block-{n}")])
                await self.settle()
                self.assertEqual(self.probe.active, [(agent, f"block-{n}")])
                self.probe.release(f"block-{n}")
            await self.idle(daemon)

    async def test_a_refused_agent_needs_no_slot(self):
        async with self.daemon() as daemon:
            self.post(daemon, "proj:a:1", "block-1")
            await self.until(lambda: self.probe.active_texts() == ["block-1"])
            self.post(daemon, "proj:a:2", "refused", executor="sde")
            await self.until(lambda: daemon.inbox.list_events(inbox_id="proj:a:2", status="failed", limit=5))
            self.assertEqual(self.probe.active_texts(), ["block-1"])
            self.assertEqual(self.probe.started, ["block-1"])

    async def test_work_handed_on_inside_an_inbox_waits_for_the_next_agents_executor(self):
        probe = self.probe

        class _Forwarding:
            executor_id = "claude"

            async def execute(self, events, session, caps):
                probe.started.append(events[-1].payload["text"])
                caps.forward_to_inbox(events[0].inbox_id, "block-relayed", target_agent="writer")
                return Silent()

        self.enterContext(patch(
            "agent_runtime.app.main.LocalCLIExecutor",
            lambda *, default_executor, **_: (
                _Forwarding() if default_executor == "claude" else probe.executor(default_executor)
            ),
        ))
        async with self.daemon() as daemon:
            self.post(daemon, "proj:a:1", "block-1")
            await self.until(lambda: self.probe.active_texts() == ["block-1"])
            self.post(daemon, "proj:a:2", "relay", executor="claude", key="task:2")
            await self.until(lambda: "relay" in self.probe.started)
            await self.settle()
            self.assertEqual(self.probe.active, [("writer", "block-1")])
            self.probe.release("block-1")
            await self.until(lambda: self.probe.active == [("writer", "block-relayed")])
            self.probe.release("block-relayed")
            await self.idle(daemon)
        self.assertEqual(self.probe.finished, ["block-1", "block-relayed"])

    async def test_a_detached_run_counts_for_its_agents_executor(self):
        daemon = build_daemon_from_config(parse_config(self.raw()))
        self.addCleanup(daemon.close)
        assert daemon.open_runs is not None
        daemon.open_runs.open(session_id="s", inbox_id="proj:a:1", executor_id="fixer", event_ids=[], code_version="")
        keys = daemon.pool._limit_keys("proj:a:1")
        self.assertEqual((keys.project, keys.executor), ("a", "sde"))

    async def test_role_agents_count_for_the_executor_they_share(self):
        raw = super().raw(projects={"a": {"inbox_id": ["proj:a", "proj:a:.+"], "inbox_bindings": [
            {"pattern": "proj:a", "default_agent": "steward"}, {"pattern": "proj:a:.+", "default_agent": "dev"},
        ]}})
        raw["executors"]["claude-code"] = {"harness": "claude-code", "max_concurrency": 1}
        raw["agents"] = {"steward": {"role": "steward", "executor": "claude-code"},
                         "dev": {"role": "sde", "executor": "claude-code", "review": False}}
        daemon = build_daemon_from_config(parse_config(raw))
        self.addCleanup(daemon.close)
        self._seq = 0
        for inbox_id in ("proj:a", "proj:a:1"):
            self.post(daemon, inbox_id, "t")
            keys = daemon.pool._limit_keys(inbox_id)
            self.assertEqual((keys.project, keys.executor), ("a", "claude-code"))
        limiter = daemon.pool.limiter
        self.assertTrue(limiter.try_acquire("proj:a", project_key="a", executor_key="claude-code"))
        self.assertFalse(limiter.available("proj:a:1", project_key="a", executor_key="claude-code"))

    def agent_raw(self, *, driver=None, **agents):
        raw = super().raw(projects={"a": {"inbox_id": ["proj:a", "proj:a:.+"], "inbox_bindings": [
            {"pattern": "proj:a", "default_agent": "steward"}, {"pattern": "proj:a:.+", "default_agent": "sde"},
        ]}})
        # The agent "sde", not an executor of that name, is what these tests run.
        del raw["executors"]["sde"]
        raw["executors"]["claude-code"] = {"harness": "claude-code", **(driver or {})}
        raw["agents"] = {"steward": {"role": "steward", "executor": "claude-code", **agents.get("steward", {})},
                         "sde": {"role": "sde", "executor": "claude-code", "review": False, "interactive": False,
                                 **agents.get("sde", {})}}
        return raw

    @contextlib.asynccontextmanager
    async def agent_daemon(self, **options):
        for executor in ("SDEExecutor", "StewardExecutor"):
            self.enterContext(patch(f"agent_runtime.app.main.{executor}",
                                    lambda *, profile, **_: self.probe.executor(profile.name)))
        raw = self.agent_raw(**options)
        with patch.object(self, "raw", lambda **_: raw):
            async with self.daemon() as daemon:
                yield daemon

    async def test_a_full_agent_does_not_hold_back_another_one_on_the_same_driver(self):
        async with self.agent_daemon(sde={"max_concurrency": 1}) as daemon:
            self.post(daemon, "proj:a:1", "block-1")
            self.post(daemon, "proj:a:2", "block-2")
            await self.until(lambda: self.probe.active == [("sde", "block-1")])
            self.post(daemon, "proj:a", "block-front-desk")
            await self.until(lambda: ("steward", "block-front-desk") in self.probe.active)
            await self.settle()
            self.assertEqual(self.probe.active, [("sde", "block-1"), ("steward", "block-front-desk")])
            self.probe.release("block-1")
            await self.until(lambda: ("sde", "block-2") in self.probe.active)
            self.probe.release("block-2", "block-front-desk")
            await self.idle(daemon)
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_an_agent_limit_counts_across_inboxes_up_to_its_own_number(self):
        async with self.agent_daemon(sde={"max_concurrency": 2}) as daemon:
            for n in range(4):
                self.post(daemon, f"proj:a:{n}", f"block-{n}")
            await self.until(lambda: len(self.probe.active) == 2)
            self.post(daemon, "proj:a", "front-desk")
            await self.until(lambda: "front-desk" in self.probe.finished)
            self.assertEqual(self.probe.peak("executor", "sde"), 2)
            self.probe.release(*(f"block-{n}" for n in range(4)))
            await self.idle(daemon)
        self.assertEqual(self.probe.peak("executor", "sde"), 2)
        self.assertEqual(len(self.probe.finished), 5)

    async def test_a_limit_set_on_the_driver_still_caps_its_agents_together(self):
        async with self.agent_daemon(driver={"max_concurrency": 2}, sde={"max_concurrency": 1}) as daemon:
            self.post(daemon, "proj:a:1", "block-1")
            self.post(daemon, "proj:a", "block-front-desk")
            await self.until(lambda: len(self.probe.active) == 2)
            # The driver is full although the second front desk inbox's agent has no limit of its own.
            self.post(daemon, "proj:a:2", "other", executor="steward")
            await self.settle()
            self.assertNotIn("other", self.probe.started)
            self.probe.release("block-1")
            await self.until(lambda: "other" in self.probe.finished)
            self.probe.release("block-front-desk")
            await self.idle(daemon)

    async def test_one_inbox_with_two_agents_of_a_driver_waits_for_the_limited_one(self):
        async with self.agent_daemon(sde={"max_concurrency": 1}) as daemon:
            self.post(daemon, "proj:a:1", "block-1")
            await self.until(lambda: self.probe.active == [("sde", "block-1")])
            self.post(daemon, "proj:a:2", "by-steward", executor="steward", key="k1")
            self.post(daemon, "proj:a:2", "block-2", key="k2")
            await self.until(lambda: "by-steward" in self.probe.finished)
            await self.settle()
            self.assertEqual(self.probe.active, [("sde", "block-1")])
            self.probe.release("block-1")
            await self.until(lambda: self.probe.active == [("sde", "block-2")])
            self.probe.release("block-2")
            await self.idle(daemon)
        self.assertEqual(self.probe.peak("executor", "sde"), 1)

    async def test_keys_name_the_agent_and_its_driver(self):
        daemon = build_daemon_from_config(parse_config(self.agent_raw(sde={"max_concurrency": 1})))
        self.addCleanup(daemon.close)
        self._seq = 0
        for inbox_id, agent in (("proj:a", "steward"), ("proj:a:1", "sde")):
            self.post(daemon, inbox_id, "t")
            keys = daemon.pool._limit_keys(inbox_id)
            self.assertEqual((keys.project, keys.agent, keys.executor), ("a", agent, "claude-code"))

    async def test_events_that_run_no_agent_need_no_slot(self):
        daemon = build_daemon_from_config(parse_config(self.raw()))
        self.addCleanup(daemon.close)
        sessions = daemon.sessions
        sessions.save(SessionRef(session_id="s", inbox_id="proj:a:1", executor_id="writer", status="active"))
        sessions.save(SessionRef(
            session_id="held:1", inbox_id="proj:a:1", executor_id="writer", status="suspended",
            pending_request_id="req", forward=ForwardSpec(target_inbox="proj:a:1", held=HeldReturn(
                agent_id="writer", external_id="r", payload={}, reason="no")),
        ))

        def expected(source, session_id, **payload):
            event = Event(inbox_id="proj:a:1", source=source, external_id="x", payload=payload, session_id=session_id)
            return AgentWorker.expected_executor_id(event, sessions, project_default=None, global_default="claude")

        self.assertEqual(expected("test", "s"), "writer")
        self.assertIsNone(expected(RETURN_CANCELLED_SOURCE, "s", delegation_id="d"))
        self.assertIsNone(expected(CLOSE_SOURCE, "s"))
        self.assertIsNone(expected("test", None, approval_request_id="req", approved=True))

    async def test_a_second_piece_is_routed_as_the_task_of_its_own_it_becomes(self):
        daemon = build_daemon_from_config(parse_config(self.raw()))
        self.addCleanup(daemon.close)
        daemon.sessions.save(SessionRef(
            session_id="s", inbox_id="proj:a:1", executor_id="claude", status="active", correlation_key="task:1",
            forward=ForwardSpec(target_inbox="proj:a:1", return_to="proj:a", delegation_id="one"),
        ))
        self._seq = 0
        piece = Event(inbox_id="proj:a:1", source=FORWARD_SOURCE, external_id="two", correlation_key="task:1",
                      payload={"text": "t", "return_to": "proj:a", "delegation_id": "two", "hop_count": 1})
        self.assertEqual(AgentWorker.expected_executor_id(
            piece, daemon.sessions, project_default="writer", global_default="claude"), "writer")


class TestAgentLimitConfig(unittest.TestCase):
    def config(self, limit, **runtime):
        return parse_config({
            "runtime": runtime,
            "executors": {"cli": {"command": "x", "max_concurrency": limit},
                          "claude-code": {"harness": "claude-code", "max_concurrency": limit}},
            "agents": {"a": {"executor": "cli"}, "b": {"executor": "cli"},
                       "steward": {"role": "steward", "executor": "claude-code"}},
        })

    def test_a_driver_limit_is_not_copied_to_its_agents(self):
        limits = concurrency_limits(self.config(2))
        self.assertEqual((limits.executors, limits.agents), ({"cli": 2, "claude-code": 2}, {}))

    def test_agents_carry_limits_of_their_own(self):
        cfg = parse_config({
            "executors": {"cli": {"command": "x", "max_concurrency": 3}, "claude-code": {"harness": "claude-code"}},
            "agents": {"cli": {"executor": "cli", "max_concurrency": 2}, "ops": {"executor": "cli", "max_concurrency": 1},
                       "sde": {"role": "sde", "executor": "claude-code", "review": False, "max_concurrency": 2},
                       "steward": {"role": "steward", "executor": "claude-code"}},
        })
        limits = concurrency_limits(cfg)
        self.assertEqual((limits.executors, limits.agents), ({"cli": 3}, {"cli": 2, "ops": 1, "sde": 2}))

    def test_legacy_executors_keep_their_limit_as_an_executor_limit(self):
        cfg = parse_config({"executors": {"sde": {"role": "sde", "review": False, "max_concurrency": 2}}})
        limits = concurrency_limits(cfg)
        self.assertEqual((limits.executors, limits.agents), ({"sde": 2}, {}))

    def test_an_invalid_agent_limit_is_rejected_with_its_path(self):
        for bad in (0, -1, True, "2", 1.5):
            for agent in ({"executor": "cli"}, {"role": "steward", "executor": "claude-code"}):
                with self.subTest(bad=bad, agent=agent), self.assertRaisesRegex(
                        ValueError, "^agents.x.max_concurrency must be a positive integer"):
                    parse_config({"executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
                                  "agents": {"x": {**agent, "max_concurrency": bad}}})

    def test_an_agent_limit_above_the_global_one_loads_with_a_warning(self):
        with self.assertLogs("agent_runtime.app.config", level="WARNING") as logs:
            parse_config({"runtime": {"max_concurrency": 2}, "executors": {"cli": {"command": "x"}},
                          "agents": {"ops": {"executor": "cli", "max_concurrency": 3}}})
        self.assertEqual([r.getMessage().split(" = ")[0] for r in logs.records], ["agents.ops.max_concurrency"])

    def test_a_limit_above_the_global_one_warns_once_for_each_executor(self):
        with self.assertLogs("agent_runtime.app.config", level="WARNING") as logs:
            self.config(3, max_concurrency=2)
        messages = [r.getMessage().split(" = ")[0] for r in logs.records]
        self.assertEqual(sorted(messages), ["executors.claude-code.max_concurrency", "executors.cli.max_concurrency"])

    def test_an_invalid_limit_of_a_pure_executor_is_rejected(self):
        for bad in (0, True, "2"):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                    ValueError, "^executors.claude-code.max_concurrency must be a positive integer"):
                parse_config({"executors": {"claude-code": {"harness": "claude-code", "max_concurrency": bad}}})


if __name__ == "__main__":
    unittest.main()
