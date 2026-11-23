import asyncio
import os
import socket
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from tests.test_project_inbox_rules import _DispatchCase, project_table

HOLD_LOGGER = "agent_runtime.app.projects"
OPS = "sys:hb-ops"


def binding_table(pattern, workdir=None, agent="cli", **extra):
    lines = ["[[runtime.inbox_bindings]]", f'pattern = "{pattern}"', f'default_agent = "{agent}"']
    if workdir is not None:
        lines.append(f'workdir = "{workdir}"')
    lines += [f"{key} = {value}" for key, value in extra.items()]
    return "\n".join(lines) + "\n"


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.ops = self.root / "ops"
        self.ops.mkdir()
        self.checkout = self.root / "repo"
        (self.checkout / "sub").mkdir(parents=True)

    def parse(self, *bindings, project=None, base_dir=None):
        raw = {
            "runtime": {"inbox_bindings": list(bindings)},
            "executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
            "agents": {"ops": {"executor": "cli"}, "asker": {"executor": "cli"}, "steward": {"role": "steward", "executor": "claude-code"},
                       "sde": {"role": "sde", "executor": "claude-code", "review": False},
                       "reviewer": {"role": "code-reviewer", "executor": "claude-code"}},
            "projects": {"p": {"repo": "o/r", "checkout": str(self.checkout), "worktree_backend": "git",
                               **(project or {})}},
        }
        return parse_config(raw, base_dir=base_dir)

    def binding(self, pattern=OPS, **extra):
        return {"pattern": pattern, "default_agent": "ops", "workdir": str(self.ops), **extra}

    def refused(self, message, *bindings, **kwargs):
        with self.assertRaisesRegex(ValueError, message):
            self.parse(*bindings, **kwargs)

    def test_a_binding_is_read_with_its_workdir(self):
        [bound] = self.parse(self.binding(agents=["ops", "asker"],
                                          rules=[{"tags": ["ask"], "agent": "asker"}])).runtime.inbox_bindings
        self.assertEqual((bound.workdir, bound.readonly, bound.binding.default_agent, bound.binding.agents),
                         (str(self.ops), False, "ops", ("ops", "asker")))
        self.assertEqual([rule.agent for rule in bound.binding.rules], ["asker"])
        self.assertTrue(bound.rule.matches(OPS))
        self.assertEqual(self.parse().runtime.inbox_bindings, ())

    def test_the_workdir_is_required(self):
        for raw in ({"pattern": OPS, "default_agent": "ops"}, self.binding(workdir=""), self.binding(workdir="  "),
                    self.binding(workdir=3)):
            with self.subTest(raw=raw):
                self.refused(r"runtime\.inbox_bindings\[0\]: workdir must be a non-empty string", raw)

    def test_home_and_relative_workdirs_are_resolved(self):
        with patch.dict(os.environ, {"HOME": str(self.root)}):
            cfg = self.parse(self.binding(workdir="~/ops"), self.binding("user:main", workdir="other"),
                             base_dir=self.root / "conf")
        self.assertEqual([b.workdir for b in cfg.runtime.inbox_bindings],
                         [str(self.ops), str(self.root / "conf" / "other")])
        self.assertFalse((self.root / "conf").exists())

    def test_two_bindings_may_not_write_one_workdir(self):
        link = self.root / "link"
        link.symlink_to(self.ops)
        for other in (self.ops, link, f"{self.ops}/../ops"):
            with self.subTest(other=str(other)):
                self.refused("same writable workdir", self.binding(), self.binding("user:main", workdir=str(other)))

    def test_a_workdir_that_does_not_exist_yet_is_compared_too(self):
        missing = str(self.root / "later")
        self.refused("same writable workdir", self.binding(workdir=missing), self.binding("user:main", workdir=missing))

    def test_readonly_bindings_may_share_a_workdir(self):
        cfg = self.parse(self.binding(checkout_access="readonly"),
                         self.binding("user:main", checkout_access="readonly"),
                         self.binding("sys:writer"))
        self.assertEqual([b.readonly for b in cfg.runtime.inbox_bindings], [True, True, False])
        self.refused("checkout_access must be", self.binding(checkout_access="write"))

    def test_a_pattern_of_many_inboxes_needs_a_readonly_workdir(self):
        self.refused("more than one inbox", self.binding("sys:ops(:.+)?"))
        self.parse(self.binding("sys:ops(:.+)?", checkout_access="readonly"))

    def test_a_writable_workdir_is_not_a_projects_checkout(self):
        for workdir in (self.checkout, self.checkout / "sub"):
            with self.subTest(workdir=str(workdir)):
                self.refused("checkout of projects.p", self.binding(workdir=str(workdir)))
                self.parse(self.binding(workdir=str(workdir), checkout_access="readonly"))
        self.parse(self.binding(workdir=str(self.root)))

    def test_a_binding_may_not_cover_a_projects_inbox(self):
        for pattern, inbox_id in (
            ("proj:p", None), ("proj:.+", None), (OPS, ["proj:p", "sys:.+"]), ("sys:a.*", ["proj:p", "sys:.*b"]),
            (OPS, OPS),
        ):
            with self.subTest(pattern=pattern, inbox_id=inbox_id):
                self.refused(r"runtime\.inbox_bindings", self.binding(pattern, checkout_access="readonly"),
                             project={"inbox_id": inbox_id} if inbox_id else None)
        self.parse(self.binding("sys:.+", checkout_access="readonly"), project={"inbox_id": ["proj:p", "proj:p:.+"]})

    def test_other_mistakes_are_rejected_like_in_project_bindings(self):
        for raw, message in (
            (self.binding(default_agent="ghost"), "default_agent 'ghost' is not declared"),
            (self.binding(agents=["asker"]), "default_agent 'ops' is not in agents"),
            (self.binding(colour="red"), "colour not supported"),
            (self.binding(pattern="sys:("), "invalid pattern"),
            (self.binding(pattern="sys/ops"), "sys/ops"),
            (self.binding(rules=[{"agent": "ops"}]), "needs source, event_type or tags"),
        ):
            with self.subTest(message=message):
                self.refused(message, raw)
        self.refused("listed twice", self.binding(), self.binding(workdir=str(self.root / "b")))

    def test_roles_work_in_a_project_and_are_refused(self):
        for agent in ("sde", "reviewer", "steward"):
            for extra in ({"default_agent": agent}, {"agents": ["ops", agent]},
                          {"rules": [{"tags": ["x"], "agent": agent}]}):
                with self.subTest(agent=agent, extra=extra):
                    self.refused(f"agent '{agent}'", self.binding(**extra))

    def test_project_bindings_take_no_workdir(self):
        self.refused("workdir not supported", project={"inbox_bindings": [
            {"pattern": "proj:p", "default_agent": "ops", "workdir": str(self.ops)}]})


class TestDispatch(_DispatchCase):
    def setUp(self):
        super().setUp()
        self.ops = self.root / "ops"
        self.ops.mkdir()
        self.project = project_table("p", "proj:p", self.repo)

    def statuses(self, inbox_id):
        return self.rows(f"SELECT status, attempts FROM event_inbox WHERE inbox_id = '{inbox_id}' ORDER BY seq")

    async def test_a_bound_inbox_runs_in_its_workdir_and_needs_no_project(self):
        path = self.write_config(self.project + binding_table(OPS, self.ops))
        await self.dispatch(path, [(OPS, {"text": "check"}, "task:1"), ("proj:p", {"text": "dev"}, "task:2")])
        runs = {r["text"]: Path(r["cwd"]) for r in self.cli_runs()}
        self.assertEqual(runs["check"], self.ops)
        self.assertTrue(runs["dev"].name.startswith("hb-"))
        self.assertIn(f"check ran in {self.ops}", self.outbox()[OPS])
        self.assertEqual(self.rows(f"SELECT COUNT(*) FROM workspaces WHERE inbox_id = '{OPS}'"), [(0,)])
        self.assert_primary_untouched()

    async def test_the_whole_binding_applies_to_a_bound_inbox(self):
        agents = f'[agents.other]\nexecutor = "cli"\n[agents.third]\nexecutor = "cli"\n'
        path = self.write_config(self.project + agents + binding_table(
            OPS, self.ops, agents='["cli", "other"]', rules='[{ tags = ["x"], agent = "other" }]'))
        await self.dispatch(path, [
            (OPS, {"text": "default"}, "task:1"),
            (OPS, {"text": "by-rule", "tags": ["x"]}, "task:2"),
            (OPS, {"text": "refused", "agent_id": "third"}, "task:3"),
        ])
        self.assertEqual({r["text"]: r["cwd"] for r in self.cli_runs()},
                         {"default": str(self.ops), "by-rule": str(self.ops)})
        self.assertEqual(sorted(self.rows("SELECT correlation_key, executor_id FROM sessions")),
                         [("task:1", "cli"), ("task:2", "other")])
        self.assertEqual([status for status, _ in self.statuses(OPS)], ["done", "done", "failed"])

    async def test_an_event_cannot_move_a_bound_inbox_to_another_directory(self):
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        path = self.write_config(self.project + binding_table(OPS, self.ops))
        await self.dispatch(path, [
            (OPS, {"text": "moved", "explicit_workdir": str(elsewhere)}, "task:1"),
            (OPS, {"text": "same", "explicit_workdir": str(self.ops)}, "task:2"),
        ])
        self.assertEqual([(r["text"], r["cwd"]) for r in self.cli_runs()], [("same", str(self.ops))])
        [refusal] = [c for (c,) in self.rows("SELECT content FROM outbox ORDER BY seq") if "refused" in c]
        self.assertIn(str(elsewhere), refusal)

    async def test_readonly_inboxes_share_a_workdir_at_the_same_time(self):
        path = self.write_config(
            self.project + binding_table("sys:ops(:.+)?", self.ops, checkout_access='"readonly"'), barrier_n=2)
        await self.dispatch(path, [("sys:ops:a", {"text": "a"}, "task:1"), ("sys:ops:b", {"text": "b"}, "task:1")])
        self.assertEqual({r["text"]: r["cwd"] for r in self.cli_runs()}, {"a": str(self.ops), "b": str(self.ops)})

    async def test_an_inbox_without_a_binding_still_waits(self):
        path = self.write_config(self.project + binding_table(OPS, self.ops))
        with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
            await self.dispatch(path, [("sys:other", {"text": "orphan"}, "task:1")])
        self.assertIn("runtime.inbox_bindings", logs.records[0].getMessage())
        self.assertEqual(self.statuses("sys:other"), [("pending", 0)])
        self.assertEqual(self.cli_runs(), [])

    async def test_an_inbox_whose_workdir_is_missing_waits_and_nothing_is_created(self):
        gone = self.root / "gone"
        path = self.write_config(self.project + binding_table(OPS, gone))
        with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
            await self.dispatch(path, [(OPS, {"text": "check"}, "task:1")])
        self.assertIn(str(gone), logs.records[0].getMessage())
        self.assertEqual(self.statuses(OPS), [("pending", 0)])
        self.assertFalse(gone.exists())

        gone.mkdir()
        await self.dispatch(path, [])
        self.assertEqual(self.statuses(OPS), [("done", 1)])
        self.assertEqual([r["cwd"] for r in self.cli_runs()], [str(gone)])

    async def test_a_reload_takes_a_complete_binding_only(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f'[channels]\nhost = "127.0.0.1"\nport = {port}\n' + self.project
        path = self.write_config(base)
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        before = path.read_text()

        async def wait_for(check):
            deadline = time.monotonic() + 30
            while not check():
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)

        stop = asyncio.Event()
        serving = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))
        try:
            with self.assertLogs(HOLD_LOGGER, level="WARNING"):
                daemon.handler.ingest(inbox_id=OPS, source="test", external_id="o1", payload={"text": "check"},
                                      correlation_key="task:1")
                await wait_for(lambda: bool(self.statuses(OPS)))
                path.write_text(before + binding_table(OPS))
                await asyncio.sleep(0.3)
                self.assertEqual(self.statuses(OPS), [("pending", 0)])
            path.write_text(before + binding_table(OPS, self.ops))
            await wait_for(lambda: self.statuses(OPS) == [("done", 1)])
        finally:
            stop.set()
            await serving
        self.assertEqual([r["cwd"] for r in self.cli_runs()], [str(self.ops)])


if __name__ == "__main__":
    unittest.main()
