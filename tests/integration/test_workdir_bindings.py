import asyncio
import socket
import time
import unittest
from pathlib import Path

from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import SessionRef
from tests.support.project_inbox_rules import _DispatchCase, project_table
from tests.support.workdir_bindings import OPS


HOLD_LOGGER = "agent_runtime.app.projects"


def binding_table(pattern, workdir=None, agent="cli", **extra):
    lines = ["[[runtime.inbox_bindings]]", f'pattern = "{pattern}"', f'default_agent = "{agent}"']
    if workdir is not None:
        lines.append(f'workdir = "{workdir}"')
    lines += [f"{key} = {value}" for key, value in extra.items()]
    return "\n".join(lines) + "\n"


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

    async def test_events_that_name_a_target_wait_for_a_missing_workdir_too(self):
        gone = self.root / "gone"
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        path = self.write_config(self.project + binding_table(OPS, gone))
        waiting = [
            ({"text": "same", "explicit_workdir": str(gone)}, "task:1"),
            ({"text": "spelled", "explicit_workdir": f"{elsewhere}/../gone"}, "task:2"),
            ({"text": "review", "review_of": "task:1"}, "task:3"),
            ({"text": "answer"}, "task:4"),
            ({"text": "approved", "approval_request_id": "req-1", "decision": "approve"}, "task:4"),
            ({"text": "close", "action": "close_session"}, "task:4"),
        ]
        daemon = build_daemon_from_config(load_config(path))
        daemon.sessions.save(SessionRef(
            session_id="s1", inbox_id=OPS, correlation_key="task:4", executor_id="cli", status="suspended",
            pending_request_id="req-1",
        ))
        daemon.close()
        with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
            await self.dispatch(path, [
                (OPS, {"text": "moved", "explicit_workdir": str(elsewhere)}, "task:0"),
                *((OPS, payload, key) for payload, key in waiting),
            ])
        self.assertEqual(self.statuses(OPS), [("done", 1)] + [("pending", 0)] * len(waiting))
        self.assertEqual(len(logs.records), len(waiting))
        [refusal] = [c for (c,) in self.rows("SELECT content FROM outbox")]
        self.assertIn("is not this inbox's workdir", refusal)
        self.assertEqual(self.cli_runs(), [])
        self.assertFalse(gone.exists())

        gone.mkdir()
        await self.dispatch(path, [])
        self.assertNotIn("pending", [status for status, _ in self.statuses(OPS)])
        runs = self.cli_runs()
        self.assertEqual({r["cwd"] for r in runs}, {str(gone)})
        self.assertLessEqual({"same", "spelled", "review"}, {r["text"] for r in runs})
        self.assertEqual(self.rows("SELECT status FROM sessions WHERE session_id = 's1'"), [("closed",)])

    async def test_another_spelling_of_the_workdir_is_the_workdir(self):
        (self.root / "link").symlink_to(self.root)
        gone = self.root / "caf\u00e9"
        spellings = ["CAF\u00c9", "cafe\u0301", "link/Caf\u00e9"]
        path = self.write_config(self.project + binding_table(OPS, gone))
        with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
            await self.dispatch(path, [(OPS, {"text": name.replace("/", "_"), "explicit_workdir": str(self.root / name)}, f"task:{name}")
                                       for name in spellings])
        self.assertEqual(self.statuses(OPS), [("pending", 0)] * len(spellings))
        self.assertEqual(len(logs.records), len(spellings))
        self.assertEqual(self.outbox(), {})

        gone.mkdir()
        # Where letter case does matter, the other spelling is another directory once this one is there.
        same = [name for name in spellings if (self.root / name).is_dir()]
        await self.dispatch(path, [])
        self.assertEqual([status for status, _ in self.statuses(OPS)], ["done"] * len(spellings))
        self.assertEqual(sorted(r["text"] for r in self.cli_runs()), sorted(n.replace("/", "_") for n in same))
        self.assertTrue(all(Path(r["cwd"]).samefile(gone) for r in self.cli_runs()))
        self.assertEqual(len([c for (c,) in self.rows("SELECT content FROM outbox") if "refused" in c]),
                         len(spellings) - len(same))

    async def test_a_directory_name_no_path_can_have_is_refused(self):
        for workdir in (self.ops, self.root / "gone"):
            with self.subTest(exists=workdir.is_dir()):
                path = self.write_config(self.project + binding_table(OPS, workdir))
                await asyncio.wait_for(self.dispatch(
                    path, [(OPS, {"text": "odd", "explicit_workdir": "a\0b"}, None)]), timeout=60)
                self.assertEqual(self.statuses(OPS)[-1], ("done", 1))
                self.assertIn("is not this inbox's workdir", self.rows("SELECT content FROM outbox ORDER BY seq")[-1][0])

    async def test_an_inbox_with_an_unusable_id_waits_and_takes_no_worker(self):
        path = self.write_config(self.project.replace('"proj:p"', '["proj:p", "proj:p:.+"]')
                                 + binding_table("sys:ops(:.+)?", self.ops, checkout_access='"readonly"'))
        bad = ["sys:ops:a/b", "sys:ops:a\\b", "sys:ops:..", "sys:ops:" + "x" * 300, "proj:p:a/b", "none/none"]
        with self.assertLogs("agent_runtime.app.main", level="WARNING") as logs:
            # More of them than there are workers, then inboxes that must still run.
            daemon = await asyncio.wait_for(self.dispatch(path, [
                *((inbox_id, {"text": "bad"}, "task:1") for inbox_id in bad),
                ("sys:ops:good", {"text": "good"}, "task:1"),
            ]), timeout=60)
        for inbox_id in bad:
            with self.subTest(inbox_id=inbox_id[:20]):
                self.assertEqual(self.statuses(inbox_id), [("pending", 0)])
                self.assertEqual(len([r for r in logs.records if repr(inbox_id) in r.getMessage()]), 1)
        self.assertEqual({r.levelname for r in logs.records}, {"WARNING"})
        self.assertEqual([r["text"] for r in self.cli_runs()], ["good"])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])
        self.assertIsNotNone(daemon)

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
