from __future__ import annotations

import asyncio
import os
import tempfile
import tomllib
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.executor_cli import SDEExecutor, StewardExecutor
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, TerminalUnavailable
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit, PermissionDecision
from tests.support.steward import CONFIG, DaemonCase, MAIN, SDE


def orca_repo(path: Path, key: str | None) -> dict[str, Any]:
    return {"kind": "git", "path": str(path), "gitRemoteIdentity": {"canonicalKey": key} if key else None}


class WorkdirConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = self.root / "checkout"
        self.repo.mkdir()
        self.repos: list[dict[str, Any]] | Exception = []
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca.registered_repos", new=self.registered))
        self.listed = 0

    def registered(self) -> list[dict[str, Any]]:
        self.listed += 1
        if isinstance(self.repos, Exception):
            raise self.repos
        return self.repos

    def raw(self, *, checkout: bool = True, **steward: Any) -> dict[str, Any]:
        raw = tomllib.loads(CONFIG.format(root=self.root, repo=self.repo, command="claude"))
        raw["agents"]["steward"].update(steward)
        if not checkout:
            del raw["projects"]["x"]["checkout"]
        return raw

    def parse(self, **options: Any) -> Any:
        return parse_config(self.raw(**options), base_dir=self.root)

    def steward(self, config: Any) -> Any:
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.db.close)
        return daemon.pool._worker_factory(MAIN, "w")._executors["steward"]  # pyright: ignore

    def used(self, config: Any) -> str:
        found = self.steward(config)._directory(MAIN)  # pyright: ignore[reportPrivateUsage]
        return str(found)

    def test_workdir_then_checkout_then_the_checkout_orca_has_registered(self) -> None:
        self.assertEqual(self.used(self.parse(workdir="sub/dir")), str(self.root / "sub/dir"))
        self.assertEqual(self.parse(workdir="~/notes").roles["steward"].workdir, str(Path("~/notes").expanduser()))
        self.assertEqual(self.used(self.parse()), str(self.repo))
        self.assertEqual(self.listed, 0)
        registered = self.root / "registered"
        registered.mkdir()
        self.repos = [orca_repo(self.root / "a", None), orca_repo(self.root / "b", "github.com/acme/xy"),
                      {"kind": "folder", "path": str(self.root)}, orca_repo(registered, "github.com/Acme/X")]
        config = self.parse(checkout=False)
        # The whole project works in it, not only its Steward.
        self.assertEqual(config.projects["x"].checkout, str(registered))
        self.assertEqual(self.used(config), str(registered))

    def test_a_checkout_orca_does_not_know_is_an_error(self) -> None:
        cases: list[tuple[list[dict[str, Any]] | Exception, str]] = [
            ([orca_repo(self.repo, "github.com/acme/other")], "no repository acme/x is registered in Orca"),
            ([orca_repo(self.repo, "github.com/acme/x"), orca_repo(self.root, "github.com/acme/x")], "more than one"),
            ([orca_repo(self.root / "gone", "github.com/acme/x")], "gone"),
            (RuntimeError("Orca is not running"), "Orca is not running"),
        ]
        for self.repos, expected in cases:
            with self.subTest(expected), self.assertRaisesRegex(ValueError, "projects.x: checkout is not set.*" + expected):
                self.parse(checkout=False)

    def test_workdir_and_branch_are_checked(self) -> None:
        for key in ("workdir", "branch"):
            for value in ("", "  ", 3, ["a"]):
                with self.subTest(key=key, value=value), \
                        self.assertRaisesRegex(ValueError, f"agents.steward: {key} must be a non-empty string"):
                    self.parse(**{key: value})
        raw = self.raw()
        raw["agents"]["sde"]["workdir"] = "x"
        with self.assertRaisesRegex(ValueError, "agents.sde: workdir not supported for role 'sde'"):
            parse_config(raw)

    def test_the_branch_reaches_the_steward(self) -> None:
        self.assertEqual(self.steward(self.parse(branch="main"))._branch, "main")  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(self.steward(self.parse())._branch, "")  # pyright: ignore[reportPrivateUsage]

    def test_the_steward_gets_terminals_only_where_the_project_has_them(self) -> None:
        self.assertIsInstance(self.steward(self.parse())._terminal, OrcaCommandTerminal)  # pyright: ignore
        raw = self.raw()
        raw["projects"]["x"]["terminal_backend"] = "none"
        raw["runtime"]["db_path"] = str(self.root / "other.db")
        unavailable = self.steward(parse_config(raw))._terminal  # pyright: ignore[reportPrivateUsage]
        self.assertIsInstance(unavailable, TerminalUnavailable)
        self.assertIn("terminal_backend", unavailable.reason)


class DaemonReturnTests(DaemonCase):
    def setUp(self) -> None:
        super().setUp()
        self.repo.mkdir()
        self.steps = iter(())
        self.roles = []
        self.answers = []

        async def steward(executor, native, prompt, caps, tools):
            self.roles.append("steward")
            step = next(self.steps)
            for call in step.get("calls", []):
                self.answers.append(await tools.call(call["tool"], call["arguments"]))
            return {"session_id": "test-steward", "structured_output": step.get("output", {"action": "silent"})}

        async def developer(executor, events, session, caps, run):
            self.roles.append("sde")
            return Emit(messages=("implemented",))

        self.enterContext(patch.object(StewardExecutor, "_converse", new=steward))
        self.enterContext(patch.object(SDEExecutor, "_execute", new=developer))

    async def test_a_deferred_return_waits_for_its_own_approval(self) -> None:
        self.steps = iter(
            [
                {"calls": [{"tool": "forward_to_inbox", "arguments": {
                    "target_inbox": SDE, "text": "do it", "return_to": MAIN}}]},
                {"output": {"action": "reply", "message": "all done"}},
            ],
        )
        check = AutoModePermissionAdapter.check

        def gate(adapter: AutoModePermissionAdapter, tool: str, args: dict[str, Any]) -> PermissionDecision:
            if tool == "inbox.forward" and args["target_inbox"] == MAIN:
                return PermissionDecision("defer", "results need a look")
            return check(adapter, tool, args)

        with patch.object(AutoModePermissionAdapter, "check", gate):
            daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "reply_channel": "feishu"}))
            kinds = [m.metadata["kind"] for m in daemon.outbox.fetch_pending(limit=50)]
            self.assertEqual(kinds, ["approval_request"])
            [card] = daemon.outbox.fetch_pending(limit=50)
            daemon.handler.ingest(
                inbox_id=SDE, source="switchboard:feishu", external_id="decision",
                payload={"event_type": "approval.decided", "approved": True,
                         "approval_request_id": card.metadata["approval_request_id"]},
            )
            stop = asyncio.Event()
            pool = asyncio.create_task(daemon.pool.run_forever(stop))
            await daemon.pool.wait_until_idle(60)
            stop.set()
            await pool
        self.assertEqual(self.roles, ["steward", "sde", "steward"])
        answers = [(m.inbox_id, m.content) for m in daemon.outbox.fetch_pending(limit=50) if m.metadata["kind"] == "emit"]
        self.assertEqual(answers, [(MAIN, "all done")])

    async def test_a_withheld_result_ends_the_stewards_wait(self) -> None:
        self.steps = iter(
            [{"calls": [{"tool": "forward_to_inbox", "arguments": {
                "target_inbox": SDE, "text": "do it", "return_to": MAIN}}]}],
        )
        check = AutoModePermissionAdapter.check

        def gate(adapter: AutoModePermissionAdapter, tool: str, args: dict[str, Any]) -> PermissionDecision:
            if tool == "inbox.forward" and args["target_inbox"] == MAIN:
                return PermissionDecision("block", "results stay here")
            return check(adapter, tool, args)

        with patch.object(AutoModePermissionAdapter, "check", gate):
            daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "reply_channel": "feishu"}))
            [card] = daemon.outbox.fetch_pending(limit=50)
            self.assertNotIn("implemented", card.content)
            daemon.handler.ingest(
                inbox_id=SDE, source="switchboard:feishu", external_id="decision",
                payload={"event_type": "approval.decided", "approved": False,
                         "approval_request_id": card.metadata["approval_request_id"]},
            )
            stop = asyncio.Event()
            pool = asyncio.create_task(daemon.pool.run_forever(stop))
            await daemon.pool.wait_until_idle(60)
            stop.set()
            await pool
        self.assertEqual(self.roles, ["steward", "sde"])
        sent = {m.metadata["kind"]: m for m in daemon.outbox.fetch_pending(limit=50)}
        self.assertEqual(sorted(sent), ["approval_request", "return_withheld"])
        notice = sent["return_withheld"]
        self.assertEqual((notice.inbox_id, notice.metadata["reply_channel"], notice.metadata["correlation_key"]),
                         (MAIN, "feishu", "feishu:1"))
        self.assertNotIn("implemented", notice.content)
        [steward] = daemon.sessions.list_sessions(inbox_id=MAIN)
        self.assertEqual(steward.forward.pending_returns, ())

    async def test_direct_forward_answers_from_the_child(self) -> None:
        self.steps = iter(
            [{"calls": [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "do it"}}]}],
        )
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "reply_channel": "feishu"}))
        messages = [m for m in daemon.outbox.fetch_pending(limit=50) if m.metadata["kind"] == "emit"]
        self.assertEqual([(m.inbox_id, m.metadata["reply_channel"], m.metadata["correlation_key"]) for m in messages],
                         [(SDE, "feishu", "feishu:1")])

    async def test_targets_the_config_does_not_cover_are_refused(self) -> None:
        targets = ["proj:x:docs", "proj:y", "proj:x:sde:a/b", "proj:x:sde:.."]
        self.steps = iter([{"calls": [
            {"tool": "forward_to_inbox", "arguments": {"target_inbox": t, "text": "x"}} for t in targets
        ] + [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "x", "target_agent": "steward"}}]}])
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x"}))
        self.assertEqual([failed for _, failed in self.answers], [True] * 5)
        self.assertEqual(len(daemon.inbox.list_events()), 1)

    async def test_agent_outside_the_whitelist_is_refused_end_to_end(self) -> None:
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "agent_id": "sde"}))
        self.assertEqual([e.status for e in daemon.inbox.list_events()], ["failed"])
        self.assertEqual(self.roles, [])
        self.assertEqual([m.metadata["kind"] for m in daemon.outbox.fetch_pending(limit=5)], ["agent_refused"])


if __name__ == "__main__":
    unittest.main()
