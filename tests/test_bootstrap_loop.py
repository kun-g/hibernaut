from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    FeishuChannelAdapter,
    GitHubIssuePoller,
    PollTarget,
    build_feishu_or_console_channel,
)
from agent_runtime.adapters.executor_cli import (
    CLITemplate,
    CommandResult,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    derive_worktree_names,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit, Event, Silent, Suspend
from agent_runtime.engine.handler import EventHandler
from tests.fakes import InMemoryInbox, InMemoryMemoryStore, InMemoryOutbox, InMemoryPermission, InMemoryTimerStore


class _FakeCaps:
    def __init__(self, *, interrupted: bool = False) -> None:
        self.memory = InMemoryMemoryStore({"instructions.md": "be quiet"})
        self.timers = InMemoryTimerStore()
        self.permissions = InMemoryPermission()
        self._interrupted = interrupted

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return self._interrupted


class TestGitHubIssuePoller(unittest.TestCase):
    def test_poll_once_idempotency_and_correlation_key(self) -> None:
        inbox = InMemoryInbox()
        handler = EventHandler(inbox=inbox)
        sample_issues = [
            {
                "number": 42,
                "title": "Fix bug",
                "body": "details",
                "labels": [{"name": "hibernaut"}],
                "updatedAt": "2026-11-21T10:00:00Z",
                "url": "https://github.com/kun-g/hibernaut/issues/42",
            }
        ]
        poller = GitHubIssuePoller(
            handler,
            targets=[PollTarget(repo="kun-g/hibernaut", inbox_id="proj:hibernaut", labels=("hibernaut",))],
            fetcher=lambda _target: sample_issues,
        )

        self.assertEqual(poller.poll_once(), 1)
        # Polling same unchanged updatedAt must be deduplicated (0 new events).
        self.assertEqual(poller.poll_once(), 0)

        pending = inbox.fetch_pending("proj:hibernaut")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].external_id, "github-poll:kun-g/hibernaut#42:2026-11-21T10:00:00Z")
        self.assertEqual(pending[0].correlation_key, "github:kun-g/hibernaut#42")

        # Updated timestamp triggers a fresh event with the same correlation_key.
        sample_issues[0]["updatedAt"] = "2026-11-21T10:05:00Z"
        self.assertEqual(poller.poll_once(), 1)
        self.assertEqual(len(inbox.fetch_pending("proj:hibernaut")), 2)


class TestOrcaWorktreeManager(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="wt_mgr_")
        self.root = Path(self._tmp.name)
        self.primary = self.root / "hibernaut"
        self.primary.mkdir()
        self.db = SQLiteDatabase(":memory:")
        self.store = SQLiteWorkspaceStore(self.db)

    def tearDown(self) -> None:
        self.db.close()
        self._tmp.cleanup()

    def test_minimal_naming_and_collision_suffix(self) -> None:
        self.assertEqual(
            derive_worktree_names("github:kun-g/hibernaut#42", 1, set()),
            ("hb-issue-42", "hb/issue-42"),
        )
        self.assertEqual(
            derive_worktree_names("github:kun-g/hibernaut#42", 1, {"hb-issue-42"}),
            ("hb-issue-42-2", "hb/issue-42-2"),
        )
        self.assertEqual(
            derive_worktree_names("pr:12", 1, set()),
            ("hb-pr-12", "hb/pr-12"),
        )
        self.assertEqual(
            derive_worktree_names("feishu:oc_1:omt_9", 7, set()),
            ("hb-task-7", "hb/task-7"),
        )

    def test_explicit_override_creating_reconcile_and_orca_create(self) -> None:
        commands: list[list[str]] = []
        created = self.root / "Orca worktrees" / "hb-issue-42"
        registered: dict[str, Path] = {}

        def fake_runner(cmd: Any, cwd: Path | None) -> CommandResult:
            _ = cwd
            cmd_list = list(cmd)
            commands.append(cmd_list)
            if cmd_list[:2] == ["git", "rev-parse"]:
                return CommandResult(0, "abc1234\n", "")
            if cmd_list[:3] == ["orca", "worktree", "create"]:
                created.mkdir(parents=True)
                (created / ".git").write_text("gitdir: ...", encoding="utf-8")
                registered["hb/issue-42"] = created
                return CommandResult(0, "Created successfully\n", "")
            if cmd_list == ["git", "worktree", "list", "--porcelain", "-z"]:
                listing = "".join(f"worktree {p}\0branch refs/heads/{b}\0\0" for b, p in registered.items())
                return CommandResult(0, listing, "")
            return CommandResult(0, "", "")

        mgr = OrcaWorktreeManager(workspace_store=self.store, backend="orca", runner=fake_runner)

        # Priority 1: explicit_workdir bypasses worktree creation.
        override = mgr.resolve_workdir(
            work_key="github:kun-g/hibernaut#42",
            primary_checkout=self.primary,
            explicit_workdir=self.primary,
        )
        self.assertEqual(override, self.primary.resolve())
        self.assertIsNone(self.store.get("github:kun-g/hibernaut#42"))

        # Priority 3: dynamic creation via Orca -> persisted as ready.
        created_dir = mgr.resolve_workdir(
            work_key="github:kun-g/hibernaut#42",
            primary_checkout=self.primary,
        )
        self.assertEqual(created_dir, created.resolve())
        rec = self.store.get("github:kun-g/hibernaut#42")
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec.status, "ready")
        self.assertEqual(rec.branch, "hb/issue-42")

        # Priority 2: subsequent call reuses persisted ready path without invoking orca again.
        cmd_count_before = len(commands)
        self.store.mark_ready(rec.work_key, workdir=str(self.root / created.name), branch=rec.branch)
        reused = mgr.resolve_workdir(
            work_key="github:kun-g/hibernaut#42",
            primary_checkout=self.primary,
        )
        self.assertEqual(reused, created_dir)
        self.assertEqual(commands[cmd_count_before:], [["git", "worktree", "list", "--porcelain", "-z"]])
        self.assertEqual(self.store.get(rec.work_key).workdir, str(created_dir))
        del registered[rec.branch]
        with self.assertRaisesRegex(RuntimeError, "No registered worktree"):
            mgr.resolve_workdir(work_key=rec.work_key, primary_checkout=self.primary)

        # Safety 1 reconciliation: interrupted 'creating' record with valid local .git -> promoted to ready.
        reconcile_dir = self.root / "hb-issue-99"
        reconcile_dir.mkdir()
        registered["hb/issue-99"] = reconcile_dir
        (reconcile_dir / ".git").write_text("gitdir: ...", encoding="utf-8")
        self.store.reserve_creating(
            work_key="github:kun-g/hibernaut#99",
            repo_checkout=str(self.primary),
            workdir=str(reconcile_dir),
            branch="hb/issue-99",
        )
        reconciled = mgr.resolve_workdir(
            work_key="github:kun-g/hibernaut#99",
            primary_checkout=self.primary,
        )
        self.assertEqual(reconciled, reconcile_dir.resolve())
        rec99 = self.store.get("github:kun-g/hibernaut#99")
        assert rec99 is not None
        self.assertEqual(rec99.status, "ready")

    def test_success_without_registered_branch_keeps_creating_intent(self) -> None:
        mgr = OrcaWorktreeManager(workspace_store=self.store, runner=lambda *_: CommandResult(0, "", ""))
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "Expected one local linked worktree"):
                mgr.resolve_workdir(work_key="issue:7", primary_checkout=self.primary)
            record = self.store.get("issue:7")
            assert record is not None
            self.assertEqual((record.status, record.branch), ("creating", "hb/issue-7"))

    def test_reclaim_dual_ownership_and_three_safety_gates(self) -> None:
        outbox = InMemoryOutbox()
        wt_dir = self.root / "hb-issue-55"
        wt_dir.mkdir()
        self.store.reserve_creating(
            work_key="github:kun-g/hibernaut#55",
            repo_checkout=str(self.primary),
            workdir=str(wt_dir),
            branch="hb/issue-55",
        )
        self.store.mark_ready(
            "github:kun-g/hibernaut#55",
            workdir=str(wt_dir),
            branch="hb/issue-55",
        )

        # Unmanaged directory refused.
        mgr_unmanaged = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=lambda _c, _d: CommandResult(0, "", ""),
        )
        self.assertFalse(mgr_unmanaged.reclaim("unknown:key").reclaimed)

        # Gate 1: open PR blocks reclaim & alerts outbox.
        def open_pr_runner(cmd: Any, _cwd: Path | None) -> CommandResult:
            if list(cmd)[:3] == ["gh", "pr", "list"]:
                return CommandResult(0, '[{"number": 101}]', "")
            return CommandResult(0, "", "")

        res_pr = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=open_pr_runner,
        ).reclaim("github:kun-g/hibernaut#55", notify_outbox=outbox, inbox_id="proj:hibernaut")
        self.assertFalse(res_pr.reclaimed)
        self.assertIn("open PR", res_pr.reason)
        self.assertEqual(len(outbox.fetch_pending()), 1)

        # Gate 2: dirty working tree blocks reclaim.
        def dirty_runner(cmd: Any, _cwd: Path | None) -> CommandResult:
            if list(cmd)[:2] == ["git", "status"]:
                return CommandResult(0, " M src/app.py\n", "")
            return CommandResult(0, "[]", "")

        res_dirty = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=dirty_runner,
        ).reclaim("github:kun-g/hibernaut#55")
        self.assertFalse(res_dirty.reclaimed)
        self.assertIn("uncommitted", res_dirty.reason)

        # Gate 3: unpushed commits block reclaim.
        def unpushed_runner(cmd: Any, _cwd: Path | None) -> CommandResult:
            if list(cmd)[:2] == ["git", "log"]:
                return CommandResult(0, "deadbeef local commit\n", "")
            if list(cmd)[:3] == ["gh", "pr", "list"]:
                return CommandResult(0, "[]", "")
            return CommandResult(0, "", "")

        res_unpushed = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=unpushed_runner,
        ).reclaim("github:kun-g/hibernaut#55")
        self.assertFalse(res_unpushed.reclaimed)
        self.assertIn("unpushed", res_unpushed.reason)

        # All 3 gates pass -> worktree reclaimed and removed from DB.
        def clean_runner(cmd: Any, _cwd: Path | None) -> CommandResult:
            if list(cmd)[:3] == ["gh", "pr", "list"]:
                return CommandResult(0, "[]", "")
            return CommandResult(0, "", "")

        res_ok = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=clean_runner,
        ).reclaim("github:kun-g/hibernaut#55")
        self.assertTrue(res_ok.reclaimed)
        self.assertIsNone(self.store.get("github:kun-g/hibernaut#55"))


class TestLocalCLIExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="cli_exec_")
        self.root = Path(self._tmp.name)
        self.db = SQLiteDatabase(":memory:")
        self.store = SQLiteWorkspaceStore(self.db)
        self.mgr = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=lambda _c, _d: CommandResult(0, "", ""),
        )

    def tearDown(self) -> None:
        self.db.close()
        self._tmp.cleanup()

    async def test_cli_env_passthrough_outcomes_and_interrupt_termination(self) -> None:
        emit_tpl = CLITemplate(
            name="claude",
            command=sys.executable,
            args=(
                "-c",
                "import json, os, sys; print(json.dumps({'outcome': 'emit', 'messages': [os.environ.get('MAGPIE_URL', '')]}))",
            ),
            env={"MAGPIE_URL": "http://127.0.0.1:8787"},
        )
        suspend_tpl = CLITemplate(
            name="suspend_cli",
            command=sys.executable,
            args=(
                "-c",
                "import json; print(json.dumps({'outcome': 'suspend', 'request_id': 'req_cli_1', 'prompt': 'approve deploy?'}))",
            ),
        )
        silent_tpl = CLITemplate(
            name="silent_cli",
            command=sys.executable,
            args=("-c", "import json; print(json.dumps({'outcome': 'silent'}))"),
        )
        slow_tpl = CLITemplate(
            name="slow_cli",
            command=sys.executable,
            args=("-c", "import time; time.sleep(10)"),
        )
        executor = LocalCLIExecutor(
            worktree_manager=self.mgr,
            inbox_checkout_map={"proj:hibernaut": self.root},
            templates={
                "claude": emit_tpl,
                "suspend_cli": suspend_tpl,
                "silent_cli": silent_tpl,
                "slow_cli": slow_tpl,
            },
            interrupt_poll_interval=0.02,
        )

        # 1. Emit + env passthrough (yetone/magpie)
        evt = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e1",
            payload={"explicit_workdir": str(self.root)},
        )
        out = await executor.execute([evt], None, _FakeCaps())
        self.assertIsInstance(out, Emit)
        assert isinstance(out, Emit)
        self.assertEqual(out.messages, ("http://127.0.0.1:8787",))

        # 2. Suspend mapping
        evt_susp = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e2",
            payload={"explicit_workdir": str(self.root), "executor": "suspend_cli"},
        )
        out_susp = await executor.execute([evt_susp], None, _FakeCaps())
        self.assertIsInstance(out_susp, Suspend)
        assert isinstance(out_susp, Suspend)
        self.assertEqual(out_susp.request_id, "req_cli_1")

        # 3. Silent mapping
        evt_sil = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e3",
            payload={"explicit_workdir": str(self.root), "executor": "silent_cli"},
        )
        self.assertIsInstance(await executor.execute([evt_sil], None, _FakeCaps()), Silent)

        # 4. Interrupt safely terminates long-running CLI subprocess
        caps = _FakeCaps(interrupted=False)
        evt_slow = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e4",
            payload={"explicit_workdir": str(self.root), "executor": "slow_cli"},
        )
        task = asyncio.create_task(executor.execute([evt_slow], None, caps))
        await asyncio.sleep(0.05)
        caps._interrupted = True
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)


class TestFeishuChannelAndConfig(unittest.IsolatedAsyncioTestCase):
    async def test_feishu_token_cache_approval_card_and_console_fallback(self) -> None:
        calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        now_box = [100.0]

        def fake_post(url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
            calls.append((url, headers, payload))
            if "tenant_access_token" in url:
                return {"tenant_access_token": "tok_abc", "expire": 7200}
            return {"code": 0, "msg": "ok"}

        ch = FeishuChannelAdapter(
            app_id="cli_a",
            app_secret="sec_b",
            default_chat_id="oc_default",
            inbox_chat_map={"proj:hibernaut": "oc_proj"},
            http_post=fake_post,
            clock=lambda: now_box[0],
        )

        await ch.send("proj:hibernaut", "Build passed", {"kind": "emit"})
        await ch.send(
            "proj:hibernaut",
            "Approve deploy?",
            {"kind": "approval_request", "request_id": "req_77"},
        )

        # Token fetched once and cached for second message (3 total HTTP calls).
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[1][2]["msg_type"], "text")
        self.assertEqual(calls[2][2]["msg_type"], "interactive")
        card = json.loads(calls[2][2]["content"])
        buttons = card["elements"][1]["actions"]
        self.assertEqual(buttons[0]["value"]["approval_request_id"], "req_77")
        self.assertTrue(buttons[0]["value"]["approved"])
        self.assertFalse(buttons[1]["value"]["approved"])

        # Missing env credentials fall back safely to ConsoleChannelAdapter.
        os.environ.pop("FEISHU_TEST_MISSING_ID", None)
        os.environ.pop("FEISHU_TEST_MISSING_SECRET", None)
        fallback = build_feishu_or_console_channel(
            app_id_env="FEISHU_TEST_MISSING_ID",
            app_secret_env="FEISHU_TEST_MISSING_SECRET",
        )
        self.assertIsInstance(fallback, ConsoleChannelAdapter)

    def test_load_example_toml_and_build_daemon_from_config(self) -> None:
        example_path = Path(__file__).resolve().parents[1] / "hibernaut.example.toml"
        cfg = load_config(example_path)
        self.assertEqual(cfg.runtime.default_executor, "claude")
        self.assertIn("claude", cfg.executors)
        self.assertIn("cursor-agent", cfg.executors)
        self.assertIn("hibernaut", cfg.projects)
        self.assertEqual(cfg.projects["hibernaut"].inbox_id, "proj:hibernaut")

        with tempfile.TemporaryDirectory(prefix="cfg_daemon_") as tmp:
            cfg_override = load_config(example_path)
            # Build daemon using temp db/memory paths via in-memory replacement
            from dataclasses import replace

            local_cfg = replace(
                cfg_override,
                runtime=replace(
                    cfg_override.runtime,
                    db_path=str(Path(tmp) / "hb.db"),
                    memory_root=str(Path(tmp) / "mem"),
                ),
            )
            daemon = build_daemon_from_config(local_cfg)
            try:
                self.assertIsNotNone(daemon.github_poller)
                self.assertIsNotNone(daemon.github_webhook)
                self.assertIsNotNone(daemon.feishu_webhook)
            finally:
                daemon.close()


if __name__ == "__main__":
    unittest.main()
