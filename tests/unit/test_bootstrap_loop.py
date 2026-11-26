from __future__ import annotations
import json
import tempfile
import unittest
import warnings
from pathlib import Path
from typing import Any
from agent_runtime.adapters.channels import ConsoleChannelAdapter
from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager, derive_worktree_names
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.fakes import InMemoryOutbox


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
        created = self.root / "Orca worktrees" / "hb-issue-42-2-2"
        registered: dict[str, Path] = {}

        def fake_runner(cmd: Any, cwd: Path | None) -> CommandResult:
            _ = cwd
            cmd_list = list(cmd)
            commands.append(cmd_list)
            if cmd_list[:2] == ["git", "rev-parse"]:
                return CommandResult(0, "abc1234\n", "")
            if cmd_list == ["git", "for-each-ref", "--format=%(refname:strip=2)", "refs/heads/hb/"]:
                return CommandResult(0, "hb/issue-42\n", "")
            if cmd_list[:3] == ["orca", "worktree", "create"]:
                self.assertEqual(cmd_list[cmd_list.index("--branch") + 1], "hb/issue-42-2")
                self.assertIn("--json", cmd_list)
                created.mkdir(parents=True)
                (created / ".git").write_text("gitdir: ...", encoding="utf-8")
                registered["hb/issue-42-2-2"] = created
                receipt = {"branch": "refs/heads/hb/issue-42-2-2", "path": str(created)}
                return CommandResult(0, json.dumps({"result": {"worktree": receipt}}), "")
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
        self.assertEqual(rec.branch, "hb/issue-42-2-2")

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
        receipt = {"branch": "refs/heads/hb/issue-7", "path": str(self.root / "hb-issue-7")}

        def runner(cmd: Any, _cwd: Path | None) -> CommandResult:
            output = json.dumps({"result": {"worktree": receipt}}) if cmd[0] == "orca" else ""
            return CommandResult(0, output, "")

        mgr = OrcaWorktreeManager(workspace_store=self.store, runner=runner)
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


class TestConfig(unittest.TestCase):
    def test_legacy_github_ingress_settings_warn_and_are_ignored(self) -> None:
        project = {"repo": "o/r", "checkout": "/tmp/repo"}
        baseline = parse_config({"projects": {"demo": project}})
        # Invalid legacy values must not prevent an installed daemon from starting after upgrade.
        raw = {
            "runtime": {"poll_interval_seconds": -1, "poll_mode": "obsolete"},
            "channels": {"github_webhook_secret_env": "OLD_SECRET", "github_token_env": "OLD_TOKEN"},
            "projects": {"demo": {**project, "github_poll": "obsolete", "poll_labels": 42}},
        }
        with warnings.catch_warnings(record=True) as notices:
            warnings.simplefilter("always", UserWarning)
            self.assertEqual(parse_config(raw), baseline)
        messages = "\n".join(str(notice.message) for notice in notices if notice.category is UserWarning)
        for section, keys in (("runtime", raw["runtime"]), ("channels", raw["channels"]),
                              ("projects.demo", ("github_poll", "poll_labels"))):
            for key in keys:
                self.assertIn(f"{section}.{key}", messages)

    def test_legacy_feishu_settings_warn_per_field_and_are_ignored(self) -> None:
        project = {"repo": "o/r", "checkout": "/tmp/repo"}
        channels = {"feishu_app_id_env": "OLD_ID", "feishu_app_secret_env": "OLD_SECRET",
                    "feishu_default_chat_id": "", "feishu_verification_token_env": False}
        raw = {"channels": channels, "projects": {"demo": {**project, "feishu_chat_id": "oc_old"}}}
        baseline = parse_config({"projects": {"demo": project}})
        with warnings.catch_warnings(record=True) as notices:
            warnings.simplefilter("always")
            self.assertEqual(parse_config(raw), baseline)
        fields = [*(f"channels.{key}" for key in channels), "projects.demo.feishu_chat_id"]
        self.assertCountEqual(
            [(w.category, str(w.message)) for w in notices],
            [(UserWarning, f"{field} is obsolete and ignored. Configure Feishu in Switchboard.") for field in fields],
        )

    def test_load_example_toml_and_build_daemon_from_config(self) -> None:
        example_path = Path(__file__).resolve().parents[2] / "hibernaut.example.toml"
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
                self.assertIsInstance(daemon.channel, ConsoleChannelAdapter)
            finally:
                daemon.close()


if __name__ == "__main__":
    unittest.main()
