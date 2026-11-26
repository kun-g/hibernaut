import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager, WorkspaceIsolationError
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions, _State
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.projects import AmbiguousProjectMatch
from agent_runtime.core.models import Emit, Event, SessionRef
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import fake_command
from tests.support.sde_executor import make_base_repo
from tests.support.project_inbox_rules import _DispatchCase, git, project_table


KEY = "github:kun-g/hibernaut#7"


class TestProjectDispatch(_DispatchCase):
    async def test_one_project_serves_three_inboxes_concurrently_in_separate_worktrees(self):
        inboxes = ["proj:hibernaut", "proj:hibernaut:drive", "proj:hibernaut:task-3"]
        path = self.write_config(project_table(
            "hibernaut", ["proj:hibernaut", "proj:hibernaut:drive", "proj:hibernaut:task-\\d+"], self.repo,
        ), barrier_n=3)
        # The same correlation_key in every inbox: only inbox ownership keeps the worktrees apart.
        daemon = await self.dispatch(path, [(i, {"text": i.replace(":", "_")}, KEY) for i in inboxes])

        out = self.outbox()
        self.assertEqual(sorted(out), sorted(inboxes), out)
        runs = {r["text"]: Path(r["cwd"]) for r in self.cli_runs()}
        self.assertEqual(len(runs), 3)
        workdirs = set(runs.values())
        self.assertEqual(len(workdirs), 3, runs)
        for text, cwd in runs.items():
            self.assertTrue(cwd.name.startswith("hb-"), cwd)
            self.assertNotEqual(cwd, self.repo)
            self.assertIn(f"{text} ran in {cwd}", out[text.replace("_", ":")])
            self.assertEqual(sorted(p.name for p in cwd.glob("marker-*")), [f"marker-{text}.txt"])
        self.assert_primary_untouched()

        owners = {key: (owner, Path(workdir)) for key, owner, workdir in
                  self.rows("SELECT work_key, inbox_id, workdir FROM workspaces")}
        self.assertEqual(owners, {
            KEY: ("proj:hibernaut", runs["proj_hibernaut"]),
            f"proj:hibernaut:drive/{KEY}": ("proj:hibernaut:drive", runs["proj_hibernaut_drive"]),
            f"proj:hibernaut:task-3/{KEY}": ("proj:hibernaut:task-3", runs["proj_hibernaut_task-3"]),
        })
        # Queues and sessions stay per inbox; memory is the project's.
        self.assertEqual(sorted(self.rows("SELECT inbox_id, correlation_key FROM sessions")),
                         sorted((i, KEY) for i in inboxes))
        self.assertEqual({daemon.partitions.binding_for(i).memory_dir.name for i in inboxes}, {"hibernaut"})
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])

        # Reclaim through MCP goes to the closing inbox's own worktree.
        assert daemon.mcp_server is not None
        (drive_session,) = self.rows("SELECT session_id FROM sessions WHERE inbox_id = 'proj:hibernaut:drive'")
        reply = daemon.mcp_server.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "hibernaut_close_session", "arguments": {"session_id": drive_session[0]}}})
        assert reply is not None
        reclaim = reply["result"]["structuredContent"]["reclaim"]
        self.assertEqual(Path(reclaim["workdir"]), runs["proj_hibernaut_drive"], reclaim)

    async def test_two_rules_of_one_project_matching_an_inbox_count_as_one_match(self):
        path = self.write_config(project_table(
            "p", ["proj:p", "proj:p:.*", "re:proj:p:task-\\d+"], self.repo,
        ))
        await self.dispatch(path, [("proj:p:task-1", {"text": "t1"}, "task:1")])
        self.assertIn("t1 ran in", self.outbox()["proj:p:task-1"])
        self.assert_primary_untouched()

    async def test_an_inbox_matching_two_projects_is_refused_before_any_executor_or_workspace(self):
        other = self.root / "other"
        other.mkdir()
        path = self.write_config(
            project_table("a", ["proj:a", "shared:.*"], self.repo)
            + project_table("b", ["proj:b", ".*:x"], other)
        )
        # Load rejects this config; dispatch still refuses the inbox for one built another way.
        self.enterContext(patch("agent_runtime.app.projects._validate_disjoint_patterns"))
        with self.assertLogs("agent_runtime.app.main", level="ERROR") as logs:
            await self.dispatch(path, [("shared:x", {"text": "amb"}, "task:amb")])
        (record,) = logs.records
        assert record.exc_info is not None
        self.assertIs(record.exc_info[0], AmbiguousProjectMatch)
        self.assertIn("'shared:x' matches more than one project (a, b)", str(record.exc_info[1]))
        self.assertEqual(self.cli_runs(), [])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM workspaces"), [(0,)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM sessions"), [(0,)])
        self.assertEqual(self.rows("SELECT status FROM event_inbox"), [("pending",)])
        self.assertEqual(self.outbox(), {})

    async def test_explicit_primary_checkout_is_refused_for_every_inbox_of_a_shared_project(self):
        path = self.write_config(project_table("hibernaut", ["proj:hibernaut", "proj:hibernaut:drive"], self.repo))
        await self.dispatch(path, [
            ("proj:hibernaut", {"text": "a", "explicit_workdir": str(self.repo)}, "task:a"),
            ("proj:hibernaut:drive", {"text": "b", "explicit_workdir": str(self.repo)}, "task:b"),
        ])
        out = self.outbox()
        for inbox in ("proj:hibernaut", "proj:hibernaut:drive"):
            self.assertIn(f"[LocalCLIExecutor] refused: explicit_workdir {self.repo} refused for '{inbox}'", out[inbox])
        self.assertEqual(self.cli_runs(), [])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM workspaces"), [(0,)])
        self.assert_primary_untouched()

    async def test_explicit_workdir_naming_another_inboxs_worktree_is_refused(self):
        path = self.write_config(project_table("hibernaut", ["proj:hibernaut", "proj:hibernaut:drive"], self.repo))
        await self.dispatch(path, [("proj:hibernaut", {"text": "first"}, KEY)])
        (primary_wt,) = [Path(r["cwd"]) for r in self.cli_runs()]
        await self.dispatch(path, [
            ("proj:hibernaut:drive", {"text": "intruder", "explicit_workdir": str(primary_wt)}, KEY),
        ])
        self.assertIn("[LocalCLIExecutor] refused", self.outbox()["proj:hibernaut:drive"])
        self.assertEqual([r["text"] for r in self.cli_runs()], ["first"])
        self.assertFalse((primary_wt / "marker-intruder.txt").exists())

    async def test_a_string_inbox_id_project_keeps_its_explicit_workdir_and_bare_workspace_keys(self):
        path = self.write_config(project_table("solo", "proj:solo", self.repo))
        await self.dispatch(path, [("proj:solo", {"text": "managed"}, KEY)])
        self.assertEqual([k for (k,) in self.rows("SELECT work_key FROM workspaces")], [KEY])
        scratch = self.root / "scratch"
        scratch.mkdir()
        await self.dispatch(path, [("proj:solo", {"text": "explicit", "explicit_workdir": str(scratch)}, "task:e")])
        self.assertEqual(Path(self.cli_runs()[-1]["cwd"]), scratch)


class TestWorktreeOwnership(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = make_base_repo(self.root)
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.store = SQLiteWorkspaceStore(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)

    def manager(self, isolate=True):
        return OrcaWorktreeManager(workspace_store=self.store, session_store=self.sessions, backend="git",
                                   primary_inbox_id="proj:h", isolate_inboxes=isolate)

    def legacy_row(self, key, inbox):
        # A worktree recorded before ownership existed, used by a session of `inbox`.
        workdir = self.manager(isolate=False).resolve_workdir(work_key=key, primary_checkout=self.repo)
        with self.db.transaction() as conn:
            conn.execute("UPDATE workspaces SET inbox_id = NULL")
        self.sessions.save(SessionRef(session_id=f"s-{inbox}", inbox_id=inbox, correlation_key=key, work_key=key))
        return workdir

    def test_isolated_resolution_needs_the_inbox(self):
        with self.assertRaisesRegex(WorkspaceIsolationError, "without its inbox_id"):
            self.manager().resolve_workdir(work_key="k", primary_checkout=self.repo)

    def test_an_ownerless_row_is_adopted_only_by_the_inbox_that_used_it(self):
        workdir = self.legacy_row("k", "proj:h")
        self.assertEqual(self.manager().resolve_workdir(work_key="k", primary_checkout=self.repo, inbox_id="proj:h"),
                         workdir)
        self.assertEqual(self.store.get("k").inbox_id, "proj:h")  # type: ignore[union-attr]

    def test_an_ownerless_row_used_by_another_inbox_is_refused(self):
        self.legacy_row("k", "proj:h:drive")
        with self.assertRaisesRegex(WorkspaceIsolationError, "was used by proj:h:drive; 'proj:h' cannot write there"):
            self.manager().resolve_workdir(work_key="k", primary_checkout=self.repo, inbox_id="proj:h")

    def test_assert_writable_accepts_only_the_owned_managed_worktree(self):
        mgr = self.manager()
        wt = mgr.resolve_workdir(work_key="k", primary_checkout=self.repo, inbox_id="proj:h:drive")
        mgr.assert_writable(inbox_id="proj:h:drive", work_key="k", workdir=wt)
        for inbox, workdir in (("proj:h:drive", self.repo), ("proj:h", wt), ("proj:h:other", wt)):
            with self.subTest(inbox=inbox, workdir=workdir), self.assertRaises(WorkspaceIsolationError):
                mgr.assert_writable(inbox_id=inbox, work_key="k", workdir=workdir)
        self.manager(isolate=False).assert_writable(inbox_id="proj:h", work_key="k", workdir=self.repo)


class TestSDEResumeIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_a_resumed_checkpoint_naming_the_primary_checkout_is_refused_before_launch(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        repo = make_base_repo(root)
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        store = SQLiteWorkspaceStore(db)
        mgr = OrcaWorktreeManager(workspace_store=store, session_store=SQLiteSessionStoreAdapter(db), backend="git",
                                  primary_inbox_id="proj:h", isolate_inboxes=True)
        sde = SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(root))),
            options=SDEOptions(review=False), reviewer=None, worktree_manager=mgr,
            inbox_checkout_map={"proj:h": repo}, tasks=SQLiteTaskMetadataStore(db),
        )
        state = _State(stage="launch_pending", workdir=str(repo), base_ref="main", requirements="r",
                       work_key="task:1", pending_prompt="go")
        session = SessionRef(session_id="s1", inbox_id="proj:h:drive", correlation_key="task:1",
                             executor_id="sde", checkpoint=state.dump(), pending_request_id="req-1")
        approve = Event(inbox_id="proj:h:drive", source="test", external_id="a1",
                        payload={"approval_request_id": "req-1", "approved": True}, correlation_key="task:1")
        caps = Mock(permissions=InMemoryPermission())
        caps.is_interrupted.return_value = False
        outcome = await sde.execute([approve], session, caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn(f"[sde] development failed: {repo} is not the managed worktree of 'task:1' owned by "
                      "'proj:h:drive'", outcome.messages[0])
        self.assertEqual(git(repo, "status", "--porcelain"), "")


if __name__ == "__main__":
    unittest.main()
