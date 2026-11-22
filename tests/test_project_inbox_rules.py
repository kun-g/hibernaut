import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from agent_runtime.adapters.channels.feishu_channel import FeishuChannelAdapter
from agent_runtime.adapters.executor_cli import OrcaWorktreeManager, WorkspaceIsolationError
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions, _State
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.app.projects import AmbiguousProjectMatch, InboxRule, ProjectRouter
from agent_runtime.core.models import Emit, Event, SessionRef
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import fake_command
from tests.test_sde_executor import make_base_repo

FAKE_CLI = textwrap.dedent(
    """
    import json, os, pathlib, sys, time

    events = json.loads(sys.argv[-1].split("## events\\n", 1)[1])
    text = events[-1]["payload"]["text"]
    cwd = os.getcwd()
    pathlib.Path(cwd, f"marker-{text}.txt").write_text(text)
    with open(os.environ["FAKE_CLI_LOG"], "a") as log:
        log.write(json.dumps({"cwd": cwd, "text": text}) + "\\n")
    barrier = os.environ.get("BARRIER_DIR")
    if barrier:
        # Every run waits here until all expected runs started: they only finish if they ran concurrently.
        pathlib.Path(barrier, text).touch()
        deadline = time.time() + 20
        while len(os.listdir(barrier)) < int(os.environ["BARRIER_N"]):
            if time.time() > deadline:
                sys.exit("barrier timeout")
            time.sleep(0.02)
    print(json.dumps({"outcome": "emit", "messages": [f"{text} ran in {cwd}"]}))
    """
)

KEY = "github:kun-g/hibernaut#7"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def project_table(name, inbox_id, checkout, **extra):
    lines = [f"[projects.{name}]", f"inbox_id = {json.dumps(inbox_id)}", f'repo = "kun-g/{name}"',
             f'checkout = "{checkout}"', 'worktree_backend = "git"']
    lines += [f"{k} = {json.dumps(v)}" for k, v in extra.items()]
    return "\n".join(lines) + "\n"


class TestInboxIdConfig(unittest.TestCase):
    def parse(self, **projects):
        return parse_config({"projects": {name: {"repo": f"o/{name}", "checkout": "/tmp/x", **item}
                                          for name, item in projects.items()}})

    def test_string_inbox_id_stays_one_exact_inbox(self):
        cfg = self.parse(hibernaut={"inbox_id": "proj:hibernaut"}, other={})
        proj = cfg.projects["hibernaut"]
        self.assertEqual(proj.inbox_id, "proj:hibernaut")
        self.assertEqual(proj.inbox_rules, (InboxRule.exact("proj:hibernaut"),))
        self.assertFalse(proj.serves_multiple_inboxes)
        self.assertEqual(cfg.projects["other"].inbox_id, "proj:other")
        self.assertFalse(cfg.projects["other"].serves_multiple_inboxes)

    def test_array_mixes_exact_ids_and_re_patterns_and_the_first_literal_is_primary(self):
        cfg = self.parse(hibernaut={"inbox_id": ["re:proj:hibernaut:task-\\d+", "proj:hibernaut",
                                                 "proj:hibernaut:drive"]})
        proj = cfg.projects["hibernaut"]
        self.assertEqual(proj.inbox_id, "proj:hibernaut")
        self.assertTrue(proj.serves_multiple_inboxes)
        self.assertEqual([str(r) for r in proj.inbox_rules],
                         ["re:proj:hibernaut:task-\\d+", "proj:hibernaut", "proj:hibernaut:drive"])
        # Reload compares configs; the same file must compare equal.
        self.assertEqual(cfg, self.parse(hibernaut={"inbox_id": ["re:proj:hibernaut:task-\\d+", "proj:hibernaut",
                                                                 "proj:hibernaut:drive"]}))

    def test_patterns_match_the_whole_inbox_id(self):
        router = ProjectRouter(self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "re:proj:hibernaut:task-\\d+"]})
                               .projects.values())
        self.assertIsNotNone(router.resolve("proj:hibernaut:task-12"))
        for miss in ("proj:hibernaut:task-12x", "xproj:hibernaut:task-1", "proj:hibernaut:task-", "proj:hibernaut:"):
            with self.subTest(miss):
                self.assertIsNone(router.resolve(miss))

    def test_malformed_inbox_ids_are_rejected_with_the_reason(self):
        cases = {
            "must be a string or a non-empty array": [],
            "entries must be non-empty strings": ["proj:a", 3],
            "entries must be non-empty strings, got ' '": ["proj:a", " "],
            "has an empty pattern": ["proj:a", "re:"],
            "invalid pattern 're:proj:\\('": ["proj:a", "re:proj:("],
            "needs at least one literal inbox_id": ["re:proj:a:.*"],
            "lists 'proj:a' twice": ["proj:a", "proj:a"],
            "is a pattern; list it in an array": "re:proj:a:.*",
            "must not be empty": "  ",
        }
        for message, raw in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.parse(a={"inbox_id": raw})

    def test_loading_a_file_that_assigns_one_literal_to_two_projects_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hibernaut.toml"
            path.write_text(project_table("a", ["proj:a", "proj:shared"], "/tmp/a")
                            + project_table("b", "proj:shared", "/tmp/b"))
            with self.assertRaisesRegex(ValueError, "inbox_id 'proj:shared' is assigned to both projects.a and "
                                                    "projects.b"):
                load_config(path)

    def test_a_literal_of_one_project_matched_by_another_projects_pattern_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "'proj:a:drive' of projects.a also matches a pattern of projects.b"):
            self.parse(a={"inbox_id": ["proj:a", "proj:a:drive"]}, b={"inbox_id": ["proj:b", "re:proj:a:.*"]})

    def test_overlap_between_a_literal_and_a_pattern_is_ambiguous_at_routing(self):
        # Load rejects this; the router still refuses it for configs built another way.
        a = parse_config({"projects": {"a": {"repo": "o/a", "checkout": "/tmp/a", "inbox_id": "proj:a:x"}}})
        b = parse_config({"projects": {"b": {"repo": "o/b", "checkout": "/tmp/b", "inbox_id": ["proj:b", "re:.*:x"]}}})
        router = ProjectRouter([a.projects["a"], b.projects["b"]])
        with self.assertRaises(AmbiguousProjectMatch) as caught:
            router.resolve("proj:a:x")
        self.assertEqual(caught.exception.projects, ("a", "b"))
        self.assertEqual([p.name for p in router.matching("proj:b")], ["b"])

    def test_pattern_inboxes_get_their_projects_settings_through_inbox_maps(self):
        cfg = self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "re:proj:hibernaut:.+"], "feishu_chat_id": "oc_h"},
                         other={"inbox_id": "proj:other"})
        router = ProjectRouter(cfg.projects.values())
        chats = router.inbox_map(lambda p: p.feishu_chat_id or None)
        self.assertEqual(chats.get("proj:hibernaut:drive"), "oc_h")
        self.assertIsNone(chats.get("proj:other"))
        self.assertEqual(list(chats), ["proj:hibernaut"])
        channel = FeishuChannelAdapter(app_id="a", app_secret="s", inbox_chat_map=chats)
        self.assertEqual(channel._resolve_chat_id("proj:hibernaut:drive"), "oc_h")


class _DispatchCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = make_base_repo(self.root)
        self.cli = self.root / "fake_cli.py"
        self.cli.write_text(FAKE_CLI)
        self.cli_log = self.root / "cli.jsonl"
        self.barrier = self.root / "barrier"
        self.barrier.mkdir()
        self.db_path = self.root / "hb.db"

    def write_config(self, projects: str, barrier_n: int = 0) -> Path:
        env = {"FAKE_CLI_LOG": str(self.cli_log)}
        if barrier_n:
            env.update(BARRIER_DIR=str(self.barrier), BARRIER_N=str(barrier_n))
        path = self.root / "hibernaut.toml"
        path.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{self.root / "mem"}"\n'
            'default_executor = "cli"\nmax_concurrency = 4\n'
            f'[executors.cli]\ncommand = "{sys.executable}"\nargs = ["{self.cli}"]\ntimeout_seconds = 60\n'
            f"[executors.cli.env]\n" + "".join(f'{k} = "{v}"\n' for k, v in env.items()) + projects
        )
        return path

    async def dispatch(self, path: Path, events: list[tuple[str, dict, str | None]]):
        daemon = build_daemon_from_config(load_config(path))
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            for n, (inbox_id, payload, key) in enumerate(events):
                daemon.handler.ingest(inbox_id=inbox_id, source="test", external_id=f"e{n}-{time.monotonic_ns()}",
                                      payload=payload,
                                      correlation_key=key)
            await daemon.pool.wait_until_idle(timeout_seconds=60)
        finally:
            stop.set()
            await pool
        return daemon

    def rows(self, sql: str):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql).fetchall()

    def outbox(self):
        return dict(self.rows("SELECT inbox_id, content FROM outbox ORDER BY seq"))

    def cli_runs(self):
        return [json.loads(line) for line in self.cli_log.read_text().splitlines()] if self.cli_log.exists() else []

    def assert_primary_untouched(self):
        self.assertEqual(git(self.repo, "status", "--porcelain", "--untracked-files=all"), "")
        self.assertEqual(sorted(p.name for p in self.repo.iterdir()), [".git", "calc.py"])


class TestProjectDispatch(_DispatchCase):
    async def test_one_project_serves_three_inboxes_concurrently_in_separate_worktrees(self):
        inboxes = ["proj:hibernaut", "proj:hibernaut:drive", "proj:hibernaut:task-3"]
        path = self.write_config(project_table(
            "hibernaut", ["proj:hibernaut", "proj:hibernaut:drive", "re:proj:hibernaut:task-\\d+"], self.repo,
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
        # Queues, sessions and memory stay per inbox.
        self.assertEqual(sorted(self.rows("SELECT inbox_id, correlation_key FROM sessions")),
                         sorted((i, KEY) for i in inboxes))
        self.assertEqual(len({daemon.partitions.binding_for(i).memory_dir for i in inboxes}), 3)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])

        # One poll and one webhook route per project, to the primary inbox.
        assert daemon.github_poller is not None
        self.assertEqual([(t.repo, t.inbox_id) for t in daemon.github_poller._targets],
                         [("kun-g/hibernaut", "proj:hibernaut")])

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
            "p", ["proj:p", "re:proj:p:.*", "re:proj:p:task-\\d+"], self.repo,
        ))
        await self.dispatch(path, [("proj:p:task-1", {"text": "t1"}, "task:1")])
        self.assertIn("t1 ran in", self.outbox()["proj:p:task-1"])
        self.assert_primary_untouched()

    async def test_an_inbox_matching_two_projects_is_refused_before_any_executor_or_workspace(self):
        other = self.root / "other"
        other.mkdir()
        path = self.write_config(
            project_table("a", ["proj:a", "re:shared:.*"], self.repo)
            + project_table("b", ["proj:b", "re:.*:x"], other)
        )
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


class TestOwnerMigration(unittest.TestCase):
    def test_existing_workspaces_rows_survive_and_gain_an_empty_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "old.db"
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "CREATE TABLE workspaces (seq INTEGER PRIMARY KEY AUTOINCREMENT, work_key TEXT NOT NULL UNIQUE, "
                    "repo_checkout TEXT NOT NULL, workdir TEXT NOT NULL, branch TEXT NOT NULL, status TEXT NOT NULL "
                    "CHECK (status IN ('creating', 'ready', 'reclaiming')), created_at TEXT NOT NULL, "
                    "updated_at TEXT NOT NULL)"
                )
                conn.execute("INSERT INTO workspaces (work_key, repo_checkout, workdir, branch, status, created_at, "
                             "updated_at) VALUES ('k', '/r', '/r/../hb-task-1', 'hb/task-1', 'ready', 't', 't')")
            db = SQLiteDatabase(path)
            try:
                record = SQLiteWorkspaceStore(db).get("k")
            finally:
                db.close()
            assert record is not None
            self.assertEqual((record.workdir, record.status, record.inbox_id), ("/r/../hb-task-1", "ready", None))


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
