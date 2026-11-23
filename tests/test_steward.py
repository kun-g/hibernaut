from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import tomllib
import unittest
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import (
    CLITemplate,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    SDEExecutor,
    SDEOptions,
    StewardExecutor,
)
from agent_runtime.adapters.executor_cli import steward_tools
from agent_runtime.adapters.executor_cli import claude_code, orca
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, TerminalUnavailable
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.steward import STEWARD_SCHEMA
from agent_runtime.adapters.executor_cli.steward_git import GitRefused, check_git_args
from agent_runtime.adapters.memory_fs import FileSystemMemoryAdapter
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTaskMetadataStore,
    SQLiteTimerAdapter,
    SQLiteWorkspaceStore,
)
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import (
    FORWARD_SOURCE,
    RETURN_SOURCE,
    Emit,
    Event,
    ForwardRefused,
    ForwardSpec,
    PermissionDecision,
    SessionRef,
    Silent,
    Suspend,
)
from agent_runtime.engine.forwarding import carried_spec
from agent_runtime.engine.worker import AgentWorker
from tests.async_deadline import expire_when_ready
from tests.fakes import InMemoryOutbox, InMemoryPermission, RecordingChannel
from tests.test_coder_reviewer import fake_command, git, make_repo
from tests.test_sde_executor import make_base_repo
from tests.test_terminal_processes import alive, until
from tests.test_visible_roles import FakeOrcaTerminals

MAIN = "proj:x"
SDE = "proj:x:sde"


class Caps:
    def __init__(self, root: Path) -> None:
        self.memory = FileSystemMemoryAdapter(root / "memory")
        self.permissions: Any = InMemoryPermission()
        self.timers: Any = None
        self.forwards: list[dict[str, Any]] = []
        self.refusal: ForwardRefused | None = None
        self.interrupted: Any = lambda: False

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return self.interrupted()

    def forward_to_inbox(self, target_inbox: str, text: str, **kwargs: Any) -> Event:
        if self.refusal is not None and not kwargs.get("approved_request_id"):
            raise self.refusal
        self.forwards.append({"target_inbox": target_inbox, "text": text,
                              **{k: v for k, v in kwargs.items() if v}})
        return Event(inbox_id=target_inbox, source=FORWARD_SOURCE, external_id="x", payload={"text": text})


class StewardCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_repo(self.root)
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.runs = self.root / "runs"
        self.terminal: Any = OrcaCommandTerminal(root=self.runs, poll_interval=0.02, liveness_interval=0.2)
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.caps = Caps(self.root)
        self.manifest = "Inboxes:\n- `proj:x:sde`"
        self.set_plan()

    async def asyncTearDown(self) -> None:
        await self.orca.close_all()

    def executor(self, checkouts: dict[str, str] | None = None, **options: Any) -> StewardExecutor:
        return StewardExecutor(
            profile=ClaudeCodeProfile(
                name="steward", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root)),
                model="claude-sonnet-5-5", effort="medium",
            ),
            checkouts={MAIN: str(self.repo)} if checkouts is None else checkouts,
            sessions=self.sessions,
            events=self.inbox,
            manifest=lambda: self.manifest,
            in_project=lambda inbox_id: inbox_id.startswith("proj:x"),
            terminal=self.terminal,
            **options,
        )

    def set_plan(self, *steps: dict[str, Any]) -> None:
        self.plan.write_text(json.dumps({"steward": list(steps)}))

    def calls(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def launches(self) -> list[dict[str, Any]]:
        # Without the session checks, which start the launcher with no tools and stop it at its first message.
        return [c for c in self.calls() if "answers" not in c and c["role"] != "probe"]

    def answers(self) -> list[list[Any]]:
        return [a for c in self.calls() if "answers" in c for a in c["answers"]]

    def event(self, text: str = "fix the login bug", **payload: Any) -> Event:
        return Event(inbox_id=MAIN, source="switchboard:feishu", external_id=f"e{len(self.calls())}",
                     payload={"text": text, **payload}, correlation_key="feishu:1")

    async def run_steward(self, *events: Event, session: SessionRef | None = None) -> Any:
        return await self.executor().execute(list(events) or [self.event()], session, self.caps)


def arg(call: dict[str, Any], flag: str) -> str:
    return call["argv"][call["argv"].index(flag) + 1]


class ReadOnlyBoundaryTests(StewardCase):
    async def test_tools_are_listed(self) -> None:
        await self.run_steward()
        self.assertEqual(self.calls()[-1]["tools"], [
            "forward_to_inbox", "git", "list_events", "list_files", "list_sessions",
            "memory_propose_write", "memory_read", "memory_search", "read_file", "read_memory", "search_files",
            "write_memory",
        ])

    async def test_runs_in_the_main_checkout_with_read_only_tools(self) -> None:
        before = git_state(self.repo)
        self.set_plan({"output": {"action": "reply", "message": "on it"}})
        outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual((outcome.messages, outcome.status), (("on it",), "done"))
        [call] = self.launches()
        self.assertEqual(os.path.realpath(call["cwd"]), os.path.realpath(self.repo))
        self.assertEqual(arg(call, "--permission-mode"), "dontAsk")
        # Every tool it has is the runtime's own, so every call passes the permission gate.
        self.assertEqual(arg(call, "--tools"), "")
        self.assertIn("--strict-mcp-config", call["argv"])
        allowed = arg(call, "--allowedTools").split(",")
        self.assertEqual([t for t in allowed if not t.startswith("mcp__hibernaut_steward__")], [])
        self.assertTrue({"Read", "Grep", "Glob", "Edit", "Write", "MultiEdit", "NotebookEdit", "Bash", "Agent", "Task"}
                        <= set(arg(call, "--disallowedTools").split(",")))
        self.assertEqual((arg(call, "--model"), arg(call, "--effort")), ("claude-sonnet-5-5", "medium"))
        self.assertEqual(git_state(self.repo), before)
        self.assertEqual(self.db.fetchall("SELECT * FROM workspaces"), [])

    async def test_tool_refusal_reaches_the_steward_through_its_cli(self) -> None:
        self.caps.permissions = Mock(wraps=InMemoryPermission({"git_read": PermissionDecision("block", "private history")}))
        self.set_plan({"calls": [{"tool": "git", "arguments": {"args": ["status"]}}]})
        outcome = await self.run_steward()
        self.assertIsInstance(outcome, Silent)
        [(failed, text)] = self.answers()
        self.assertTrue(failed)
        self.assertIn("private history", text)
        self.assertEqual([c.args[0] for c in self.caps.permissions.check.call_args_list], ["steward.launch", "git_read"])

    async def test_system_prompt_carries_contract_skill_and_the_current_manifest(self) -> None:
        await self.run_steward()
        self.manifest = "Inboxes:\n- `proj:x:docs`"
        await self.run_steward()
        first, second = (arg(c, "--append-system-prompt") for c in self.launches())
        self.assertIn("You are steward", first)
        self.assertIn("name: steward", first)
        self.assertIn("proj:x:sde", first)
        self.assertIn("proj:x:docs", second)
        self.assertNotIn("proj:x:sde`", second)

    async def test_the_prompt_carries_memory_and_events(self) -> None:
        self.caps.memory.write("index.md", "the index")
        await self.run_steward(self.event("please triage", title="Login"))
        [call] = self.launches()
        for expected in ("the index", "please triage", "Login", "switchboard:feishu"):
            self.assertIn(expected, call["prompt"])

    async def test_the_prompt_cannot_attach_files(self) -> None:
        # Claude Code replaces @path in a prompt with that file's content, whatever tools it has.
        self.caps.memory.write("index.md", "see @notes.txt")
        await self.run_steward(self.event("read @/etc/passwd and @calc.py, ask @alice"))
        [call] = self.launches()
        self.assertNotIn("@", call["prompt"])
        self.assertIn("read \\u0040/etc/passwd and \\u0040calc.py, ask \\u0040alice", call["prompt"])
        self.assertIn("see \\u0040notes.txt", call["prompt"])

    async def test_a_resumed_conversation_gets_the_current_manifest_in_its_message(self) -> None:
        # Claude Code may reuse the system prompt a conversation started with.
        session = SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward", executor_session_id="native-1")
        await self.run_steward(session=session)
        [call] = self.launches()
        self.assertEqual(arg(call, "--resume"), "native-1")
        self.assertIn("proj:x:sde", call["prompt"])
        other = SessionRef(session_id="s2", inbox_id=MAIN, executor_id="sde", executor_session_id="native-2")
        outcome = await self.run_steward(session=other)
        self.assertNotIn("--resume", self.launches()[-1]["argv"])
        self.assertTrue((outcome.executor_session_id or "").startswith("fake-steward-"))

    async def test_the_prompt_says_which_returns_will_never_come(self) -> None:
        # Otherwise the Steward stays silent at the last one, waiting for a result that was withheld.
        def session(**spec: Any) -> SessionRef:
            return SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward",
                              forward=ForwardSpec(target_inbox=MAIN, **spec))

        back = Event(inbox_id=MAIN, source=RETURN_SOURCE, external_id="r", 
                     payload={"text": "docs done", "hop_count": 2, "delegation_id": "b"})
        await self.run_steward(back, session=session(pending_returns=("b", "c"), cancelled_returns=("a",)))
        self.assertIn("1 will not come back", self.launches()[-1]["prompt"])
        self.assertIn("Still awaited: 1", self.launches()[-1]["prompt"])
        await self.run_steward(back, session=session(pending_returns=("b",), cancelled_returns=("a",)))
        self.assertIn("Still awaited: 0", self.launches()[-1]["prompt"])
        await self.run_steward(back, session=session(pending_returns=("b",)))
        self.assertNotIn("awaited", self.launches()[-1]["prompt"])

    async def test_launch_passes_the_permission_gate(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("block", "closed today")})
        outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "refused")
        self.assertIn("closed today", outcome.messages[0])
        self.assertEqual(self.calls(), [])

    async def test_failures_are_reported_as_failed(self) -> None:
        outcome = await self.executor({}).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.set_plan({"error": "overloaded"})
        outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.assertIn("overloaded", outcome.messages[0])
        self.set_plan({"output": {"action": "dance"}})
        outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")


def git_state(repo: Path) -> tuple[str, str, str]:
    def out(*args: str) -> str:
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout

    return out("status", "--porcelain"), out("rev-parse", "HEAD"), out("worktree", "list")


class ToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = make_repo(self.root)
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        self.sessions = SQLiteSessionStoreAdapter(db)
        self.inbox = SQLiteInboxAdapter(db)
        self.caps = Caps(self.root)
        self.tools = steward_tools.StewardTools(
            caps=self.caps, checkout=self.repo, inbox_id=MAIN, sessions=self.sessions,
            events=self.inbox, in_project=lambda inbox_id: inbox_id.startswith("proj:x"),
        )

    async def tool(self, name: str, **arguments: Any) -> tuple[bool, str]:
        text, failed = await self.tools.call(name, arguments)
        return failed, text

    async def test_forward(self) -> None:
        failed, text = await self.tool(
            "forward_to_inbox", target_inbox=SDE, text="do it", title="T", tags=["bugfix"], target_agent="sde",
            return_to=MAIN,
        )
        self.assertFalse(failed)
        self.assertIn(SDE, text)
        self.assertEqual(self.caps.forwards, [{"target_inbox": SDE, "text": "do it", "title": "T", "tags": ["bugfix"],
                                               "target_agent": "sde", "return_to": MAIN}])

    async def test_a_refused_forward_tells_the_steward_why(self) -> None:
        self.caps.refusal = ForwardRefused("hop limit reached")
        failed, text = await self.tool("forward_to_inbox", target_inbox=SDE, text="do it")
        self.assertTrue(failed)
        self.assertIn("hop limit reached", text)

    async def test_the_steward_cannot_claim_an_approval(self) -> None:
        self.caps.refusal = ForwardRefused("ask first", "block")
        failed, _ = await self.tool("forward_to_inbox", target_inbox=SDE, text="x", approved_request_id="req_1")
        self.assertTrue(failed)
        self.assertEqual(self.caps.forwards, [])

    async def test_bad_arguments_are_errors_not_crashes(self) -> None:
        for arguments in ({}, {"target_inbox": 1, "text": "x"}, {"target_inbox": SDE, "text": "x", "tags": "a"}):
            failed, _ = await self.tool("forward_to_inbox", **arguments)
            self.assertTrue(failed, arguments)
        failed, text = await self.tool("rm", path="/")
        self.assertTrue(failed)
        self.assertIn("rm", text)

    async def test_memory(self) -> None:
        self.assertFalse((await self.tool("write_memory", path="notes/a.md", content="kept"))[0])
        self.assertEqual(await self.tool("read_memory", path="notes/a.md"), (False, "kept"))
        for path in ("../escape.md", "notes/missing.md"):
            self.assertTrue((await self.tool("read_memory", path=path))[0])
        self.assertTrue((await self.tool("write_memory", path="../../escape.md", content="x"))[0])
        self.assertFalse((self.root / "escape.md").exists())

    async def test_every_tool_passes_the_permission_gate(self) -> None:
        seen: list[str] = []

        class Deny:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                seen.append(tool_name)
                return PermissionDecision("allow" if tool_name == "steward.launch" else "defer", "no")

        self.caps.permissions = Deny()
        for name, arguments in (
            ("read_memory", {"path": "index.md"}), ("write_memory", {"path": "index.md", "content": "x"}),
            ("list_sessions", {}), ("list_events", {}), ("git", {"args": ["status"]}),
            ("read_file", {"path": "calc.py"}), ("search_files", {"pattern": "x"}), ("list_files", {}),
            ("memory_search", {"query": "x"}), ("memory_read", {"uri": "index.md"}),
            ("memory_propose_write", {"target_uri": "index.md", "op": "append", "content": "x", "rationale": "r"}),
        ):
            failed, text = await self.tool(name, **arguments)
            self.assertTrue(failed, name)
            self.assertIn("no", text)
        self.assertEqual([s for s in seen if s != "steward.launch"],
                         ["read_memory", "write_memory", "list_sessions", "list_events", "git_read", "read_file",
                          "search_files", "list_files", "memory_search", "memory_read", "memory_propose_write"])
        self.assertEqual(self.caps.memory.read_on_demand("index.md"), "")

    async def test_default_policy_allows_the_stewards_tools(self) -> None:
        policy = AutoModePermissionAdapter()
        for name in ("steward.launch", "inbox.forward", "read_memory", "write_memory", "list_sessions", "list_events",
                     "git_read", "read_file", "search_files", "list_files"):
            self.assertEqual(policy.check(name, {}).action, "allow", name)

    async def test_sessions_and_events_of_this_project_only(self) -> None:
        self.sessions.save(SessionRef(session_id="mine", inbox_id=SDE, executor_id="sde", correlation_key="k"))
        self.sessions.save(SessionRef(session_id="foreign", inbox_id="proj:y", executor_id="sde"))
        for inbox_id in (SDE, "proj:y"):
            self.inbox.append(Event(inbox_id=inbox_id, source="s", external_id="e", payload={"title": f"T {inbox_id}"}))
        _, text = await self.tool("list_sessions")
        self.assertIn("mine", text)
        self.assertNotIn("foreign", text)
        _, text = await self.tool("list_events", inbox_id=SDE)
        self.assertIn(f"T {SDE}", text)
        for tool in ("list_sessions", "list_events"):
            failed, text = await self.tool(tool, inbox_id="proj:y")
            self.assertTrue(failed, tool)
        _, text = await self.tool("list_events")
        self.assertNotIn("proj:y", text)

    async def test_git_reads(self) -> None:
        failed, text = await self.tool("git", args=["log", "--oneline", "-3"])
        self.assertFalse(failed)
        self.assertIn("divide", text)
        failed, text = await self.tool("git", args=["blame", "calc.py"])
        self.assertFalse(failed, text)
        failed, text = await self.tool("git", args=["log", "no-such-ref"])
        self.assertTrue(failed)

    async def test_files(self) -> None:
        (self.repo / "docs").mkdir()
        (self.repo / "docs/new.md").write_text("divide by zero\nsecond line\n")
        failed, text = await self.tool("read_file", path="docs/new.md")
        self.assertEqual((failed, text), (False, "1\tdivide by zero\n2\tsecond line\n"))
        self.assertEqual(await self.tool("read_file", path="./docs/new.md", offset=2, limit=1), (False, "2\tsecond line\n"))
        failed, text = await self.tool("search_files", pattern="div[a-z]+ by", glob="docs/**")
        self.assertEqual((failed, text), (False, "docs/new.md:1:divide by zero\n"))
        failed, text = await self.tool("search_files", pattern="divide")
        self.assertIn("calc.py:", text)
        self.assertEqual(await self.tool("search_files", pattern="no such text"), (False, ""))
        failed, text = await self.tool("list_files")
        self.assertTrue({"calc.py", "docs/new.md"} <= set(text.split()))
        self.assertEqual(await self.tool("list_files", glob="**/*.md"), (False, "docs/new.md\n"))
        for name, arguments in (
            ("read_file", {"path": "missing.py"}), ("read_file", {"path": "docs"}), ("read_file", {}),
            ("read_file", {"path": "calc.py", "offset": 0}), ("read_file", {"path": "calc.py", "limit": "x"}),
            ("search_files", {"pattern": "("}), ("search_files", {"pattern": ""}), ("list_files", {"glob": 1}),
        ):
            self.assertTrue((await self.tool(name, **arguments))[0], (name, arguments))

    async def test_a_large_file_is_read_in_parts(self) -> None:
        (self.repo / "big.txt").write_text("".join(f"line {n}\n" for n in range(1, 5001)))
        _, text = await self.tool("read_file", path="big.txt")
        self.assertIn("2000\tline 2000\n", text)
        self.assertNotIn("line 2001", text)
        self.assertIn("offset 2001", text)
        (self.repo / "blob.bin").write_bytes(b"a\0b")
        self.assertTrue((await self.tool("read_file", path="blob.bin"))[0])

    async def test_only_the_checkouts_own_files_are_read(self) -> None:
        (self.root / "outside.txt").write_text("needle outside")
        (self.repo / ".gitignore").write_text(".env\n")
        (self.repo / ".env").write_text("needle=1")
        (self.repo / "out").symlink_to(self.root / "outside.txt")
        (self.repo / "env").symlink_to(self.repo / ".env")
        (self.repo / "up").symlink_to(self.root)
        (self.repo / "conf").symlink_to(self.repo / ".git/config")
        for path in ("../outside.txt", str(self.root / "outside.txt"), "~/x", "a/../../outside.txt", "out", "up/outside.txt",
                     ".git/config", "conf", ".env", "env", "", "x\0y"):
            failed, text = await self.tool("read_file", path=path)
            self.assertTrue(failed, path)
            self.assertNotIn("needle", text)
        _, text = await self.tool("search_files", pattern="needle")
        self.assertEqual(text, "")
        _, text = await self.tool("search_files", pattern="url|bare")
        self.assertNotIn(".git/", text)
        for glob in ("../*", str(self.root / "*"), "~/*", ":(top)../*"):
            for name, arguments in (("search_files", {"pattern": "needle", "glob": glob}), ("list_files", {"glob": glob})):
                failed, text = await self.tool(name, **arguments)
                self.assertTrue(failed or not text, (name, glob, text))
        _, text = await self.tool("list_files")
        self.assertNotIn(".env", text.split())

    async def test_a_read_of_one_file_can_be_refused(self) -> None:
        (self.repo / "secrets").mkdir()
        (self.repo / "secrets/key.txt").write_text("needle in the key\n")
        (self.repo / "open.txt").write_text("needle in the open\n")
        (self.repo / "alias").symlink_to("secrets/key.txt")
        seen: list[tuple[str, dict[str, Any]]] = []

        class Policy:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                seen.append((tool_name, args))
                private = tool_name == "read_file" and args["path"].startswith("secrets/")
                return PermissionDecision("block" if private else "allow", "private")

        self.caps.permissions = Policy()
        for path in ("secrets/key.txt", "./secrets//key.txt", "alias"):
            failed, text = await self.tool("read_file", path=path)
            self.assertTrue(failed, path)
            self.assertIn("private", text)
            self.assertNotIn("needle", text)
        self.assertEqual(seen, [("read_file", {"path": "secrets/key.txt"})] * 3)
        self.assertEqual((await self.tool("read_file", path="open.txt"))[1], "1\tneedle in the open\n")
        # A search returns file content too, so each file it found is asked for as a read.
        _, text = await self.tool("search_files", pattern="needle")
        # It does not tell either whether the pattern matches in a file that may not be read.
        self.assertEqual(text, "open.txt:1:needle in the open\n")

    async def test_a_file_that_may_not_be_read_is_not_shown_through_git_either(self) -> None:
        (self.repo / "secrets").mkdir()
        (self.repo / "secrets/key.txt").write_text("needle one\n")
        (self.repo / "open.txt").write_text("in the open\n")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "add both")
        git(self.repo, "tag", "-a", "v1", "-m", "v1")
        (self.repo / "open.txt").write_text("in the open 2\n")
        git(self.repo, "commit", "-qam", "open only")
        (self.repo / "secrets/key.txt").write_text("needle two \n")
        git(self.repo, "add", "-A")
        (self.repo / "secrets/key.txt").write_text("needle three \n")
        (self.repo / "open.txt").write_text("in the open 3\n")
        blob = subprocess.run(["git", "-C", str(self.repo), "rev-parse", "HEAD:secrets/key.txt"], check=True,
                              capture_output=True, text=True).stdout.strip()
        asked: list[str] = []

        class Policy:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                if tool_name != "read_file":
                    return PermissionDecision("allow")
                asked.append(args["path"])
                return PermissionDecision("block" if args["path"].startswith("secrets/") else "allow", "private")

        self.caps.permissions = Policy()
        self.assertTrue((await self.tool("read_file", path="secrets/key.txt"))[0])
        self.assertNotIn("needle", (await self.tool("search_files", pattern="needle"))[1])
        for args in (
            ["show", "HEAD:secrets/key.txt"], ["show", "v1^{commit}:secrets/key.txt"], ["show", "HEAD:./secrets/key.txt"],
            ["show", "HEAD^{/add :both}:secrets/key.txt"], ["show", blob], ["show", blob[:12]], ["show", "HEAD~1"],
            ["show", "v1"], ["show", "-p", "HEAD~1"], ["show", "--stat", "-p", "HEAD~1"], ["show", "HEAD", "HEAD~1"],
            ["show", "HEAD~2..HEAD"], ["show", "--patch-with-stat", "HEAD~1"], ["show", "HEAD~1", "--", "secrets"],
            ["diff"], ["diff", "--cached"], ["diff", "HEAD"], ["diff", "HEAD~2", "HEAD"], ["diff", "HEAD~2..HEAD"],
            ["diff", "--check"], ["diff", "--", "secrets/key.txt"], ["diff", "--", "*.txt"], ["diff", blob, "HEAD:open.txt"],
            ["diff", "--stat", "-U1"], ["diff", "--word-diff", "HEAD"],
            ["blame", "secrets/key.txt"], ["blame", "HEAD", "--", "secrets/key.txt"], ["blame", "secrets/key.txt", "HEAD"],
            ["blame", "./secrets/key.txt"], ["blame", "-L1,1", "secrets/key.txt"], ["blame", "--", "secrets/key.txt"],
            ["log", "-p"], ["log", "-u"], ["log", "--patch"], ["log", "--patc"], ["log", "-U1"], ["log", "-c", "-p"],
            ["log", "-Sneed"], ["log", "-G", "need"], ["log", "-L1,1:secrets/key.txt"], ["log", "--full-diff", "-p"],
            ["log", "--patch-with-raw"], ["log", "--stat", "-p"], ["log", "--cc"], ["log", "--remerge-diff"],
            ["status", "-v"], ["status", "-vv"], ["status", "--verbose"], ["rev-parse", "HEAD:secrets/key.txt"],
        ):
            failed, text = await self.tool("git", args=args)
            self.assertNotIn("needle", text, args)
            self.assertTrue(failed, (args, text))
        for args, expected in (
            # What the gate allows to be read is still shown ...
            (["show", "HEAD"], "+in the open 2"), (["show", "HEAD:open.txt"], "in the open 2"),
            (["show", "HEAD~1", "--", "open.txt"], "+in the open"), (["diff", "--", "open.txt"], "+in the open 3"),
            (["blame", "open.txt"], "in the open 3"), (["blame", "HEAD", "--", "open.txt"], "in the open 2"),
            (["diff", "HEAD~1", "HEAD"], "+in the open 2"),
            # ... and so is everything about the history that is not content.
            (["log", "--stat", "-3"], "secrets/key.txt"), (["show", "--stat", "HEAD~1"], "secrets/key.txt"),
            (["show", "--no-patch", "HEAD~1"], "add both"), (["diff", "--name-status"], "secrets/key.txt"),
            (["log", "--oneline", "--", "secrets/key.txt"], "add both"), (["status", "--short"], "secrets/key.txt"),
            (["show", "--name-only", "v1"], "secrets/key.txt"),
        ):
            failed, text = await self.tool("git", args=args)
            self.assertFalse(failed, (args, text))
            self.assertIn(expected, text, args)
            self.assertNotIn("needle", text, args)
        self.assertIn("open.txt", asked)

    async def test_search_judges_the_whole_path_whatever_is_in_it(self) -> None:
        (self.repo / "private\npublic.txt").write_text("needle one\n")
        (self.repo / "private\rpublic.txt").write_text("needle two\n")
        (self.repo / "private\n7").write_text("needle three\n")
        (self.repo / "public.txt").write_text("needle four\rneedle five\r\nneedle six\n")
        (self.repo / "open\nname.txt").write_text("needle seven\n")
        git(self.repo, "add", "-A")
        judged: list[str] = []

        class Policy:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                if tool_name == "read_file":
                    judged.append(args["path"])
                    return PermissionDecision("block" if args["path"].startswith("private") else "allow", "private")
                return PermissionDecision("allow", "")

        self.caps.permissions = Policy()
        failed, text = await self.tool("search_files", pattern="needle")
        self.assertFalse(failed, text)
        self.assertEqual(set(judged), {"private\npublic.txt", "private\rpublic.txt", "private\n7", "public.txt",
                                       "open\nname.txt"})
        for hidden in ("one", "two", "three"):
            self.assertNotIn(f"needle {hidden}", text)
        for shown in ("four", "five", "six", "seven"):
            self.assertIn(f"needle {shown}", text)
        self.assertIn('"open\\nname.txt":1:needle seven', text)

    async def test_search_shows_nothing_it_cannot_tell_the_file_of(self) -> None:
        for record in (b"no record", b"a.txt\0x\0needle\n", b"a.txt\x001\0needle"):
            with self.subTest(record), patch.object(steward_tools, "run_git", return_value=record):
                failed, text = await self.tool("search_files", pattern="needle")
                self.assertTrue(failed, text)
                self.assertNotIn("needle\n", text)
        with patch.object(steward_tools, "run_git", return_value=b"bad\xff.txt\x001\0needle x\ncalc.py\x001\0needle a\0b\ncalc.py\x002:needle c\n"):
            self.assertEqual(await self.tool("search_files", pattern="needle"),
                             (False, "calc.py:1:needle a\0b\ncalc.py:2:needle c\n"))

    async def test_a_path_in_history_shows_only_a_file(self) -> None:
        (self.repo / "secrets").mkdir()
        (self.repo / "secrets/key.txt").write_text("needle\n")
        (self.repo / "open.txt").write_text("in the open\n")
        (self.repo / "link").symlink_to("open.txt")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "add both")
        commit = subprocess.run(["git", "-C", str(self.repo), "rev-parse", "HEAD"], check=True, capture_output=True,
                                text=True).stdout.strip()
        # A submodule entry whose commit happens to be in this repository.
        git(self.repo, "update-index", "--add", "--cacheinfo", f"160000,{commit},vendor/mod")
        git(self.repo, "commit", "-qm", "add a submodule")

        class Policy:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                private = tool_name == "read_file" and args["path"].startswith("secrets/")
                return PermissionDecision("block" if private else "allow", "private")

        self.caps.permissions = Policy()
        for path in ("vendor/mod", "vendor", "secrets", "link", "missing.txt", "open.txt/"):
            failed, text = await self.tool("git", args=["show", f"HEAD:{path}"])
            self.assertTrue(failed, (path, text))
            self.assertNotIn("needle", text, path)
        self.assertEqual(await self.tool("git", args=["show", "HEAD:open.txt"]), (False, "in the open\n"))
        # In a patch a submodule is its commit id, whatever the configuration asks for.
        git(self.repo, "config", "diff.submodule", "log")
        for args in (["show", "HEAD"], ["diff", "HEAD~1", "HEAD"], ["diff"], ["show", "HEAD", "--stat", "--summary"],
                     ["diff", "--stat", "HEAD~1", "HEAD"], ["log", "--stat", "-n", "1"]):
            failed, text = await self.tool("git", args=args)
            self.assertFalse(failed, text)
            self.assertNotIn("needle", text, args)

    async def test_git_runs_no_configured_program(self) -> None:
        marker = self.root / "ran"
        converter = self.root / "convert"
        converter.write_text(f"#!/bin/sh\ntouch {marker}\necho CONVERTED\n")
        converter.chmod(0o755)
        (self.repo / ".gitattributes").write_text("*.txt diff=up filter=a=b.c\n")
        (self.repo / "open.txt").write_text("raw line\n")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "add it")
        (self.repo / "open.txt").write_text("raw line 2\n")
        for key in ("diff.up.textconv", "diff.up.command", "filter.a=b.c.clean", "filter.a=b.c.process"):
            git(self.repo, "config", key, str(converter))
        git(self.repo, "config", "filter.a=b.c.required", "true")
        for args, expected in (
            (["blame", "open.txt"], "raw line 2"), (["blame", "HEAD", "--", "open.txt"], "raw line"),
            (["diff"], "+raw line 2"), (["diff", "--stat"], "open.txt"), (["status", "--short"], "M open.txt"),
            (["show", "HEAD"], "+raw line"), (["show", "HEAD:open.txt"], "raw line"), (["log", "--stat"], "open.txt"),
        ):
            failed, text = await self.tool("git", args=args)
            self.assertFalse(failed, (args, text))
            self.assertIn(expected, text, args)
            self.assertNotIn("CONVERTED", text, args)
            self.assertFalse(marker.exists(), args)
        self.assertFalse((await self.tool("search_files", pattern="raw"))[0])
        self.assertFalse((await self.tool("list_files"))[0])
        self.assertFalse(marker.exists())

    async def test_git_never_enters_a_submodule(self) -> None:
        inner = self.root / "inner"
        marker = self.root / "entered"
        hook = self.root / "monitor"
        hook.write_text(f"#!/bin/sh\ntouch {marker}\n")
        hook.chmod(0o755)
        subprocess.run(["git", "init", "-q", str(inner)], check=True)
        (inner / "f").write_text("x\n")
        for args in (["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@x", "commit", "-qm", "x"]):
            subprocess.run(["git", "-C", str(inner), *args], check=True)
        git(self.repo, "-c", "protocol.file.allow=always", "submodule", "--quiet", "add", str(inner), "mod")
        git(self.repo, "commit", "-qam", "add the submodule")
        git(self.repo / "mod", "config", "filter.x.clean", str(hook))
        (self.repo / "mod/.gitattributes").write_text("* filter=x\n")
        (self.repo / "mod/f").write_text("y\n")
        for args in (["status"], ["diff"], ["diff", "--stat"], ["diff", "HEAD"]):
            failed, text = await self.tool("git", args=args)
            self.assertFalse(failed, (args, text))
            self.assertFalse(marker.exists(), args)

    async def test_a_merge_shows_only_files_that_may_be_read(self) -> None:
        (self.repo / "key.txt").write_text("base\n")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "base")
        base = subprocess.run(["git", "branch", "--show-current"], cwd=self.repo, check=True, capture_output=True,
                              text=True).stdout.strip()
        git(self.repo, "checkout", "-qb", "side")
        (self.repo / "key.txt").write_text("needle side\n")
        git(self.repo, "commit", "-qam", "side")
        git(self.repo, "checkout", "-q", base)
        (self.repo / "key.txt").write_text("needle main\n")
        git(self.repo, "commit", "-qam", "main")
        subprocess.run(["git", "merge", "-q", "side"], cwd=self.repo, capture_output=True, check=False)
        (self.repo / "key.txt").write_text("needle merged\n")
        self.caps.permissions = InMemoryPermission()
        self.assertIn("needle", (await self.tool("git", args=["diff"]))[1])
        denied = InMemoryPermission({"read_file": PermissionDecision("defer", "private")})
        self.caps.permissions = denied
        self.assertNotIn("needle", (await self.tool("git", args=["diff"]))[1])
        git(self.repo, "commit", "-qam", "merged")
        failed, text = await self.tool("git", args=["show", "HEAD"])
        self.assertTrue(failed)
        self.assertNotIn("needle", text)
        self.caps.permissions = InMemoryPermission()
        self.assertIn("needle merged", (await self.tool("git", args=["show", "HEAD"]))[1])

    async def test_git_writes_are_refused(self) -> None:
        before = git_state(self.repo)
        for args in (["commit", "-m", "x"], ["checkout", "main"], ["branch", "x"], ["diff", "--output=leak"], [], ["-c", "x"]):
            failed, _ = await self.tool("git", args=args)
            self.assertTrue(failed, args)
        self.assertEqual(git_state(self.repo), before)
        self.assertFalse((self.repo / "leak").exists())

    async def test_git_does_not_tell_whether_a_file_outside_the_checkout_exists(self) -> None:
        (self.root / "outside.txt").write_text("secret")
        for rev in (f"HEAD:{self.root}/outside.txt", "HEAD:../outside.txt", ":../outside.txt", "HEAD:a/../../outside.txt"):
            failed, text = await self.tool("git", args=["show", rev])
            self.assertTrue(failed)
            self.assertTrue(text.startswith("git: ") and text.endswith("is outside the checkout"), text)

    def test_git_argument_rules(self) -> None:
        for args in (["status", "--short"], ["diff", "main...HEAD", "--", "src/a.py"], ["log", "--oneline", "a..b"],
                     ["show", "HEAD:calc.py"], ["diff", "HEAD..origin/main"], ["rev-parse", "HEAD"], ["merge-base", "a", "b"],
                     ["log", "-5", "-n", "3"], ["diff", "-U3", "-w"], ["blame", "-w", "-L1,/S/", "a"],
                     ["blame", "--ignore-rev", "abc", "a"],
                     ["show", "HEAD~1:src/a.py"], ["log", "--format=%h: %s"], ["log", "--since=2026-01-01 10:00"],
                     ["diff", "origin/main...HEAD"], ["show", "HEAD@{0}:a"]):
            check_git_args(args)
        for args in (["push"], ["diff", "--output", "x"], ["log", "--outp=x"], ["diff", "--ext-diff"], ["diff", "--ext"],
                     ["diff", "--no-index", "a", "b"], ["diff", "../x", "a"], ["diff", "/etc/passwd", "a"],
                     ["show", "--textconv"], ["blame", "--contents", "/etc/passwd"], ["diff", "-O/etc/passwd"],
                     ["log", "--show-signature"], ["diff", "a/../../b"], ["diff", "~/x", "a"], ["grep", "-O", "x"],
                     ["blame", "--ignore-revs-file=../x", "a"], ["blame", "--ignore-revs-f", "x", "a"],
                     ["blame", "-S../x", "a"], ["blame", "-wS", "/x", "a"], ["diff", "-pO/x"],
                     ["show", "HEAD:../x"], ["show", "HEAD:foo/../../x"], ["show", ":../x"], ["show", ":x"],
                     ["show", "HEAD:/etc/passwd"], ["show", "HEAD:~/x"], ["diff", "HEAD:../x", "HEAD:a"],
                     ["diff", "--", ":(top)../x"], ["diff", "a..HEAD:../x"], ["show", "HEAD:.."],
                     ["rev-parse", "HEAD:/etc/passwd"], ["log", "-L1,2:/etc/passwd"], ["log", "-SOauth"],
                     ["log", "--grep", "x"], ["log", "-n"], ["blame", "-L"], ["blame", "a", "b", "c"], ["blame"],
                     ["diff", "a", "b", "c"], ["log", "HEAD:a"], ["show", "HEAD:a", "HEAD:b"], ["diff", "--ext-diff=x"]):
            with self.assertRaises(GitRefused, msg=str(args)):
                check_git_args(args)


class SuspendCase(StewardCase):
    def suspended(self, outcome: Suspend, native: str | None = "native-1") -> SessionRef:
        return SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward", executor_session_id=native,
                          status="suspended", checkpoint=outcome.session_checkpoint,
                          pending_request_id=outcome.request_id)

    def decision(self, outcome: Suspend, approved: bool) -> Event:
        return Event(inbox_id=MAIN, source="switchboard:feishu", external_id="d", payload={
            "event_type": "approval.decided", "approval_request_id": outcome.request_id, "approved": approved,
        })


class SuspendTests(SuspendCase):
    async def test_a_deferred_launch_waits_and_then_runs_the_original_event(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        outcome = await self.run_steward(self.event("triage this"))
        assert isinstance(outcome, Suspend)
        self.assertEqual(self.calls(), [])
        # Anything but the decision keeps the same request open and is not lost.
        again = await self.run_steward(self.event("one more thing"), session=self.suspended(outcome))
        assert isinstance(again, Suspend)
        self.assertEqual(again.request_id, outcome.request_id)
        self.set_plan({"output": {"action": "reply", "message": "done"}})
        final = await self.run_steward(self.decision(again, True), session=self.suspended(again))
        self.assertIsInstance(final, Emit)
        [call] = self.launches()
        self.assertIn("triage this", call["prompt"])
        self.assertIn("one more thing", call["prompt"])

    async def test_an_approved_launch_is_judged_again(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        outcome = await self.run_steward(self.event("triage this"))
        assert isinstance(outcome, Suspend)
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("block", "closed today")})
        final = await self.run_steward(self.decision(outcome, True), session=self.suspended(outcome))
        assert isinstance(final, Emit)
        self.assertEqual(final.status, "refused")
        self.assertIn("closed today", final.messages[0])
        self.assertEqual(self.calls(), [])

    async def test_an_approval_holds_only_for_the_launch_it_was_asked_for(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        other = self.root / "other"
        other.mkdir()
        fake = fake_command(self.root)
        variants = {
            "checkout": ({MAIN: str(other)}, {}),
            "launcher": (None, {"launcher": ClaudeCodeLauncher(name="second", command=fake)}),
            "command": (None, {"launcher": ClaudeCodeLauncher(name="fake", command=f"{self.root}/elsewhere")}),
            "launcher env": (None, {"launcher": ClaudeCodeLauncher(name="fake", command=fake, env={"A": "1"})}),
            "env": (None, {"env": {"A": "secret-value"}}),
            "model": (None, {"model": "claude-opus-5-5"}),
        }
        for name, (checkouts, changed) in variants.items():
            with self.subTest(name):
                outcome = await self.run_steward(self.event("triage this"))
                assert isinstance(outcome, Suspend)
                executor = self.executor(checkouts)
                executor._profile = replace(executor._profile, **changed)  # pyright: ignore[reportPrivateUsage]
                asked = await executor.execute([self.decision(outcome, True)], self.suspended(outcome), self.caps)
                assert isinstance(asked, Suspend)
                self.assertNotEqual(asked.request_id, outcome.request_id)
                self.assertEqual(self.calls(), [])
                self.assertNotIn("secret-value", asked.session_checkpoint)
                # The new request is for the launch as it is now, and keeps what it was for.
                if name in ("model", "checkout"):
                    final = await executor.execute([self.decision(asked, True)], self.suspended(asked), self.caps)
                    self.assertIsInstance(final, Silent)
                    self.assertIn("triage this", self.launches()[-1]["prompt"])
                    self.log.unlink()

    async def test_a_request_from_before_the_arguments_were_kept_is_asked_again(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        outcome = await self.run_steward()
        assert isinstance(outcome, Suspend)
        old = json.loads(outcome.session_checkpoint)
        del old["launch"]
        session = replace(self.suspended(outcome), checkpoint=json.dumps(old))
        self.assertIsInstance(await self.run_steward(self.decision(outcome, True), session=session), Suspend)
        self.assertEqual(self.calls(), [])

    async def test_a_second_attempt_is_judged_too(self) -> None:
        answers = [PermissionDecision("allow", ""), PermissionDecision("block", "closed meanwhile")]

        class Policy:
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                return answers.pop(0)

        self.caps.permissions = Policy()
        self.set_plan({"error": "no such conversation"}, {"output": {"action": "reply", "message": "hello"}})
        session = SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward", executor_session_id="gone")
        outcome = await self.run_steward(session=session)
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "refused")
        self.assertEqual(len(self.launches()), 1)

    async def test_a_rejected_launch_stops(self) -> None:
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        outcome = await self.run_steward()
        assert isinstance(outcome, Suspend)
        final = await self.run_steward(self.decision(outcome, False), session=self.suspended(outcome))
        assert isinstance(final, Emit)
        self.assertEqual(final.status, "stopped")
        self.assertEqual(self.calls(), [])

    async def deferred_forward(self) -> Suspend:
        self.caps.refusal = ForwardRefused("ask first", "defer")
        self.set_plan({"calls": [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "do it",
                                                                           "return_to": MAIN}}],
                       "output": {"action": "reply", "message": "handed to sde"}})
        outcome = await self.run_steward()
        assert isinstance(outcome, Suspend)
        self.assertEqual(self.caps.forwards, [])
        self.assertIn(SDE, outcome.prompt)
        self.assertIn("do it", outcome.prompt)
        return outcome

    async def test_a_deferred_forward_goes_out_once_approved(self) -> None:
        outcome = await self.deferred_forward()
        final = await self.run_steward(self.decision(outcome, True), session=self.suspended(outcome))
        assert isinstance(final, Emit)
        self.assertEqual(final.messages, ("handed to sde",))
        self.assertEqual(self.caps.forwards, [{"target_inbox": SDE, "text": "do it", "return_to": MAIN,
                                               "approved_request_id": outcome.request_id}])
        self.assertEqual(len(self.launches()), 1)

    async def test_a_rejected_forward_goes_back_to_the_steward(self) -> None:
        outcome = await self.deferred_forward()
        self.set_plan({"output": {"action": "reply", "message": "not handed over"}})
        final = await self.run_steward(self.decision(outcome, False), session=self.suspended(outcome))
        assert isinstance(final, Emit)
        self.assertEqual((final.messages, self.caps.forwards), (("not handed over",), []))
        self.assertIn("declined", self.launches()[-1]["prompt"])

    async def test_messages_during_the_wait_are_handled_after_the_decision(self) -> None:
        outcome = await self.deferred_forward()
        again = await self.run_steward(self.event("also update the docs"), session=self.suspended(outcome))
        assert isinstance(again, Suspend)
        self.set_plan({"output": {"action": "silent", "message": ""}})
        final = await self.run_steward(self.decision(again, True), session=self.suspended(again))
        self.assertIsInstance(final, Silent)
        self.assertEqual(len(self.caps.forwards), 1)
        self.assertIn("also update the docs", self.launches()[-1]["prompt"])


class TerminalTests(SuspendCase):
    # The Steward's claude runs in an Orca terminal, never as a child of this process: FakeOrcaTerminals runs the
    # terminal's command in a real shell, so the wrapper, the session check and the fake claude all really run.
    def handles(self) -> list[str]:
        return list(self.orca.terminals)

    def assert_closed(self, count: int = 1) -> None:
        self.assertEqual(len(self.handles()), count)
        for handle in self.handles():
            self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
            self.assertIsNotNone(self.orca.terminals[handle]["proc"].returncode)
        self.assertEqual(list(self.runs.iterdir()), [])

    async def all_gone(self, pidfile: Path) -> None:
        pids = [int(p) for p in pidfile.read_text().split()]
        self.assertEqual(len(pids), 2)
        self.assertTrue(await until(lambda: not any(alive(p) for p in pids)), pids)

    async def test_it_runs_in_an_orca_terminal_and_starts_no_subprocess(self) -> None:
        self.set_plan({"output": {"action": "reply", "message": "on it"}})
        started: list[tuple[Any, ...]] = []
        real = asyncio.create_subprocess_exec

        async def record(*args: Any, **kwargs: Any) -> Any:
            started.append(args)
            return await real(*args, **kwargs)

        with patch("asyncio.create_subprocess_exec", new=record), \
                patch.object(claude_code, "run_claude_code", side_effect=AssertionError("started as a subprocess")):
            outcome = await self.run_steward(self.event("the secret task"))
        assert isinstance(outcome, Emit)
        self.assertEqual((outcome.messages, outcome.status), (("on it",), "done"))
        [handle] = self.handles()
        term = self.orca.terminals[handle]
        # The only process started is the fake Orca's own terminal shell.
        self.assertEqual(started, [("/bin/sh", "-c", term["command"])])
        self.assertEqual(Path(term["worktree"]), self.repo)
        self.assertEqual(term["title"], "steward · proj:x")
        self.assertNotIn("the secret task", term["command"])
        self.assertNotIn("You are steward", term["command"])
        [call] = self.launches()
        self.assertEqual(call["argv"][:3], ["-p", "--output-format", "json"])
        self.assertNotIn("--verbose", call["argv"])
        self.assertIn("the secret task", call["prompt"])
        self.assertFalse(any("the secret task" in a for a in call["argv"]))
        self.assert_closed()

    async def test_the_arguments_reach_claude_unchanged(self) -> None:
        session = SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward", executor_session_id="native-1")
        self.set_plan({"calls": [{"tool": "read_file", "arguments": {"path": "calc.py"}}],
                       "output": {"action": "reply", "message": "read it"}})
        outcome = await self.run_steward(session=session)
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.executor_session_id, "native-1")
        [call] = self.launches()
        self.assertEqual(arg(call, "--resume"), "native-1")
        self.assertEqual(json.loads(arg(call, "--json-schema")), STEWARD_SCHEMA)
        self.assertIn("You are steward", arg(call, "--append-system-prompt"))
        self.assertEqual((arg(call, "--model"), arg(call, "--effort"), arg(call, "--permission-mode")),
                         ("claude-sonnet-5-5", "medium", "dontAsk"))
        server = json.loads(arg(call, "--mcp-config"))["mcpServers"]["hibernaut_steward"]
        self.assertEqual(server["args"][0], "-I")
        self.assertTrue(server["args"][1].endswith("steward_mcp.py"))
        # The tools answered over that server's socket.
        self.assertIn("divide", self.answers()[-1][1])

    async def test_a_prompt_longer_than_an_argument_may_be(self) -> None:
        await self.run_steward(self.event("x" * 400_000))
        self.assertIn("x" * 400_000, self.launches()[0]["prompt"])

    async def test_a_verbose_result_is_read_too(self) -> None:
        self.set_plan({"output": {"action": "reply", "message": "on it"}})
        with patch.dict(os.environ, {"FAKE_VERBOSE": "1"}):
            outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.messages, ("on it",))

    async def test_the_environment_keeps_its_rules(self) -> None:
        # The terminal's own credentials and routing go; what the launcher and the executor set stays, a gateway
        # for a model that is not Claude's included.
        inherited = {"ANTHROPIC_API_KEY": "paid", "CLAUDE_CODE_OAUTH_TOKEN": "t", "CLAUDE_CODE_USE_BEDROCK": "1",
                     "ANTHROPIC_BASE_URL": "http://elsewhere", "ORCA_PANE_KEY": "pane", "ORCA_TAB_ID": "tab",
                     "ORCA_AGENT_HOOK_PORT": "1", "ORCA_AGENT_LAUNCH_ID": "l", "ORCA_WORKTREE_ID": "wt"}
        executor = self.executor()
        executor._profile = replace(  # pyright: ignore[reportPrivateUsage]
            executor._profile, model="codex/gpt-6.2", env={"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425",  # pyright: ignore[reportPrivateUsage]
                                                           "ANTHROPIC_AUTH_TOKEN": "gateway"},
            launcher=replace(executor._profile.launcher, env={"FAKE_MARK": "launcher"}))  # pyright: ignore[reportPrivateUsage]
        with patch.dict(os.environ, {**inherited, "FAKE_API_KEY_SOURCE": "ANTHROPIC_AUTH_TOKEN"}):
            outcome = await executor.execute([self.event()], None, self.caps)
        self.assertIsInstance(outcome, Silent)
        [call] = self.launches()
        self.assertEqual((call["paid_env"], call["base_url"], call["auth_token"], call["mark"]),
                         (["ANTHROPIC_AUTH_TOKEN"], "http://127.0.0.1:3425", "gateway", "launcher"))
        self.assertEqual(arg(call, "--model"), "codex/gpt-6.2")
        # Orca must not register this session, or it reopens it once the process ends.
        self.assertNotIn("ORCA_PANE_KEY", call["orca_env"])
        self.assertEqual([k for k in call["orca_env"] if k in inherited], ["ORCA_WORKTREE_ID"])

    async def test_a_session_on_another_account_or_model_is_stopped_before_it_runs(self) -> None:
        for env, expected in (({"FAKE_API_KEY_SOURCE": "ANTHROPIC_API_KEY"}, "billing is 'subscription'"),
                              ({"FAKE_MODEL": "claude-haiku-4-5"}, "Claude Code selected 'claude-haiku-4-5'")):
            with self.subTest(expected), patch.dict(os.environ, env):
                outcome = await self.run_steward()
                assert isinstance(outcome, Emit)
                self.assertEqual(outcome.status, "failed")
                self.assertIn(expected, outcome.messages[0])
                self.assertEqual(self.launches(), [])
        self.assert_closed(2)

    async def test_a_failed_run_closes_its_terminal(self) -> None:
        self.set_plan({"error": "overloaded"}, {"exit": 3})
        for expected in ("overloaded", "exited (3) without a result: crashed"):
            outcome = await self.run_steward()
            assert isinstance(outcome, Emit)
            self.assertEqual(outcome.status, "failed")
            self.assertIn(expected, outcome.messages[0])
        self.assert_closed(2)

    async def test_a_launcher_that_does_not_start_is_a_failure(self) -> None:
        executor = self.executor()
        executor._profile = replace(executor._profile, launcher=ClaudeCodeLauncher(  # pyright: ignore[reportPrivateUsage]
            name="gone", command=str(self.root / "no-such-claude")))
        outcome = await executor.execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.assertIn("no-such-claude", outcome.messages[0])
        self.assert_closed()

    async def test_a_timeout_closes_the_terminal_and_ends_claude(self) -> None:
        pids = self.root / "pids"
        self.set_plan({"pidfile": str(pids)})
        executor = self.executor()
        executor._profile = replace(executor._profile, timeout_seconds=6.0)  # pyright: ignore[reportPrivateUsage]
        outcome = await expire_when_ready(
            executor.execute([self.event()], None, self.caps), 6.0,
            lambda: pids.exists() and len(pids.read_text().split()) == 2,
        )
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.assertIn("timed out after 6s", outcome.messages[0])
        self.assert_closed()
        await self.all_gone(pids)

    async def test_an_interrupt_closes_the_terminal_and_ends_claude(self) -> None:
        pids = self.root / "pids"
        self.set_plan({"pidfile": str(pids)})
        self.caps.interrupted = lambda: pids.exists() and len(pids.read_text().split()) == 2
        with self.assertRaises(asyncio.CancelledError):
            await self.run_steward()
        self.assert_closed()
        await self.all_gone(pids)

    async def test_claude_ends_even_when_orca_loses_the_close(self) -> None:
        pids = self.root / "pids"
        self.set_plan({"pidfile": str(pids)})
        self.orca.lose_closes = True
        self.caps.interrupted = lambda: pids.exists() and len(pids.read_text().split()) == 2
        with self.assertRaises(asyncio.CancelledError):
            await self.run_steward()
        await self.all_gone(pids)

    async def test_without_orca_the_run_waits_for_a_person_instead_of_a_subprocess(self) -> None:
        async def broken(*args: str, cwd: Path) -> dict[str, Any]:
            raise RuntimeError("Orca is not running")

        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=broken):
            outcome = await self.run_steward(self.event("triage this"))
            assert isinstance(outcome, Suspend)
            self.assertIn("Orca is not running", outcome.prompt)
            # Still unavailable after the approval: asked again, nothing lost.
            again = await self.run_steward(self.decision(outcome, True), session=self.suspended(outcome))
            assert isinstance(again, Suspend)
            self.assertNotEqual(again.request_id, outcome.request_id)
        self.assertEqual(self.calls(), [])
        waiting = await self.run_steward(self.event("one more thing"), session=self.suspended(again))
        assert isinstance(waiting, Suspend)
        self.set_plan({"output": {"action": "reply", "message": "done"}})
        final = await self.run_steward(self.decision(waiting, True), session=self.suspended(waiting))
        assert isinstance(final, Emit)
        self.assertEqual(final.messages, ("done",))
        [call] = self.launches()
        self.assertIn("triage this", call["prompt"])
        self.assertIn("one more thing", call["prompt"])
        self.assertEqual(arg(call, "--resume"), "native-1")

    async def test_a_rejected_retry_stops(self) -> None:
        self.terminal = TerminalUnavailable("project 'x' has terminal_backend = \"none\"")
        outcome = await self.run_steward()
        assert isinstance(outcome, Suspend)
        self.assertIn("terminal_backend = \"none\"", outcome.prompt)
        final = await self.run_steward(self.decision(outcome, False), session=self.suspended(outcome))
        assert isinstance(final, Emit)
        self.assertEqual(final.status, "stopped")
        self.assertEqual((self.calls(), self.orca.calls), ([], []))

    async def test_a_retry_approval_does_not_stand_in_for_a_launch_approval(self) -> None:
        available, self.terminal = self.terminal, TerminalUnavailable("no terminal")
        outcome = await self.run_steward()
        assert isinstance(outcome, Suspend)
        self.terminal = available
        self.caps.permissions = InMemoryPermission({"steward.launch": PermissionDecision("defer", "ask")})
        asked = await self.run_steward(self.decision(outcome, True), session=self.suspended(outcome))
        assert isinstance(asked, Suspend)
        self.assertEqual((asked.prompt, self.calls()), ("ask", []))
        # An approved launch that met no terminal keeps its approval.
        self.terminal = TerminalUnavailable("no terminal")
        unopened = await self.run_steward(self.decision(asked, True), session=self.suspended(asked))
        assert isinstance(unopened, Suspend)
        self.assertIn("no terminal", unopened.prompt)
        self.terminal = available
        final = await self.run_steward(self.decision(unopened, True), session=self.suspended(unopened))
        self.assertIsInstance(final, Silent)

    async def test_a_terminal_lost_during_the_run_waits_for_a_person(self) -> None:
        pids = self.root / "pids"
        self.set_plan({"pidfile": str(pids)})
        task = asyncio.create_task(self.run_steward(self.event("triage this")))
        self.assertTrue(await until(lambda: pids.exists() and len(pids.read_text().split()) == 2))
        [handle] = self.handles()
        await self.orca.close(handle)
        outcome = await task
        assert isinstance(outcome, Suspend)
        self.assertIn(f"Orca terminal {handle} was closed before", outcome.prompt)
        await self.all_gone(pids)

    async def test_a_terminal_lost_after_a_forward_is_a_failure(self) -> None:
        # A failed Run sends none of its forwards; a suspended one would, and the retry would send them again.
        pids = self.root / "pids"
        self.set_plan({"calls": [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "do it"}}],
                       "pidfile_after": str(pids)})
        task = asyncio.create_task(self.run_steward())
        self.assertTrue(await until(lambda: pids.exists() and len(pids.read_text().split()) == 2))
        await self.orca.close(self.handles()[0])
        outcome = await task
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.assertIn("was closed before", outcome.messages[0])


class WorkdirTests(SuspendCase):
    def setUp(self) -> None:
        super().setUp()
        self.other = self.root / "other"
        git(self.root, "clone", "-q", str(self.repo), str(self.other))
        (self.other / "only-here.md").write_text("the other directory\n")

    async def test_the_configured_directory_replaces_the_checkout(self) -> None:
        self.set_plan({"calls": [{"tool": "read_file", "arguments": {"path": "only-here.md"}}]})
        outcome = await self.executor(workdir=str(self.other)).execute([self.event()], None, self.caps)
        self.assertIsInstance(outcome, Silent)
        [call] = self.launches()
        self.assertEqual(os.path.realpath(call["cwd"]), str(self.other))
        self.assertIn("the other directory", self.answers()[-1][1])
        [term] = self.orca.created()
        self.assertEqual(Path(term["worktree"]), self.other)

    async def test_a_missing_directory_is_named(self) -> None:
        outcome = await self.executor(workdir=str(self.root / "gone")).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertEqual(outcome.status, "failed")
        self.assertIn(str(self.root / "gone"), outcome.messages[0])

    async def test_another_branch_waits_for_a_person_and_is_not_switched(self) -> None:
        git(self.other, "checkout", "-qb", "elsewhere")
        before = git_state(self.other)
        options = {"workdir": str(self.other), "branch": "release"}
        outcome = await self.executor(**options).execute([self.event("triage this")], None, self.caps)
        assert isinstance(outcome, Suspend)
        for part in (str(self.other), "'elsewhere'", "'release'"):
            self.assertIn(part, outcome.prompt)
        self.assertEqual((git_state(self.other), self.calls(), self.orca.calls), (before, [], []))
        # Checked again before every run, the approved one included.
        again = await self.executor(**options).execute([self.decision(outcome, True)], self.suspended(outcome), self.caps)
        assert isinstance(again, Suspend)
        git(self.other, "checkout", "-qb", "release")
        final = await self.executor(**options).execute([self.decision(again, True)], self.suspended(again), self.caps)
        self.assertIsInstance(final, Silent)
        self.assertIn("triage this", self.launches()[-1]["prompt"])

    async def test_a_directory_on_no_branch_waits_too(self) -> None:
        git(self.other, "checkout", "-q", "--detach")
        plain = self.root / "plain"
        plain.mkdir()
        for workdir, expected in ((self.other, "no branch"), (plain, "git")):
            outcome = await self.executor(workdir=str(workdir), branch="main").execute([self.event()], None, self.caps)
            assert isinstance(outcome, Suspend)
            self.assertIn(expected, outcome.prompt)
        self.assertEqual(self.calls(), [])


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


class RegisteredReposTests(unittest.TestCase):
    def test_the_repositories_come_from_the_orca_command(self) -> None:
        bin_dir = Path(self.enterContext(tempfile.TemporaryDirectory())) / "bin"
        bin_dir.mkdir()
        script = bin_dir / "orca"

        def answer(body: str) -> None:
            script.write_text(f"#!/bin/sh\n[ \"$*\" = 'repo list --json' ] || exit 9\n{body}\n")
            script.chmod(0o700)

        with patch.dict(os.environ, {"PATH": f"{bin_dir}:/usr/bin:/bin"}):
            answer("""echo '{"ok": true, "result": {"repos": [{"path": "/r"}]}}'""")
            self.assertEqual(orca.registered_repos(), [{"path": "/r"}])
            for body, expected in (("echo broken >&2; exit 1", "broken"), ("echo nonsense", ""),
                                   ("""echo '{"ok": false}'""", "")):
                answer(body)
                with self.assertRaisesRegex(RuntimeError, f"orca repo list.*{expected}"):
                    orca.registered_repos()
            script.unlink()
            with self.assertRaisesRegex(RuntimeError, "orca repo list"):
                orca.registered_repos()


CONFIG = """
[runtime]
db_path = "{root}/x.db"
memory_root = "{root}/memory"
default_agent = "steward"
worktree_sweep_interval_seconds = 0

[launchers.fake]
command = "{command}"

[executors.claude-code]
launcher = "fake"

[agents.steward]
role = "steward"
executor = "claude-code"
model = "claude-sonnet-5-5"

[agents.sde]
role = "sde"
executor = "claude-code"
review = false
interactive = false
description = "develops tasks"

[projects.x]
repo = "acme/x"
checkout = "{repo}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = ["proj:x", "proj:x:.+"]
default_agent = "steward"

[[projects.x.inbox_bindings]]
pattern = "proj:x"
default_agent = "steward"
agents = ["steward"]

[[projects.x.inbox_bindings]]
pattern = "proj:x:sde(:.*)?"
default_agent = "sde"
agents = ["sde"]
"""


class DaemonTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        origin = make_base_repo(self.root)
        self.repo = self.root / "checkout"
        git(self.root, "clone", "-q", str(origin), str(self.repo))
        git(self.repo, "config", "user.email", "t@example.invalid")
        git(self.repo, "config", "user.name", "t")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.channel = RecordingChannel()

    async def asyncTearDown(self) -> None:
        await self.orca.close_all()

    def raw(self) -> dict[str, Any]:
        return tomllib.loads(CONFIG.format(root=self.root, repo=self.repo, command=fake_command(self.root)))

    async def run_daemon(self, *events: tuple[str, str | None, dict[str, Any]]) -> Any:
        daemon = build_daemon_from_config(parse_config(self.raw()), channel=self.channel)
        self.addCleanup(daemon.db.close)
        stop = asyncio.Event()
        daemon.pool.bind(asyncio.get_running_loop())
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        for index, (inbox_id, key, payload) in enumerate(events):
            daemon.handler.ingest(inbox_id=inbox_id, source="switchboard:feishu", external_id=f"e{index}",
                                  payload=payload, correlation_key=key)
        await daemon.pool.wait_until_idle(60)
        stop.set()
        await pool
        return daemon

    def launches(self) -> list[dict[str, Any]]:
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [c for c in calls if "answers" not in c and c["role"] != "probe"]

    def set_plan(self, steward: Sequence[dict[str, Any]], sde: Sequence[dict[str, Any]] = ()) -> None:
        self.plan.write_text(json.dumps({"steward": list(steward), "sde": list(sde)}))

    async def test_split_work_comes_back_and_the_steward_answers(self) -> None:
        self.set_plan(
            [
                {"calls": [{"tool": "forward_to_inbox", "arguments": {
                    "target_inbox": SDE, "text": "divide must reject zero", "title": "Zero divisor", "return_to": MAIN,
                }}]},
                {"output": {"action": "reply", "message": "fixed and checked"}},
            ],
            [{"write": {"calc.py": "def divide(a, b):\n    return a / b\n"}}],
        )
        daemon = await self.run_daemon((MAIN, "feishu:1", {
            "text": "divide is broken", "reply_channel": "feishu", "reply_target": "oc_1"}))
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde", "steward"])
        steward_call, sde_call, back_call = self.launches()
        self.assertEqual(os.path.realpath(steward_call["cwd"]), str(self.repo))
        self.assertNotEqual(os.path.realpath(sde_call["cwd"]), str(self.repo))
        system = arg(steward_call, "--append-system-prompt")
        self.assertIn("proj:x:sde(:.*)?", system)
        self.assertIn("develops tasks", system)
        self.assertIn("--resume", back_call["argv"])
        self.assertIn("implemented", back_call["prompt"])
        messages = [m for m in daemon.outbox.fetch_pending(limit=50) if m.metadata["kind"] == "emit"]
        self.assertEqual([(m.inbox_id, m.content) for m in messages], [(MAIN, "fixed and checked")])
        self.assertEqual(messages[0].metadata["reply_target"], "oc_1")
        # Only the child owns a worktree; the Steward's main checkout is untouched.
        rows = daemon.db.fetchall("SELECT inbox_id FROM workspaces")
        self.assertEqual([r["inbox_id"] for r in rows], [SDE])
        self.assertEqual(git_state(self.repo)[0], "")
        sources = [e.source for e in daemon.inbox.list_events()]
        self.assertEqual(sorted(sources), sorted(["switchboard:feishu", FORWARD_SOURCE, RETURN_SOURCE]))

    async def test_a_run_retried_after_orca_was_unavailable_stays_in_its_trace(self) -> None:
        self.set_plan([{"calls": [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "do it"}}]}],
                      [{"write": {"calc.py": "x = 1\n"}}])

        async def broken(*args: str, cwd: Path) -> dict[str, Any]:
            raise RuntimeError("Orca is not running")

        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=broken):
            daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "trace_id": "trace-1"}))
        [card] = daemon.outbox.fetch_pending(limit=50)
        self.assertIn("Orca is not running", card.content)
        self.assertFalse(self.log.exists())
        daemon.handler.ingest(
            inbox_id=MAIN, source="switchboard:feishu", external_id="decision",
            payload={"event_type": "approval.decided", "approved": True,
                     "approval_request_id": card.metadata["approval_request_id"]},
        )
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        await daemon.pool.wait_until_idle(60)
        stop.set()
        await pool
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde"])
        events = daemon.inbox.list_events()
        [asked] = [e for e in events if e.external_id == "e0"]
        [forwarded] = [e for e in events if e.source == FORWARD_SOURCE]
        self.assertEqual((forwarded.payload["trace_id"], forwarded.payload["parent_event_id"]),
                         ("trace-1", asked.event_id))

    async def test_a_deferred_return_waits_for_its_own_approval(self) -> None:
        self.set_plan(
            [
                {"calls": [{"tool": "forward_to_inbox", "arguments": {
                    "target_inbox": SDE, "text": "do it", "return_to": MAIN}}]},
                {"output": {"action": "reply", "message": "all done"}},
            ],
            [{"write": {"calc.py": "x = 1\n"}}],
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
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde", "steward"])
        answers = [(m.inbox_id, m.content) for m in daemon.outbox.fetch_pending(limit=50) if m.metadata["kind"] == "emit"]
        self.assertEqual(answers, [(MAIN, "all done")])

    async def test_a_withheld_result_ends_the_stewards_wait(self) -> None:
        self.set_plan(
            [{"calls": [{"tool": "forward_to_inbox", "arguments": {
                "target_inbox": SDE, "text": "do it", "return_to": MAIN}}]}],
            [{"write": {"calc.py": "x = 1\n"}}],
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
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde"])
        sent = {m.metadata["kind"]: m for m in daemon.outbox.fetch_pending(limit=50)}
        self.assertEqual(sorted(sent), ["approval_request", "return_withheld"])
        notice = sent["return_withheld"]
        self.assertEqual((notice.inbox_id, notice.metadata["reply_channel"], notice.metadata["correlation_key"]),
                         (MAIN, "feishu", "feishu:1"))
        self.assertNotIn("implemented", notice.content)
        [steward] = daemon.sessions.list_sessions(inbox_id=MAIN)
        self.assertEqual(steward.forward.pending_returns, ())

    async def test_direct_forward_answers_from_the_child(self) -> None:
        self.set_plan(
            [{"calls": [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "do it"}}]}],
            [{"write": {"calc.py": "x = 1\n"}}],
        )
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "reply_channel": "feishu"}))
        messages = [m for m in daemon.outbox.fetch_pending(limit=50) if m.metadata["kind"] == "emit"]
        self.assertEqual([(m.inbox_id, m.metadata["reply_channel"], m.metadata["correlation_key"]) for m in messages],
                         [(SDE, "feishu", "feishu:1")])

    async def test_targets_the_config_does_not_cover_are_refused(self) -> None:
        targets = ["proj:x:docs", "proj:y", "proj:x:sde:a/b", "proj:x:sde:.."]
        self.set_plan([{"calls": [
            {"tool": "forward_to_inbox", "arguments": {"target_inbox": t, "text": "x"}} for t in targets
        ] + [{"tool": "forward_to_inbox", "arguments": {"target_inbox": SDE, "text": "x", "target_agent": "steward"}}]}])
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x"}))
        answers = [a for c in map(json.loads, self.log.read_text().splitlines()) for a in c.get("answers", [])]
        self.assertEqual([failed for failed, _ in answers], [True] * 5)
        self.assertEqual(len(daemon.inbox.list_events()), 1)

    async def test_agent_outside_the_whitelist_is_refused_end_to_end(self) -> None:
        daemon = await self.run_daemon((MAIN, "feishu:1", {"text": "x", "agent_id": "sde"}))
        self.assertEqual([e.status for e in daemon.inbox.list_events()], ["failed"])
        self.assertFalse(self.log.exists())
        self.assertEqual([m.metadata["kind"] for m in daemon.outbox.fetch_pending(limit=5)], ["agent_refused"])


class RelayedWorkTests(StewardCase):
    # Agents that share an inbox hand a task on in one worktree, whatever correlation key the relay runs under.
    def setUp(self) -> None:
        super().setUp()
        self.plan.write_text(json.dumps({"sde": [{"write": {"calc.py": "x = 1\n"}}]}))
        self.worktrees = OrcaWorktreeManager(
            workspace_store=SQLiteWorkspaceStore(self.db), session_store=self.sessions, backend="git",
        )
        script = self.root / "where"
        script.write_text("#!/bin/sh\npwd\n")
        script.chmod(0o700)
        self.cli = LocalCLIExecutor(
            worktree_manager=self.worktrees, inbox_checkout_map={MAIN: str(self.repo)}, default_executor="pwd",
            templates={"pwd": CLITemplate(name="pwd", command=str(script))},
        )

    def relay(self, source: str = FORWARD_SOURCE) -> Event:
        return Event(inbox_id=MAIN, source=source, external_id="r", correlation_key="forward:sde:feishu:1",
                     payload={"text": "go on", "base_ref": "HEAD", "work_key": "feishu:1", "forwarded_from": MAIN})

    def work_keys(self) -> list[str]:
        return [r["work_key"] for r in self.db.fetchall("SELECT work_key FROM workspaces")]

    async def test_sde_continues_in_the_worktree_of_the_relayed_work(self) -> None:
        sde = SDEExecutor(
            profile=ClaudeCodeProfile(
                name="sde", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root))),
            options=SDEOptions(review=False), reviewer=None, worktree_manager=self.worktrees,
            inbox_checkout_map={MAIN: str(self.repo)}, tasks=SQLiteTaskMetadataStore(self.db),
        )
        await sde.execute([self.relay()], None, self.caps)  # type: ignore[arg-type]
        self.assertEqual(self.work_keys(), ["feishu:1"])

    async def test_cli_agent_continues_in_the_worktree_of_the_relayed_work(self) -> None:
        await self.cli.execute([self.relay()], None, self.caps)  # type: ignore[arg-type]
        self.assertEqual(self.work_keys(), ["feishu:1"])

    async def test_only_a_relay_names_the_work_it_continues(self) -> None:
        await self.cli.execute([self.relay("webhook")], None, self.caps)  # type: ignore[arg-type]
        self.assertEqual(self.work_keys(), ["forward:sde:feishu:1"])

    async def test_what_follows_a_relay_stays_in_its_worktree(self) -> None:
        # Through the worker: the relay, two plain messages to the same session, then an approval's answer.
        outbox = InMemoryOutbox()
        worker = AgentWorker(
            worker_id="w", inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=self.caps.memory,
            permissions=self.caps.permissions, outbox=outbox, lock=SQLiteLeaseLockAdapter(self.db),
            sessions=self.sessions, executor=self.cli,
        )
        self.sessions.save(SessionRef(session_id="dev", inbox_id=MAIN, correlation_key="feishu:1", work_key="feishu:1"))
        key = "forward:sde:feishu:1"
        self.inbox.append(Event(inbox_id=MAIN, source=FORWARD_SOURCE, external_id="r", correlation_key=key, payload={
            "text": "go on", "work_key": "feishu:1", "review_of": "feishu:1", "hop_count": 1, "forwarded_from": MAIN,
            "parent_session_id": "dev", "origin_key": "feishu:1"}))
        await worker.process_inbox(MAIN)
        [session] = [s for s in self.sessions.list_sessions(inbox_id=MAIN) if s.session_id != "dev"]
        for n in (1, 2):
            self.inbox.append(Event(inbox_id=MAIN, source="switchboard:feishu", external_id=f"f{n}", correlation_key=key,
                                    payload={"text": "and this"}))
            await worker.process_inbox(MAIN)
        self.inbox.append(Event(inbox_id=MAIN, source="timer:follow_up", external_id="t", payload={"text": "tick"},
                                session_id=session.session_id))
        await worker.process_inbox(MAIN)
        cwds = [m.content.strip() for m in outbox.fetch_pending(limit=100)]
        self.assertEqual(len(cwds), 4)
        self.assertEqual(len(set(cwds)), 1, cwds)
        self.assertEqual(self.work_keys(), ["feishu:1"])

    async def test_sde_and_reviewer_follow_ups_stay_in_the_relayed_worktree(self) -> None:
        spec = carried_spec(self.relay())
        session = SessionRef(session_id="s2", inbox_id=MAIN, correlation_key="forward:sde:feishu:1", executor_id="sde",
                             work_key="feishu:1", forward=spec)
        follow_up = Event(inbox_id=MAIN, source="switchboard:feishu", external_id="f", payload={"text": "and this",
                          "base_ref": "HEAD"}, correlation_key="forward:sde:feishu:1")
        sde = SDEExecutor(
            profile=ClaudeCodeProfile(
                name="sde", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root))),
            options=SDEOptions(review=False), reviewer=None, worktree_manager=self.worktrees,
            inbox_checkout_map={MAIN: str(self.repo)}, tasks=SQLiteTaskMetadataStore(self.db),
        )
        await sde.execute([follow_up], session, self.caps)  # type: ignore[arg-type]
        self.assertEqual(self.work_keys(), ["feishu:1"])
        reviewer = CoderReviewerExecutor(
            profile=ClaudeCodeProfile(
                name="code-reviewer", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root))),
            workspaces=SQLiteWorkspaceStore(self.db), tasks=SQLiteTaskMetadataStore(self.db),
        )
        request = reviewer._review_input([replace(follow_up, payload={"text": "look again"})], session)  # pyright: ignore
        [row] = self.db.fetchall("SELECT workdir FROM workspaces")
        self.assertEqual(str(request.workdir), row["workdir"])


if __name__ == "__main__":
    unittest.main()
