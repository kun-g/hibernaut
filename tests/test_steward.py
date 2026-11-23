from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import tomllib
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.executor_cli import (
    CLITemplate,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    SDEExecutor,
    SDEOptions,
    StewardExecutor,
)
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.steward_tools import GitRefused, check_git_args
from agent_runtime.adapters.memory_fs import FileSystemMemoryAdapter
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTaskMetadataStore,
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
    PermissionDecision,
    SessionRef,
    Silent,
    Suspend,
)
from tests.fakes import InMemoryPermission, RecordingChannel
from tests.test_coder_reviewer import fake_command, git, make_repo
from tests.test_sde_executor import make_base_repo

MAIN = "proj:x"
SDE = "proj:x:sde"


class Caps:
    def __init__(self, root: Path) -> None:
        self.memory = FileSystemMemoryAdapter(root / "memory")
        self.permissions: Any = InMemoryPermission()
        self.timers: Any = None
        self.forwards: list[dict[str, Any]] = []
        self.refusal: ForwardRefused | None = None

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return False

    def forward_to_inbox(self, target_inbox: str, text: str, **kwargs: Any) -> Event:
        if self.refusal is not None and not kwargs.get("approved_request_id"):
            raise self.refusal
        self.forwards.append({"target_inbox": target_inbox, "text": text,
                              **{k: v for k, v in kwargs.items() if v}})
        return Event(inbox_id=target_inbox, source=FORWARD_SOURCE, external_id="x", payload={"text": text})


class StewardCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repo = make_repo(self.root)
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

    def executor(self, checkouts: dict[str, str] | None = None) -> StewardExecutor:
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
        )

    def set_plan(self, *steps: dict[str, Any]) -> None:
        self.plan.write_text(json.dumps({"steward": list(steps)}))

    def calls(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def launches(self) -> list[dict[str, Any]]:
        return [c for c in self.calls() if "answers" not in c]

    def answers(self) -> list[list[Any]]:
        return [a for c in self.calls() if "answers" in c for a in c["answers"]]

    def event(self, text: str = "fix the login bug", **payload: Any) -> Event:
        return Event(inbox_id=MAIN, source="switchboard:feishu", external_id=f"e{len(self.calls())}",
                     payload={"text": text, **payload}, correlation_key="feishu:1")

    async def run_steward(self, *events: Event, session: SessionRef | None = None) -> Any:
        return await self.executor().execute(list(events) or [self.event()], session, self.caps)

    async def tool(self, name: str, **arguments: Any) -> tuple[bool, str]:
        self.set_plan({"calls": [{"tool": name, "arguments": arguments}]})
        outcome = await self.run_steward()
        self.assertIsInstance(outcome, Silent)
        failed, text = self.answers()[-1]
        return failed, text


def arg(call: dict[str, Any], flag: str) -> str:
    return call["argv"][call["argv"].index(flag) + 1]


class ReadOnlyBoundaryTests(StewardCase):
    async def test_runs_in_the_main_checkout_with_read_only_tools(self) -> None:
        before = git_state(self.repo)
        self.set_plan({"output": {"action": "reply", "message": "on it"}})
        outcome = await self.run_steward()
        assert isinstance(outcome, Emit)
        self.assertEqual((outcome.messages, outcome.status), (("on it",), "done"))
        [call] = self.launches()
        self.assertEqual(os.path.realpath(call["cwd"]), os.path.realpath(self.repo))
        self.assertEqual(arg(call, "--permission-mode"), "dontAsk")
        self.assertEqual(arg(call, "--tools"), "Read,Grep,Glob")
        self.assertIn("--strict-mcp-config", call["argv"])
        allowed = arg(call, "--allowedTools").split(",")
        self.assertFalse([t for t in allowed if t.startswith(("Bash", "Edit", "Write", "Notebook", "MultiEdit"))])
        self.assertTrue({"Edit", "Write", "MultiEdit", "NotebookEdit", "Bash", "Agent", "Task"}
                        <= set(arg(call, "--disallowedTools").split(",")))
        self.assertEqual((arg(call, "--model"), arg(call, "--effort")), ("claude-sonnet-5-5", "medium"))
        self.assertEqual(git_state(self.repo), before)
        self.assertEqual(self.db.fetchall("SELECT * FROM workspaces"), [])

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


class ToolTests(StewardCase):
    async def test_tools_are_listed(self) -> None:
        await self.run_steward()
        self.assertEqual(self.calls()[-1]["tools"], [
            "forward_to_inbox", "git", "list_events", "list_sessions", "read_memory", "write_memory",
        ])

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
        ):
            failed, text = await self.tool(name, **arguments)
            self.assertTrue(failed, name)
            self.assertIn("no", text)
        self.assertEqual([s for s in seen if s != "steward.launch"],
                         ["read_memory", "write_memory", "list_sessions", "list_events", "git_read"])
        self.assertEqual(self.caps.memory.read_on_demand("index.md"), "")

    async def test_default_policy_allows_the_stewards_tools(self) -> None:
        policy = AutoModePermissionAdapter()
        for name in ("steward.launch", "inbox.forward", "read_memory", "write_memory", "list_sessions", "list_events",
                     "git_read"):
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
                     ["log", "-SOauth", "-5"], ["diff", "-U3", "-w"], ["blame", "-w", "-L1,/S/", "a"],
                     ["blame", "--ignore-rev", "abc", "a"],
                     ["show", "HEAD~1:src/a.py"], ["log", "--format=%h: %s"], ["log", "-L1,2:src/a.py"],
                     ["log", "-Sfoo:/bar"], ["diff", "origin/main...HEAD"], ["show", "HEAD@{0}:a"]):
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
                     ["rev-parse", "HEAD:/etc/passwd"], ["log", "-L1,2:/etc/passwd"]):
            with self.assertRaises(GitRefused, msg=str(args)):
                check_git_args(args)


class SuspendTests(StewardCase):
    def suspended(self, outcome: Suspend, native: str | None = "native-1") -> SessionRef:
        return SessionRef(session_id="s1", inbox_id=MAIN, executor_id="steward", executor_session_id=native,
                          status="suspended", checkpoint=outcome.session_checkpoint,
                          pending_request_id=outcome.request_id)

    def decision(self, outcome: Suspend, approved: bool) -> Event:
        return Event(inbox_id=MAIN, source="switchboard:feishu", external_id="d", payload={
            "event_type": "approval.decided", "approval_request_id": outcome.request_id, "approved": approved,
        })

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
        self.channel = RecordingChannel()

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
        return [c for c in map(json.loads, self.log.read_text().splitlines()) if "answers" not in c]

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
        self.cli = LocalCLIExecutor(
            worktree_manager=self.worktrees, inbox_checkout_map={MAIN: str(self.repo)}, default_executor="pwd",
            templates={"pwd": CLITemplate(name="pwd", command="pwd")},
        )

    def relay(self, source: str = FORWARD_SOURCE) -> Event:
        return Event(inbox_id=MAIN, source=source, external_id="r", correlation_key="forward:sde:feishu:1",
                     payload={"text": "go on", "base_ref": "HEAD", "work_key": "feishu:1"})

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


if __name__ == "__main__":
    unittest.main()
