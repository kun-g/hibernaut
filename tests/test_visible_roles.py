import asyncio
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import PROXY_PLACEHOLDER_KEY, ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import HANDOFF_SCRIPT, HANDOFF_TOOLS, OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.reviewer import REVIEW_SCHEMA, CoderReviewerExecutor, worktree_fingerprint
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter, SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, SessionRef, Suspend
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import CHANGES_REQUESTED, _Capture, fake_command, git
from tests.test_sde_executor import BUGGY, EVIDENCE, FIXED, PASS, PROXY, dev, event, make_base_repo


class FakeOrcaTerminals:
    """Orca CLI double that runs each terminal's --command in a real shell, the way Orca types it into a login
    shell, so the gate, the billing probe and the interactive agent all really run. `terminal send` types a
    line into the running process; `terminal list` holds only terminals whose process is still running."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.terminals: dict[str, dict] = {}
        self.calls: list[tuple[str, ...]] = []

    def _info(self, handle: str) -> dict:
        term = self.terminals[handle]
        return {"handle": handle, "incarnationId": term["incarnation"], "title": term["title"],
                "connected": True, "writable": True, "agentIdentity": "claude",
                "worktreePath": term["worktree"]}

    async def __call__(self, *args: str, cwd: Path) -> dict:
        self.calls.append(args)
        verb = " ".join(args[:2])
        flags = dict(zip(args[2::2], args[3::2]))
        if verb == "terminal create":
            worktree = flags["--worktree"].removeprefix("path:")
            handle = f"term_{uuid.uuid4().hex[:8]}"
            screen = (self.root / f"{handle}.screen").open("wb")
            proc = await asyncio.create_subprocess_exec(
                "/bin/sh", "-c", flags["--command"], cwd=worktree,
                stdin=asyncio.subprocess.PIPE, stdout=screen, stderr=asyncio.subprocess.STDOUT,
            )
            screen.close()
            self.terminals[handle] = {"proc": proc, "title": flags["--title"], "worktree": worktree,
                                      "incarnation": uuid.uuid4().hex, "command": flags["--command"]}
            return {"terminal": {"handle": handle, "incarnationId": self.terminals[handle]["incarnation"],
                                 "title": flags["--title"]}}
        if verb == "terminal list":
            return {"terminals": [self._info(h) for h, t in self.terminals.items() if t["proc"].returncode is None]}
        handle = flags.get("--terminal", "")
        term = self.terminals.get(handle)
        if verb == "terminal show":
            if term is None:
                raise RuntimeError("terminal not found")
            return {"terminal": self._info(handle)}
        if verb == "terminal send":
            assert term is not None and term["proc"].returncode is None
            term["proc"].stdin.write((flags["--text"] + "\n").encode())
            await term["proc"].stdin.drain()
            return {"send": {"handle": handle, "accepted": True, "prompt": {"processIncarnation": term["incarnation"]}}}
        if verb == "terminal close":
            await self.kill(handle)
            return {"close": {"handle": handle}}
        raise AssertionError(f"unexpected orca call {args}")

    async def kill(self, handle: str) -> None:
        proc = self.terminals[handle]["proc"]
        if proc.returncode is None:
            proc.kill()
            await proc.wait()

    async def close_all(self) -> None:
        for handle in self.terminals:
            await self.kill(handle)

    def created(self) -> list[dict]:
        return [t for t in self.terminals.values()]

    def verbs(self) -> list[str]:
        return [" ".join(c[:2]) for c in self.calls]

    def screen(self, handle: str) -> str:
        return (self.root / f"{handle}.screen").read_text(errors="replace")


# Default for the case helpers: the case's own Orca runner.
_RUNNER = object()


def arg(call: dict, flag: str) -> str:
    return call["argv"][call["argv"].index(flag) + 1]


class VisibleCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_base_repo(self.root)
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {
            "FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan), "FAKE_SESSIONS": str(self.root / "sessions"),
        }))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.tasks = SQLiteTaskMetadataStore(self.db)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.runner = OrcaVisibleRunner(bindings=self.bindings, poll_interval=0.02, liveness_interval=0.2,
                                        handoff_root=self.root / "handoff")
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    async def asyncTearDown(self):
        await self.orca.close_all()

    def set_plan(self, sde=(), reviewer=()):
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def launcher(self):
        return ClaudeCodeLauncher(name="fake", command=fake_command(self.root))

    def reviewer(self, terminal=_RUNNER):
        return CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=self.launcher(), model="claude-sonnet-5-5",
                                      effort="medium"),
            workspaces=self.workspaces, tasks=self.tasks, terminal=self.runner if terminal is _RUNNER else terminal,
        )

    def sde(self, review=True, timeout=30.0, terminal=_RUNNER, reviewer_terminal=_RUNNER, **overrides):
        overrides.setdefault("launcher", self.launcher())
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", timeout_seconds=timeout, **overrides),
            options=SDEOptions(review=review, reviewer="coder-reviewer"),
            reviewer=self.reviewer(reviewer_terminal) if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={}, tasks=self.tasks,
            terminal=self.runner if terminal is _RUNNER else terminal,
        )

    def calls(self, role=None, mode=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if (role is None or r["role"] == role) and (mode is None or r["mode"] == mode)]

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor",
                "explicit_workdir": str(self.repo), "base_ref": "main", **extra}

    @staticmethod
    def session_after(outcome, **extra):
        return SessionRef(session_id=outcome.session_id, inbox_id="proj:x", correlation_key="task:1",
                          executor_id="sde", executor_session_id=outcome.executor_session_id, **extra)


class TestVisibleSDE(VisibleCase):
    async def test_development_and_builtin_review_run_in_role_terminals_with_role_settings(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[PASS])
        outcome = await self.sde().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        self.assertIn("[sde] delivered (review=pass after 1 round", text)
        self.assertIn(EVIDENCE, text)
        dev_term, review_term = self.orca.created()
        # Each role has its own terminal in the target worktree, labelled with the role and the task.
        self.assertEqual(dev_term["title"], "sde · task:1")
        self.assertEqual(review_term["title"], "coder-reviewer (code-reviewer) · review of task:1 round 1")
        self.assertEqual({dev_term["worktree"], review_term["worktree"]}, {str(self.repo)})
        dev_handle = next(h for h, t in self.orca.terminals.items() if t is dev_term)
        review_handle = next(h for h, t in self.orca.terminals.items() if t is review_term)
        # Interactive sessions (no -p), with the role's tools, model, effort and an explicit permission mode.
        (dev_call,) = self.calls("sde", "interactive")
        (review_call,) = self.calls("reviewer", "interactive")
        self.assertEqual(self.calls(mode="print", role="sde") + self.calls(mode="print", role="reviewer"), [])
        self.assertEqual((arg(dev_call, "--model"), arg(dev_call, "--effort")), ("claude-opus-5-5", "high"))
        self.assertEqual(arg(dev_call, "--permission-mode"), "dontAsk")
        self.assertIn("Edit", arg(dev_call, "--allowedTools").split(","))
        dev_disallowed = arg(dev_call, "--disallowedTools").split(",")
        self.assertIn("Bash(git push --force*)", dev_disallowed)
        self.assertIn("Bash(git push * -f)", dev_disallowed)
        for allowed_cmd in ("Bash(git push:*)", "Bash(gh pr create:*)", "Bash(gh pr merge:*)"):
            self.assertNotIn(allowed_cmd, dev_disallowed)
        self.assertIn("failing test", arg(dev_call, "--append-system-prompt"))
        self.assertIn("Review only", arg(review_call, "--append-system-prompt"))
        self.assertIn("divide must reject a zero divisor", dev_call["prompt"])
        self.assertEqual(arg(dev_call, "--session-id"), outcome.executor_session_id)
        self.assertIn(f"Orca terminal: {dev_handle} (Claude Code session {outcome.executor_session_id})", text)
        # No extra directory is added (a new one opens a blocking trust dialog); both roles take the task from and
        # return the result to the launcher's handoff tools.
        for call in (dev_call, review_call):
            self.assertNotIn("--add-dir", call["argv"])
            self.assertEqual(call["handoff_tools"], ["get_task", "submit_result"])
            self.assertTrue(set(HANDOFF_TOOLS) <= set(arg(call, "--allowedTools").split(",")))
        # The reviewer is read-only: no file-writing tool at all, and every one of them denied.
        self.assertEqual((arg(review_call, "--model"), arg(review_call, "--effort")), ("claude-sonnet-5-5", "medium"))
        allowed = arg(review_call, "--allowedTools").split(",")
        self.assertFalse({"Bash"} & set(allowed))
        self.assertEqual([t for t in allowed if t.startswith(("Edit", "Write", "MultiEdit", "NotebookEdit"))], [])
        self.assertTrue({"Edit", "Write", "MultiEdit", "NotebookEdit"} <= set(arg(review_call, "--disallowedTools").split(",")))
        self.assertNotEqual(arg(review_call, "--session-id"), outcome.executor_session_id)
        for part in ("divide must reject a zero divisor", EVIDENCE, "+    if b == 0:"):
            self.assertIn(part, review_call["prompt"])
        self.assertIn(f"Orca terminal: {review_handle}", text)
        # Both launches passed the same session check as a headless run: a probe stopped at init.
        probes = self.calls("probe")
        self.assertEqual(len(probes), 2)
        self.assertTrue(all("-p" in p["argv"] and arg(p, "--permission-mode") == "dontAsk" for p in probes))
        self.assertEqual([arg(p, "--model") for p in probes], ["claude-opus-5-5", "claude-sonnet-5-5"])
        binding = self.bindings.get(outcome.session_id)
        self.assertEqual((binding.handle, binding.native_session_id, binding.executor_id),
                         (dev_handle, outcome.executor_session_id, "sde"))

    async def test_fix_round_continues_the_live_conversation_and_rereview_is_a_fresh_reviewer(self):
        self.set_plan(sde=[dev(BUGGY), dev(FIXED, summary="zero divisor rejected")], reviewer=[CHANGES_REQUESTED, PASS])
        outcome = await self.sde().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 2 round", outcome.messages[0])
        first, fix = self.calls("sde", "interactive")
        self.assertEqual((first["session_id"], fix["session_id"]), (outcome.executor_session_id,) * 2)
        self.assertEqual((first["turn"], fix["turn"]), (1, 2))
        # The fix went to the same terminal; nothing new was started for the developer.
        self.assertEqual([t["title"] for t in self.orca.created()],
                         ["sde · task:1", "coder-reviewer (code-reviewer) · review of task:1 round 1",
                          "coder-reviewer (code-reviewer) · review of task:1 round 2"])
        self.assertIn("terminal send", self.orca.verbs())
        for part in ("calc.py:2", "divide(1, 0)", "reject zero divisors"):
            self.assertIn(part, fix["prompt"])
        review1, review2 = self.calls("reviewer", "interactive")
        self.assertNotEqual(review1["session_id"], review2["session_id"])
        self.assertEqual(review2["turn"], 1)
        self.assertIn("Previous review round", review2["prompt"])
        self.assertIn("+    if b == 0:", review2["prompt"])

    async def test_answer_after_suspend_resumes_the_closed_conversation(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?"), dev(FIXED)], reviewer=[PASS])
        executor = self.sde()
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        native = outcome.executor_session_id
        # The operator closed the tab (or the daemon restarted) before answering.
        await self.orca.close_all()
        session = self.session_after(outcome, status="suspended", checkpoint=outcome.session_checkpoint,
                                     pending_request_id=outcome.request_id)
        answer = event({"approval_request_id": outcome.request_id, "approved": True, "comment": "Raise ValueError"})
        resumed = await executor.execute([answer], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        _, again = self.calls("sde", "interactive")
        self.assertEqual(arg(again, "--resume"), native)
        self.assertNotIn("--session-id", again["argv"])
        self.assertEqual((again["session_id"], again["turn"]), (native, 2))
        self.assertIn("Raise ValueError", again["prompt"])
        self.assertEqual(resumed.executor_session_id, native)

    async def test_follow_up_event_reuses_the_live_terminal(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?"), dev(FIXED)])
        executor = self.sde(review=False)
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        session = self.session_after(outcome, status="suspended", checkpoint=outcome.session_checkpoint,
                                     pending_request_id=outcome.request_id)
        answer = event({"approval_request_id": outcome.request_id, "approved": True, "comment": "Raise"})
        resumed = await executor.execute([answer], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertEqual(len(self.orca.created()), 1)
        self.assertEqual([c["turn"] for c in self.calls("sde", "interactive")], [1, 2])

    async def test_paid_credentials_stop_the_launch_before_the_interactive_session(self):
        self.set_plan(sde=[dev(FIXED)])
        with patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": "apiKeyHelper"}):
            outcome = await self.sde().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("development failed: billing is 'subscription'", outcome.messages[0])
        self.assertEqual(self.calls(mode="interactive"), [])
        (handle,) = self.orca.terminals
        self.assertIn("terminal close", self.orca.verbs())
        self.assertIsNotNone(self.orca.terminals[handle]["proc"].returncode)

    async def test_api_billing_reaches_the_interactive_session_only_through_the_proxy(self):
        self.set_plan(sde=[dev(FIXED)])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-real-secret", "ANTHROPIC_AUTH_TOKEN": "tok"}):
            outcome = await self.sde(review=False, billing="api", credential_proxy_url=PROXY).execute(
                [event(self.payload())], None, self.caps
            )
        assert isinstance(outcome, Emit), outcome
        (call,) = self.calls("sde", "interactive")
        self.assertEqual((call["base_url"], call["api_key"]), (PROXY, PROXY_PLACEHOLDER_KEY))
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", call["paid_env"])
        # The placeholder key is set by the gate, never typed into the terminal.
        self.assertNotIn(PROXY_PLACEHOLDER_KEY, self.orca.created()[0]["command"])

    async def test_timeout_closes_the_terminal_and_keeps_the_conversation_resumable(self):
        self.set_plan(sde=[{"hang": True}])
        outcome = await self.sde(review=False, timeout=3).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("development failed: timed out after 3", outcome.messages[0])
        (handle,) = self.orca.terminals
        self.assertIsNotNone(self.orca.terminals[handle]["proc"].returncode)
        binding = self.bindings.get(outcome.session_id)
        self.assertIsNone(binding.handle)
        self.assertEqual(binding.native_session_id, arg(self.calls("sde", "interactive")[0], "--session-id"))

    async def test_interrupt_cancels_without_output_and_closes_the_terminal(self):
        self.set_plan(sde=[{"hang": True}])
        seen = []

        def interrupted():
            seen.append(1)
            return bool(self.calls("sde", "interactive"))

        self.caps.is_interrupted.side_effect = interrupted
        with self.assertRaises(asyncio.CancelledError):
            await self.sde(review=False).execute([event(self.payload())], None, self.caps)
        (handle,) = self.orca.terminals
        self.assertIsNotNone(self.orca.terminals[handle]["proc"].returncode)

    async def test_agent_that_exits_without_a_result_is_reported(self):
        self.set_plan(sde=[{"exit": True}])
        outcome = await self.sde(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("exited before it returned its result", outcome.messages[0])

    async def test_handoff_directories_are_removed_after_each_run(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[PASS])
        outcome = await self.sde().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        # The task conversation keeps its directory (and schema) for later turns; turn directories and the
        # one-shot reviewer's directory are gone.
        left = sorted(str(p.relative_to(self.root / "handoff")) for p in (self.root / "handoff").rglob("*"))
        self.assertEqual(left, [outcome.session_id, f"{outcome.session_id}/schema.json"])


class TestVisibleReviewer(VisibleCase):
    def record_task_workspace(self):
        self.workspaces.reserve_creating(work_key="task:1", repo_checkout=str(self.repo), workdir=str(self.repo),
                                         branch="hb/task-1")
        self.workspaces.mark_ready("task:1", workdir=str(self.repo), branch="hb/task-1")
        (self.repo / "calc.py").write_text(FIXED["calc.py"])

    async def test_review_of_runs_in_its_own_terminal_in_the_developer_worktree(self):
        self.record_task_workspace()
        self.set_plan(reviewer=[CHANGES_REQUESTED])
        before = worktree_fingerprint(self.repo)
        review = event({"executor_id": "coder-reviewer", "text": "divide must reject zero", "review_of": "task:1",
                        "test_evidence": EVIDENCE, "base_ref": "main"}, correlation_key="review:task:1")
        outcome = await self.reviewer().execute([review], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (term,) = self.orca.created()
        self.assertEqual((term["title"], term["worktree"]), ("coder-reviewer (code-reviewer) · review of task:1", str(self.repo)))
        (call,) = self.calls("reviewer", "interactive")
        self.assertEqual(call["cwd"], str(self.repo))
        self.assertEqual(outcome.executor_session_id, arg(call, "--session-id"))
        self.assertIn("verdict=changes_requested", outcome.messages[0])
        self.assertIn("calc.py:2", outcome.messages[0])
        self.assertIn("Orca terminal: term_", outcome.messages[0])
        self.assertEqual(worktree_fingerprint(self.repo), before)

    async def test_a_reviewer_write_into_the_worktree_voids_the_review(self):
        self.record_task_workspace()
        self.set_plan(reviewer=[PASS])
        review = event({"executor_id": "coder-reviewer", "text": "divide must reject zero", "review_of": "task:1",
                        "base_ref": "main"})
        with patch.dict(os.environ, {"FAKE_TOUCH": str(self.repo / "sneaky.txt")}):
            outcome = await self.reviewer().execute([review], None, self.caps)
        self.assertIn("review failed: the working tree", outcome.messages[0])
        self.assertNotIn("verdict=pass", outcome.messages[0])

    async def test_headless_reviewer_is_unchanged(self):
        self.record_task_workspace()
        self.set_plan(reviewer=[PASS])
        review = event({"executor_id": "coder-reviewer", "text": "divide must reject zero", "review_of": "task:1",
                        "base_ref": "main"})
        outcome = await self.reviewer(terminal=None).execute([review], None, self.caps)
        self.assertIn("verdict=pass", outcome.messages[0])
        self.assertEqual(self.orca.calls, [])
        self.assertEqual([c["mode"] for c in self.calls("reviewer")], ["print"])


class TestHandoffServer(unittest.TestCase):
    """The launcher-side MCP server a visible agent uses to fetch its task and return its result."""

    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        (self.root / "schema.json").write_text(json.dumps(REVIEW_SCHEMA))
        self.turn = self.root / "turn-abc"
        self.turn.mkdir()
        (self.turn / "prompt.md").write_text("review the divide change")
        self.proc = subprocess.Popen([sys.executable, "-I", str(HANDOFF_SCRIPT), str(self.root)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.addCleanup(self.proc.wait)
        self.addCleanup(self.proc.stdin.close)
        self.addCleanup(self.proc.stdout.close)
        self.ids = 0

    def rpc(self, method, params=None):
        self.ids += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.ids, "method": method,
                                          "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        return json.loads(self.proc.stdout.readline())

    def call(self, name, **arguments):
        result = self.rpc("tools/call", {"name": name, "arguments": arguments})["result"]
        return result.get("isError", False), result["content"][0]["text"]

    def test_lists_task_and_result_tools_with_the_role_schema(self):
        init = self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}})["result"]
        self.assertEqual(init["protocolVersion"], "2025-06-18")
        self.assertIn("tools", init["capabilities"])
        tools = {t["name"]: t for t in self.rpc("tools/list")["result"]["tools"]}
        self.assertEqual(sorted(tools), ["get_task", "submit_result"])
        submit = tools["submit_result"]["inputSchema"]
        self.assertEqual(submit["properties"]["verdict"], REVIEW_SCHEMA["properties"]["verdict"])
        self.assertEqual(sorted(submit["required"]), sorted([*REVIEW_SCHEMA["required"], "turn"]))

    def test_returns_the_task_and_stores_a_valid_result_without_the_turn(self):
        self.assertEqual(self.call("get_task", turn="turn-abc"), (False, "review the divide change"))
        review = {"verdict": "pass", "summary": "ok", "blocking": []}
        failed, _ = self.call("submit_result", turn="turn-abc", **review)
        self.assertFalse(failed)
        self.assertEqual(json.loads((self.turn / "result.json").read_text()), review)

    def test_rejects_a_result_that_does_not_match_the_schema(self):
        failed, text = self.call("submit_result", turn="turn-abc", verdict="maybe", summary="ok", blocking=[])
        self.assertTrue(failed)
        self.assertIn("verdict", text)
        failed, text = self.call("submit_result", turn="turn-abc", verdict="pass", blocking=[{"location": "a.py"}])
        self.assertTrue(failed)
        self.assertIn("summary", text)
        self.assertFalse((self.turn / "result.json").exists())

    def test_turns_outside_the_handoff_root_are_refused(self):
        outside = self.root.parent / "elsewhere"
        for turn in ("../elsewhere", str(outside), "turn-missing", "schema.json"):
            failed, _ = self.call("get_task", turn=turn)
            self.assertTrue(failed, turn)
            failed, _ = self.call("submit_result", turn=turn, verdict="pass", summary="ok", blocking=[])
            self.assertTrue(failed, turn)
        self.assertFalse(outside.exists())

    def test_unknown_methods_get_an_error_and_notifications_no_reply(self):
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        reply = self.rpc("resources/list")
        self.assertEqual((reply["id"], reply["error"]["code"]), (self.ids, -32601))


class TestVisibleRolesThroughEvents(unittest.TestCase):
    def test_sde_and_independent_review_events_get_separate_visible_conversations(self):
        root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        repo = make_base_repo(root)
        workdir = root / "hb-task-42"
        git(repo, "worktree", "add", "-q", "-b", "hb/task-42", str(workdir))
        log, plan = root / "calls.jsonl", root / "plan.json"
        plan.write_text(json.dumps({"sde": [dev(FIXED)], "reviewer": [PASS, CHANGES_REQUESTED]}))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"
        launcher = fake_command(root)
        path.write_text(
            f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "sde"\n'
            f"[channels]\nport = {port}\n"
            f'[launchers.fake]\ncommand = "{launcher}"\n'
            '[executors.coder-reviewer]\nrole = "coder-reviewer"\nlauncher = "fake"\nmodel = "claude-sonnet-5-5"\n'
            '[executors.sde]\nrole = "sde"\nlauncher = "fake"\n'
            f'[projects.demo]\ninbox_id = "proj:demo"\nrepo = "o/demo"\ncheckout = "{repo}"\nworktree_backend = "orca"\n'
        )
        orca = FakeOrcaTerminals(root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=orca))
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(log), "FAKE_PLAN": str(plan),
                                                  "FAKE_SESSIONS": str(root / "sessions")}))
        stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))
        self.enterContext(patch("agent_runtime.adapters.channels.github_poller.GitHubIssuePoller.poll_once", return_value=0))

        def post(external_id, correlation_key, payload):
            body = {"inbox_id": "proj:demo", "external_id": external_id, "correlation_key": correlation_key,
                    "payload": payload}
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
                return conn.getresponse().status
            finally:
                conn.close()

        def outbox():
            with sqlite3.connect(db_path) as conn:
                return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

        def sessions():
            with sqlite3.connect(db_path) as conn:
                return {row[0]: row[1:] for row in conn.execute(
                    "SELECT correlation_key, executor_id, executor_session_id, session_id FROM sessions")}

        def calls(role):
            rows = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return [r for r in rows if r["role"] == role and r["mode"] == "interactive"]

        async def scenario():
            config = load_config(path)
            daemon = build_daemon_from_config(config)
            daemon.workspaces.reserve_creating(work_key="task-42", repo_checkout=str(repo), workdir=str(workdir),
                                               branch="hb/task-42")
            daemon.workspaces.mark_ready("task-42", workdir=str(workdir), branch="hb/task-42")
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(path, config, daemon, stop))

            async def wait_for(check):
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    self.assertFalse(server.done(), stderr.getvalue())
                    try:
                        if check():
                            return
                    except OSError:
                        pass
                    await asyncio.sleep(0.05)
                self.fail(f"outbox={outbox()} calls={orca.verbs()} {stderr.getvalue()}")

            try:
                task = {"executor_id": "sde", "text": "divide must reject a zero divisor", "base_ref": "main"}
                await wait_for(lambda: post("task-42", "task-42", task) == 202)
                await wait_for(lambda: any("[sde] delivered" in m for m in outbox()))
                before = await asyncio.to_thread(worktree_fingerprint, workdir)
                review = {"executor_id": "coder-reviewer", "text": "divide must reject a zero divisor",
                          "review_of": "task-42", "test_evidence": EVIDENCE, "base_ref": "main"}
                await asyncio.to_thread(post, "review-42-r1", "review:task-42", review)
                await wait_for(lambda: any("verdict=changes_requested" in m for m in outbox()))
                delivered = next(m for m in outbox() if "[sde] delivered" in m)
                standalone = next(m for m in outbox() if "verdict=changes_requested" in m)
                self.assertEqual(await asyncio.to_thread(worktree_fingerprint, workdir), before)
                by_key = sessions()
                sde_exec, sde_native, sde_session = by_key["task-42"]
                rev_exec, rev_native, _ = by_key["review:task-42"]
                self.assertEqual((sde_exec, rev_exec), ("sde", "coder-reviewer"))
                (dev_call,) = calls("sde")
                builtin, independent = calls("reviewer")
                self.assertEqual(sde_native, dev_call["session_id"])
                self.assertEqual(rev_native, independent["session_id"])
                self.assertEqual(len({sde_native, builtin["session_id"], rev_native}), 3)
                self.assertTrue(all(c["cwd"] == str(workdir) for c in (dev_call, builtin, independent)))
                titles = [t["title"] for t in orca.created()]
                self.assertEqual(titles, ["sde · task-42", "coder-reviewer (code-reviewer) · review of task-42 round 1",
                                          "coder-reviewer (code-reviewer) · review of task-42"])
                handles = list(orca.terminals)
                self.assertIn(f"Orca terminal: {handles[0]} (Claude Code session {sde_native})", delivered)
                self.assertIn(f"Orca terminal: {handles[1]}", delivered)
                self.assertIn(f"Orca terminal: {handles[2]} (Claude Code session {rev_native})", standalone)
                with sqlite3.connect(db_path) as conn:
                    bound = conn.execute("SELECT handle, native_session_id FROM terminal_bindings WHERE session_id = ?",
                                         (sde_session,)).fetchone()
                self.assertEqual(bound, (handles[0], sde_native))
            finally:
                stop.set()
                await server
                await orca.close_all()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
