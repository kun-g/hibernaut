import asyncio
import json
import os
import shutil
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.delivery import MERGE_TOOL
from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate, OrcaWorktreeManager
from agent_runtime.adapters.executor_cli import orca as orca_module
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, TerminalUnavailable, terminal_title
from agent_runtime.adapters.executor_cli.project_guidance import pin_head
from agent_runtime.adapters.executor_cli.executor import LocalCLIExecutor, render_cli_args
from agent_runtime.adapters.executor_cli.reviewer import REVIEW_SCHEMA, ReviewAborted, ReviewRequest
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.core.models import Emit, Event, PermissionDecision
from tests.support.async_deadline import expire_when_ready
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import CHANGES_REQUESTED, fake_command, make_repo
from tests.support.pr_delivery import DeliveryCase
from tests.support.visible_roles import FakeOrcaTerminals
from tests.support.cli_reviewer import CLIPlan, EVIDENCE, ORCA_LOG, PASS, TerminalCase, cli_command


REAL_ORCA = orca_module._orca


class TestCLIReviewer(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repo = make_repo(self.root)
        self.cli = CLIPlan(self.root)
        self.enterContext(patch.dict(os.environ, self.cli.env()))
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    def reviewer(self, **overrides):
        fields = {"name": "grok", "command": cli_command(self.root), "args": ("-p",), **overrides}
        return CLIReviewer(template=CLITemplate(**fields))

    def request(self, **extra):
        return ReviewRequest(workdir=self.repo, requirements="divide must reject a zero divisor",
                             test_evidence=EVIDENCE, base_ref="main", pin=pin_head(self.repo, "test"), **extra)

    async def test_bare_json_verdict_and_the_prompt_carry_the_review_inputs_and_contract(self):
        self.cli.set(json.dumps(PASS))
        template_env = {"FAKE_CLI_MARK": "from-template"}
        review = await self.reviewer(env=template_env).review(self.request(previous_round="## Previous review round\nX"), self.caps)
        self.assertEqual((review["verdict"], review["summary"], review["blocking"]), ("pass", "zero is rejected", []))
        (call,) = self.cli.calls()
        self.assertEqual(call["argv"][0], "-p")
        prompt = call["argv"][1]
        self.assertEqual(Path(call["cwd"]).resolve(), self.repo.resolve())
        self.assertEqual(call["env"], "from-template")
        self.assertFalse(call["stdin_tty"])
        for part in ("## Task requirements\ndivide must reject a zero divisor", f"## Test evidence\n{EVIDENCE}",
                     "+    return a / b", "## Previous review round\nX", "## Review contract",
                     "Do not create, edit, stage, commit", "location (path:line), trigger", "guidance_clause",
                     "Output exactly one JSON object and nothing else", '"required": ["verdict", "summary", "blocking"]'):
            self.assertIn(part, prompt)
        self.assertTrue(prompt.rstrip().endswith("}"), "the output schema closes the prompt")

    async def test_the_prompt_placeholder_follows_the_template(self):
        self.cli.set(json.dumps(PASS))
        reviewer = self.reviewer(args=("--print", "--prompt={prompt}", "--format", "json"),
                                 resume_args=("--resume", "{executor_session_id}"))
        await reviewer.review(self.request(), self.caps)
        argv = self.cli.calls()[0]["argv"]
        self.assertEqual(argv[:1] + argv[2:], ["--print", "--format", "json"])
        self.assertTrue(argv[1].startswith("--prompt=## Task requirements"))

    async def test_the_review_schema_placeholder_is_the_compact_schema(self):
        self.cli.set(json.dumps({"text": json.dumps(PASS), "structuredOutput": PASS}))
        reviewer = self.reviewer(args=("-p", "{prompt}", "--permission-mode", "plan", "--json-schema", "{review_schema}"))
        # A prompt that quotes a placeholder keeps it: substitution never rescans inserted text.
        review = await reviewer.review(self.request(previous_round="quoted {review_schema} and {prompt}"), self.caps)
        self.assertEqual(review["verdict"], "pass")
        argv = self.cli.calls()[0]["argv"]
        self.assertEqual(argv[0], "-p")
        self.assertEqual(argv[2:5], ["--permission-mode", "plan", "--json-schema"])
        self.assertEqual(json.loads(argv[5]), REVIEW_SCHEMA)
        self.assertEqual(argv[5], json.dumps(REVIEW_SCHEMA, separators=(",", ":")))
        self.assertIn("quoted {review_schema} and {prompt}", argv[1])

    def test_outside_a_review_the_review_schema_placeholder_stays_literal(self):
        args = ("-p", "{prompt}", "--json-schema", "{review_schema}", "--resume", "{executor_session_id}")
        self.assertEqual(render_cli_args(args, prompt="hi {executor_session_id}", executor_session_id="s1"),
                         ["-p", "hi {executor_session_id}", "--json-schema", "{review_schema}", "--resume", "s1"])
        local = LocalCLIExecutor(worktree_manager=Mock(), inbox_checkout_map={})
        template = CLITemplate(name="grok", command="grok", args=args[:4])
        rendered = local._render_cli_args(template, prompt="task", session=None)  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(rendered, ["-p", "task", "--json-schema", "{review_schema}"])

    def test_the_example_grok_reviewer_passes_the_review_schema(self):
        raw = tomllib.loads((Path(__file__).resolve().parents[2] / "hibernaut.example.toml").read_text(encoding="utf-8"))
        template = parse_config(raw).executors["grok-reviewer"]
        self.assertEqual((template.command, template.args),
                         ("grok", ("-p", "{prompt}", "--permission-mode", "plan", "--json-schema", "{review_schema}")))

    async def test_a_failed_or_timed_out_run_aborts_without_a_retry(self):
        self.cli.set({"stdout": json.dumps(PASS), "stderr": "auth expired: run grok login\n", "exit": 3})
        with self.assertRaisesRegex(ReviewAborted, "grok: '.*review-cli' exited with code 3: auth expired: run grok login"):
            await self.reviewer().review(self.request(), self.caps)
        self.assertEqual(len(self.cli.calls()), 1)
        self.cli.set({"sleep": 5, "stdout": json.dumps(PASS)})
        with self.assertRaisesRegex(ReviewAborted, "timed out after 0.5s"):
            await self.reviewer(timeout_seconds=0.5).review(self.request(), self.caps)
        with self.assertRaisesRegex(ReviewAborted, "cannot start '/nonexistent/grok'"):
            await self.reviewer(command="/nonexistent/grok").review(self.request(), self.caps)

    async def test_a_review_that_changes_the_worktree_is_discarded(self):
        self.cli.set({"stdout": json.dumps(PASS), "write": {"calc.py": "def divide(a, b):\n    return 0\n"}})
        with self.assertRaisesRegex(ReviewAborted, "changed during the review by grok; findings discarded"):
            await self.reviewer().review(self.request(), self.caps)
        self.assertEqual(len(self.cli.calls()), 1)

    def test_launch_gate_and_output_format(self):
        seen = []
        self.caps.permissions = Mock(check=lambda tool, args: seen.append((tool, args)) or PermissionDecision("allow", "ok"))
        reviewer = self.reviewer(command="grok")
        reviewer.check_launch(self.caps, self.repo)
        self.assertEqual(seen, [("code_reviewer.launch", {"executor": "grok", "workdir": str(self.repo), "command": "grok"})])
        review = {**CHANGES_REQUESTED, "developer_messages": []}
        lines = reviewer.format_review(review).splitlines()
        self.assertEqual(lines[0], "[grok] verdict=changes_requested (cli=grok)")
        self.assertEqual(lines[1:4], ["Summary: division is unguarded", "Blocking:", "1. calc.py:2"])
        self.assertIn("Notes:", lines)


class TestSDEWithCLIReviewer(DeliveryCase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.wt = self.clone("task-1", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.cli = CLIPlan(self.root)
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan),
                                                  **self.cli.env()}))
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    def set_dev(self, *outputs):
        steps = [{"write": files, "commit": "work", "output": {
            "status": "done", "summary": summary, "test_evidence": EVIDENCE, "change_kind": "fix",
            "change_rationale": "divide handled zero wrongly"}} for files, summary in outputs]
        self.plan.write_text(json.dumps({"sde": steps, "reviewer": []}))

    def executor(self, delivery):
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=launcher),
            options=SDEOptions(review=True, reviewer="grok"),
            reviewer=CLIReviewer(template=CLITemplate(name="grok", command=cli_command(self.root), args=("-p",))),
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={},
            tasks=SQLiteTaskMetadataStore(self.db),
            delivery=delivery,
        )

    def payload(self):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero divisors",
                "explicit_workdir": str(self.wt), "base_ref": "origin/main"}

    def event(self):
        return Event(inbox_id="proj:x", source="test", external_id=f"e-{os.urandom(4).hex()}", payload=self.payload(),
                     correlation_key="task:1")

    def dev_calls(self):
        return [json.loads(ln) for ln in self.log.read_text().splitlines()] if self.log.exists() else []

    async def test_a_cli_pass_counts_as_review_pass_for_a_merge_that_requires_review(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "divide fixed"))
        self.cli.set(json.dumps({"result": json.dumps(PASS)}))
        delivery = self.delivery(auto_merge=True)
        self.assertTrue(delivery.config.require_review)
        outcome = await self.executor(delivery).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        main_sha = self.git_bare("rev-parse", "main")
        for part in ("[sde] delivered (review=pass after 1 round", "[grok] verdict=pass (cli=",
                     f"Merge commit: {main_sha}"):
            self.assertIn(part, text)
        self.assertNotIn("not merged", text)
        self.assertEqual(len(self.cli.calls()), 1)
        self.assertEqual(Path(self.cli.calls()[0]["cwd"]).resolve(), self.wt.resolve())

    def git_bare(self, *args):
        import subprocess
        return subprocess.run(["git", *args], cwd=self.bare, check=True, capture_output=True, text=True).stdout.strip()


class OrcaFailingToClose:
    # An Orca double whose `terminal close` fails; everything else goes to the wrapped one.
    def __init__(self, orca, error):
        self.orca = orca
        self.error = error
        self.closes = []

    async def __call__(self, *args, cwd):
        if args[:2] == ("terminal", "close"):
            self.closes.append(args[3])
            raise self.error
        return await self.orca(*args, cwd=cwd)


class TestCLIReviewerTerminal(TerminalCase):
    async def test_the_cli_runs_in_a_visible_terminal_and_the_verdict_comes_from_files(self):
        self.cli.set({"stdout": json.dumps(PASS), "stderr": "reading calc.py\n"})
        reviewer = self.reviewer(env={"FAKE_CLI_MARK": "from-template"})
        review = await reviewer.review(self.request(), self.caps)
        self.assertEqual((review["verdict"], review["summary"]), ("pass", "zero is rejected"))
        (handle,) = self.handles()
        term = self.orca.terminals[handle]
        self.assertEqual(term["title"], terminal_title("grok", "code-reviewer", "review of task-1 round 1"))
        self.assertEqual(term["title"], "grok (code-reviewer) · review of task-1 round 1")
        self.assertEqual(Path(term["worktree"]), self.repo)
        # The prompt reaches the CLI as its {prompt} argument but never the terminal command line.
        (call,) = self.cli.calls()
        self.assertEqual(call["argv"][0], "-p")
        self.assertIn("## Task requirements\ndivide must reject a zero divisor", call["argv"][1])
        self.assertTrue(call["argv"][1].rstrip().endswith("}"))
        self.assertEqual((Path(call["cwd"]), call["env"], call["stdin_tty"]), (self.repo, "from-template", False))
        self.assertNotIn("divide must reject", term["command"])
        self.assertLess(len(term["command"]), 400)
        # A person watching the terminal sees the CLI's output as it runs.
        screen = self.orca.screen(handle)
        self.assertIn(json.dumps(PASS), screen)
        self.assertIn("reading calc.py", screen)
        # Once the result is read the terminal is closed, so Orca cannot resume the CLI's chat in its tab; the run's
        # files are removed.
        self.assertEqual(self.closes(), [handle])
        self.assertFalse(await self.still_running(handle))
        self.assertEqual(list(self.runs.iterdir()), [])
        self.assertEqual(review["cli_terminals"], [handle])
        lines = reviewer.format_review(review).splitlines()
        self.assertEqual(lines[0], f"[grok] verdict=pass (cli={cli_command(self.root)} terminal=orca)")
        self.assertEqual(lines[1], f"Orca terminal: {handle}")
        self.assertEqual(lines[2], "Summary: zero is rejected")

    async def test_the_review_schema_placeholder_is_filled_in_the_terminal_too(self):
        self.cli.set({"stdout": json.dumps({"text": json.dumps(PASS), "structuredOutput": PASS})})
        reviewer = self.reviewer(args=("-p", "{prompt}", "--json-schema", "{review_schema}"))
        review = await reviewer.review(self.request(), self.caps)
        self.assertEqual(review["verdict"], "pass")
        (call,) = self.cli.calls()
        self.assertEqual(call["argv"][2], "--json-schema")
        self.assertEqual(json.loads(call["argv"][3]), REVIEW_SCHEMA)

    async def test_a_format_retry_runs_in_a_new_terminal(self):
        self.cli.set("Looks good to me!", f"```json\n{json.dumps(CHANGES_REQUESTED)}\n```")
        review = await self.reviewer().review(self.request(), self.caps)
        self.assertEqual(review["verdict"], "changes_requested")
        first, retry = self.handles()
        self.assertEqual([self.orca.terminals[h]["title"] for h in (first, retry)],
                         ["grok (code-reviewer) · review of task-1 round 1",
                          "grok (code-reviewer) · review of task-1 round 1 format retry"])
        self.assertIn("## Your previous output was rejected", self.cli.calls()[1]["argv"][1])
        self.assertEqual(review["cli_terminals"], [first, retry])
        self.assertEqual(self.closes(), [first, retry])

    async def test_the_result_is_read_before_the_terminal_is_closed(self):
        self.cli.set(json.dumps(PASS))
        seen = []

        async def orca(*args, cwd):
            if args[:2] == ("terminal", "close"):
                (run_dir,) = self.runs.iterdir()
                seen.append((run_dir / "exit.json").exists())
                (run_dir / "stdout").write_text("gone")
            return await self.orca(*args, cwd=cwd)

        with patch(f"{ORCA_LOG}._orca", new=orca):
            review = await self.reviewer().review(self.request(), self.caps)
        self.assertEqual((seen, review["verdict"]), ([True], "pass"))

    async def test_a_review_without_a_verdict_still_names_its_terminals_and_sessions(self):
        self.cli.set(json.dumps({"result": "fine", "session_id": "chat-1"}), "still no verdict")
        with self.assertRaises(ReviewAborted) as failed:
            await self.reviewer().review(self.request(), self.caps)
        first, retry = self.handles()
        self.assertIn("returned no valid verdict after one retry", str(failed.exception))
        self.assertTrue(str(failed.exception).endswith(f"(Orca terminal: {first} (CLI session chat-1), {retry})"),
                        str(failed.exception))
        self.assertEqual(self.closes(), [first, retry])

    async def test_a_result_that_cannot_be_read_closes_the_terminal(self):
        self.cli.set(json.dumps(PASS))
        with patch(f"{ORCA_LOG}._read_json", side_effect=lambda p: b"not json" if p.exists() else None), \
                self.assertRaisesRegex(ReviewAborted, "grok: Orca terminal term_"):
            await self.reviewer().review(self.request(), self.caps)
        (handle,) = self.handles()
        self.assertEqual(self.closes(), [handle])
        self.assertEqual(list(self.runs.iterdir()), [])

    async def test_a_terminal_that_is_already_gone_is_not_a_failed_close(self):
        self.cli.set(json.dumps(PASS))
        failing = OrcaFailingToClose(self.orca, RuntimeError('{"error": {"code": "terminal_handle_stale"}}'))

        async def orca(*args, cwd):
            if args[:2] == ("terminal", "close"):
                await self.still_running(args[3])
            return await failing(*args, cwd=cwd)

        with patch(f"{ORCA_LOG}._orca", new=orca), self.assertNoLogs(ORCA_LOG, "WARNING"):
            review = await self.reviewer().review(self.request(), self.caps)
        self.assertEqual((review["verdict"], failing.closes), ("pass", self.handles()))

    async def test_a_cancellation_during_the_close_still_closes_the_terminal(self):
        self.cli.set(json.dumps(PASS))
        closing, finish, closed = asyncio.Event(), asyncio.Event(), []

        async def orca(*args, cwd):
            if args[:2] == ("terminal", "close"):
                closing.set()
                await finish.wait()
                closed.append(args[3])
            return await self.orca(*args, cwd=cwd)

        with patch(f"{ORCA_LOG}._orca", new=orca):
            task = asyncio.create_task(self.reviewer().review(self.request(), self.caps))
            await asyncio.wait_for(closing.wait(), 10)
            task.cancel()
            finish.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(closed, self.handles())
        self.assertEqual(list(self.runs.iterdir()), [])

    async def test_a_nonzero_exit_fails_with_the_code_and_stderr(self):
        self.cli.set({"stdout": json.dumps(PASS), "stderr": "auth expired: run grok login\n", "exit": 3})
        with self.assertRaisesRegex(ReviewAborted, "grok: '.*review-cli' exited with code 3: auth expired: run grok login"):
            await self.reviewer().review(self.request(), self.caps)
        self.assertEqual(len(self.cli.calls()), 1)
        with self.assertRaisesRegex(ReviewAborted, "grok: cannot start '/nonexistent/grok'"):
            await self.reviewer(command="/nonexistent/grok").review(self.request(), self.caps)
        self.assertEqual(len(self.handles()), 2)
        self.assertEqual(self.closes(), self.handles())

    async def test_a_timeout_closes_the_terminal(self):
        self.cli.set({"wait_for_stop": True})
        with self.assertRaisesRegex(ReviewAborted, r"grok: '.*review-cli' timed out after 0.5s in Orca terminal term_"):
            await expire_when_ready(self.reviewer(timeout_seconds=0.5).review(self.request(), self.caps),
                                    0.5, lambda: bool(self.cli.calls()))
        (handle,) = self.handles()
        self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
        self.assertFalse(self.alive(handle))
        self.assertEqual(list(self.runs.iterdir()), [])

    async def test_a_terminal_closed_by_a_person_fails_the_review(self):
        self.cli.set({"sleep": 3, "stdout": json.dumps(PASS)})
        task = asyncio.create_task(self.reviewer().review(self.request(), self.caps))
        while not self.handles():
            await asyncio.sleep(0.02)
        (handle,) = self.handles()
        await self.orca.kill(handle)
        with self.assertRaisesRegex(ReviewAborted, f"grok: Orca terminal {handle} was closed before "
                                                   r"'.*review-cli' finished"):
            await task

    async def test_an_interrupt_closes_the_terminal(self):
        self.cli.set({"sleep": 3, "stdout": json.dumps(PASS)})
        self.caps.is_interrupted.side_effect = lambda: bool(self.handles())
        with self.assertRaises(asyncio.CancelledError):
            await self.reviewer().review(self.request(), self.caps)
        (handle,) = self.handles()
        self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
        self.assertFalse(self.alive(handle))

    async def test_an_orca_failure_falls_back_to_a_subprocess_and_says_so(self):
        async def broken(*args, cwd):
            raise RuntimeError("Error: no Orca worktree matches this path")

        self.cli.set(json.dumps(PASS))
        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=broken):
            reviewer = self.reviewer()
            review = await reviewer.review(self.request(), self.caps)
        self.assertEqual(review["verdict"], "pass")
        self.assertEqual(len(self.cli.calls()), 1)
        lines = reviewer.format_review(review).splitlines()
        self.assertEqual(lines[0], f"[grok] verdict=pass (cli={cli_command(self.root)} terminal=fallback)")
        self.assertEqual(lines[1], "Terminal fallback: Orca could not open a terminal (RuntimeError Error: no Orca "
                                   "worktree matches this path); ran as a subprocess")

    async def test_a_missing_orca_command_falls_back(self):
        self.cli.set(json.dumps(PASS))
        # A PATH with git but no orca.
        empty = self.root / "no-orca-path"
        empty.mkdir()
        (empty / "git").symlink_to(shutil.which("git"))
        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=REAL_ORCA), \
                patch.dict(os.environ, {"PATH": str(empty)}):
            reviewer = self.reviewer()
            review = await reviewer.review(self.request(), self.caps)
        lines = reviewer.format_review(review).splitlines()
        self.assertTrue(lines[0].endswith("terminal=fallback)"), lines[0])
        self.assertRegex(lines[1], r"^Terminal fallback: Orca could not open a terminal \(FileNotFoundError .*orca")
        self.assertEqual(len(self.cli.calls()), 1)

    async def test_a_project_without_orca_terminals_falls_back(self):
        self.cli.set(json.dumps(PASS))
        reviewer = self.reviewer(runner=TerminalUnavailable("project 'x' has terminal_backend = \"none\""))
        review = await reviewer.review(self.request(), self.caps)
        self.assertEqual(self.orca.calls, [])
        lines = reviewer.format_review(review).splitlines()
        self.assertTrue(lines[0].endswith("terminal=fallback)"), lines[0])
        self.assertEqual(lines[1], "Terminal fallback: project 'x' has terminal_backend = \"none\"; ran as a subprocess")

    async def test_without_the_option_nothing_changes(self):
        self.cli.set(json.dumps(PASS))
        reviewer = self.reviewer(terminal="none")
        review = await reviewer.review(self.request(), self.caps)
        self.assertEqual(self.orca.calls, [])
        self.assertNotIn("cli_terminals", review)
        lines = reviewer.format_review(review).splitlines()
        self.assertEqual(lines[:2], [f"[grok] verdict=pass (cli={cli_command(self.root)})", "Summary: zero is rejected"])


class TestSDEClosesReviewTerminals(TestSDEWithCLIReviewer):
    def setUp(self):
        super().setUp()
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))

    async def asyncTearDown(self):
        await self.orca.close_all()

    def executor(self, delivery):
        executor = super().executor(delivery)
        template = CLITemplate(name="grok", command=cli_command(self.root), args=("-p",), terminal="orca")
        executor._reviewer = CLIReviewer(  # pyright: ignore
            template=template,
            terminal=OrcaCommandTerminal(root=self.root / "cli-runs", poll_interval=0.02, liveness_interval=0.2))
        return executor

    def closed(self):
        return [c[3] for c in self.orca.calls if c[:2] == ("terminal", "close")]

    async def test_review_terminals_close_with_a_verified_delivery(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "first try"), ({"calc.py": "x = 2\n"}, "zero divisor rejected"))
        self.cli.set(f"```json\n{json.dumps(CHANGES_REQUESTED)}\n```", json.dumps(PASS))
        self.caps.permissions = InMemoryPermission({MERGE_TOOL: PermissionDecision("block", "not in this test")})
        outcome = await self.executor(self.delivery(close_terminal=True)).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        first, second = self.orca.terminals
        # Each was closed right after its review; closing them again with the delivery is not a failure.
        self.assertEqual(self.closed(), [first, second, first, second])
        self.assertIn(f"Review terminals: closed {first}, {second}", outcome.messages[0])
        self.assertNotIn("could not close", outcome.messages[0])
        for handle in (first, second):
            self.assertIsNotNone(self.orca.terminals[handle]["proc"].returncode)
