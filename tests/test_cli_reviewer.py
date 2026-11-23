import asyncio
import json
import os
import shutil
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.delivery import MERGE_TOOL
from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate, CoderReviewerExecutor, OrcaWorktreeManager
from agent_runtime.adapters.executor_cli import orca as orca_module
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, TerminalUnavailable, terminal_title
from agent_runtime.adapters.executor_cli.project_guidance import pin_head
from agent_runtime.adapters.executor_cli.cli_reviewer import extract_verdict
from agent_runtime.adapters.executor_cli.executor import LocalCLIExecutor, render_cli_args
from agent_runtime.adapters.executor_cli.reviewer import REVIEW_SCHEMA, ReviewAborted, ReviewRequest, validate_review
from agent_runtime.adapters.executor_cli.role_skills import builtin_skill
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit, Event, PermissionDecision
from tests.async_deadline import expire_when_ready
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import CHANGES_REQUESTED, fake_command, make_repo
from tests.test_pr_delivery import DeliveryCase
from tests.test_visible_roles import FakeOrcaTerminals

FAKE_CLI = Path(__file__).with_name("fake_cli_reviewer.py")
PASS = {"verdict": "pass", "summary": "zero is rejected", "blocking": []}
EVIDENCE = "red: test_zero FAILED\ngreen: 4 passed"
REAL_ORCA = orca_module._orca


def cli_command(root: Path) -> str:
    script = root / "review-cli"
    script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CLI}" "$@"\n')
    script.chmod(0o700)
    return str(script)


class CLIPlan:
    def __init__(self, root: Path) -> None:
        self.plan = root / "cli-plan.json"
        self.log = root / "cli-calls.jsonl"

    def env(self) -> dict[str, str]:
        return {"FAKE_CLI_PLAN": str(self.plan), "FAKE_CLI_LOG": str(self.log)}

    def set(self, *steps):
        self.plan.write_text(json.dumps([s if isinstance(s, dict) else {"stdout": s} for s in steps]))

    def calls(self):
        return [json.loads(ln) for ln in self.log.read_text().splitlines()] if self.log.exists() else []


class TestReviewerConfig(unittest.TestCase):
    def test_a_cli_template_can_review_including_the_built_in_ones(self):
        cfg = parse_config({"executors": {"sde": {"role": "sde", "reviewer": "grok"}}})
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "grok")  # type: ignore[union-attr]
        cfg = parse_config({"executors": {"mycli": {"command": "my-review", "args": ["--json", "{prompt}"]},
                                          "sde": {"role": "sde", "reviewer": "mycli"}}})
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "mycli")  # type: ignore[union-attr]
        cfg = parse_config({"executors": {"r": {"role": "code-reviewer"}, "sde": {"role": "sde", "reviewer": "r"}}})
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "r")  # type: ignore[union-attr]

    def test_a_reviewer_that_cannot_review_fails_with_the_choices(self):
        base = {"r": {"role": "code-reviewer"}, "other-sde": {"role": "sde", "review": False}}
        choices = r"\(claude, cursor-agent, dsh, grok, r\)"
        cases = {
            rf"executors\.sde: reviewer 'sde' is this executor itself; choose a code-reviewer executor or a CLI "
            rf"executor {choices} or set review = false": "sde",
            rf"executors\.sde: reviewer 'other-sde' has role 'sde', which cannot review; .*{choices}": "other-sde",
            rf"executors\.sde: reviewer 'ghost' is not a declared executor; .*{choices}": "ghost",
        }
        for message, reviewer in cases.items():
            with self.subTest(reviewer), self.assertRaisesRegex(ValueError, message):
                parse_config({"executors": {**base, "sde": {"role": "sde", "reviewer": reviewer}}})
        # With review off the reviewer is not used, so it is not checked.
        cfg = parse_config({"executors": {"sde": {"role": "sde", "review": False, "reviewer": "ghost"}}})
        self.assertFalse(cfg.roles["sde"].sde.review)  # type: ignore[union-attr]

    def test_the_old_role_and_builtin_skill_names_still_work(self):
        cfg = parse_config({"executors": {"cr": {"role": "coder-reviewer", "skills": ["builtin:coder-reviewer"]},
                                          "new": {"role": "code-reviewer", "skills": ["builtin:coder-reviewer"]},
                                          "sde": {"role": "sde", "reviewer": "cr"}}})
        skill = builtin_skill("code-reviewer")
        self.assertEqual(skill.parent.name, "code-reviewer")
        self.assertTrue(skill.is_file())
        self.assertEqual(builtin_skill("coder-reviewer"), skill)
        for name in ("cr", "new"):
            self.assertEqual((cfg.roles[name].role, cfg.roles[name].profile.skills), ("code-reviewer", (skill,)))
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "cr")  # type: ignore[union-attr]
        with self.assertRaisesRegex(ValueError, "'builtin:coder-reviewer' is not available for role 'sde'"):
            parse_config({"executors": {"sde": {"role": "sde", "review": False, "skills": ["builtin:coder-reviewer"]}}})

    def test_the_default_reviewer_falls_back_to_an_old_coder_reviewer_executor(self):
        legacy = {"coder-reviewer": {"role": "coder-reviewer"}}
        cfg = parse_config({"executors": {"sde": {"role": "sde"}, **legacy}})
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "coder-reviewer")  # type: ignore[union-attr]
        both = {**legacy, "code-reviewer": {"role": "code-reviewer"}}
        cfg = parse_config({"executors": {"sde": {"role": "sde"}, **both}})
        self.assertEqual(cfg.roles["sde"].sde.reviewer, "code-reviewer")  # type: ignore[union-attr]
        # An explicit name never falls back, and a CLI executor named coder-reviewer is not the old reviewer role.
        with self.assertRaisesRegex(ValueError, "reviewer 'code-reviewer' is not a declared executor"):
            parse_config({"executors": {"sde": {"role": "sde", "reviewer": "code-reviewer"}, **legacy}})
        with self.assertRaisesRegex(ValueError, "reviewer 'code-reviewer' is not a declared executor"):
            parse_config({"executors": {"sde": {"role": "sde"}, "coder-reviewer": {"command": "x"}}})

    def test_the_assembled_sde_reviews_through_the_configured_kind_of_reviewer(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))

        def sde_reviewer(executors):
            cfg = parse_config({"runtime": {"db_path": str(root / "hb.db"), "memory_root": str(root / "mem"),
                                            "default_executor": "sde"}, "executors": executors})
            daemon = build_daemon_from_config(cfg)
            self.addCleanup(daemon.db.close)
            worker = daemon.pool._worker_factory("anything", "w")  # pyright: ignore
            return worker._executors["sde"]._reviewer  # pyright: ignore

        cli = sde_reviewer({"sde": {"role": "sde", "reviewer": "grok"}})
        self.assertIsInstance(cli, CLIReviewer)
        self.assertEqual(cli.executor_id, "grok")
        role = sde_reviewer({"sde": {"role": "sde"}, "coder-reviewer": {"role": "coder-reviewer"}})
        self.assertIsInstance(role, CoderReviewerExecutor)
        self.assertEqual(role.executor_id, "coder-reviewer")


class TestReviewPermissionName(unittest.TestCase):
    def test_the_new_tool_name_is_allowed_and_old_rules_apply_to_it(self):
        self.assertEqual(AutoModePermissionAdapter().check("code_reviewer.launch", {}).action, "allow")
        self.assertEqual(AutoModePermissionAdapter().check("coder_reviewer.launch", {}).action, "allow")
        old_defer = AutoModePermissionAdapter(defer_tools=frozenset({"coder_reviewer.launch"}))
        self.assertEqual(old_defer.check("code_reviewer.launch", {}).action, "defer")
        old_block = AutoModePermissionAdapter(block_tools=frozenset({"coder_reviewer.launch"}))
        self.assertEqual(old_block.check("code_reviewer.launch", {}).action, "block")
        old_allow = AutoModePermissionAdapter(allow_tools=frozenset({"coder_reviewer.launch"}), defer_tools=frozenset())
        self.assertEqual(old_allow.check("code_reviewer.launch", {}).action, "allow")


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
        raw = tomllib.loads((Path(__file__).resolve().parents[1] / "hibernaut.example.toml").read_text(encoding="utf-8"))
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


class TestCLIReviewOutput(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request = ReviewRequest(workdir=Path("repo"), requirements="divide", test_evidence=EVIDENCE,
                                     base_ref="main", pin=Mock())
        self.caps = Mock(permissions=InMemoryPermission())
        self.reviewer = CLIReviewer(template=CLITemplate(name="grok", command="grok"))
        self.run_cli = self.enterContext(patch.object(self.reviewer, "_run_subprocess", new_callable=AsyncMock))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))

    async def test_a_fenced_block_or_a_result_envelope_is_unwrapped(self):
        fenced = f"Reviewed the diff.\n\n```json\n{json.dumps(CHANGES_REQUESTED, indent=2)}\n```\n"
        cases = {
            "fenced": fenced,
            "envelope with a fenced block": json.dumps({"type": "result", "result": fenced, "session_id": "s"}),
            "envelope with bare JSON": json.dumps({"result": json.dumps(CHANGES_REQUESTED)}),
        }
        for name, stdout in cases.items():
            with self.subTest(name):
                self.run_cli.return_value = stdout
                review = await self.reviewer.review(self.request, self.caps)
                self.assertEqual(review["verdict"], "changes_requested")
                self.assertEqual(review["blocking"][0]["location"], "calc.py:2")

    async def test_a_grok_json_schema_envelope_is_unwrapped(self):
        # grok 1.0.92 with --json-schema: the verdict sits in structuredOutput and, as a JSON string, in text.
        grok = {"text": json.dumps(PASS), "stopReason": "end_turn", "sessionId": "01a20300-937a-76d0-ab50-7f96ab2aff6a",
                "thought": "...", "usage": {"input_tokens": 16667, "output_tokens": 767}, "num_turns": 1,
                "total_cost_usd": 0.0146, "structuredOutput": PASS}
        text_only = {k: v for k, v in grok.items() if k != "structuredOutput"}
        cases = {
            "structuredOutput wins over text": ({**grok, "text": json.dumps(CHANGES_REQUESTED)}, "pass"),
            "text only, bare JSON": (text_only, "pass"),
            "text only, fenced block": ({**text_only, "text": f"Done.\n```json\n{json.dumps(CHANGES_REQUESTED)}\n```"},
                                        "changes_requested"),
        }
        for name, (envelope, verdict) in cases.items():
            with self.subTest(name):
                self.assertEqual(validate_review(extract_verdict(json.dumps(envelope)))["verdict"], verdict)
                self.run_cli.return_value = json.dumps(envelope, indent=2)
                review = await self.reviewer.review(self.request, self.caps)
                self.assertEqual(review["verdict"], verdict)
        with self.assertRaisesRegex(ReviewAborted, "text envelope holds no JSON object"):
            extract_verdict(json.dumps({**text_only, "text": "I could not finish."}))

    async def test_a_malformed_answer_is_retried_once_with_the_error(self):
        contradictory = {"verdict": "pass", "summary": "s", "blocking": CHANGES_REQUESTED["blocking"]}
        for name, bad in (("not json", "Looks good to me!"), ("invalid verdict", json.dumps(contradictory))):
            with self.subTest(name):
                self.run_cli.reset_mock()
                self.run_cli.side_effect = [bad, json.dumps(PASS)]
                review = await self.reviewer.review(self.request, self.caps)
                self.assertEqual(review["verdict"], "pass")
                first, retry = (call.args[0] for call in self.run_cli.await_args_list)
                self.assertNotIn("Your previous output was rejected", first)
                self.assertIn("## Your previous output was rejected", retry)
                self.assertTrue(retry.startswith(first))

    async def test_a_second_malformed_answer_aborts_the_review(self):
        self.run_cli.side_effect = ["no verdict here", '```json\n{"verdict": "maybe"}\n```']
        with self.assertRaisesRegex(ReviewAborted, "grok returned no valid verdict after one retry: the reviewer "
                                                   "returned a malformed verdict"):
            await self.reviewer.review(self.request, self.caps)
        self.assertEqual(self.run_cli.await_count, 2)

    async def test_the_record_keeps_the_terminal_and_the_cli_session(self):
        cases = {
            "session_id": ({"type": "result", "result": json.dumps(PASS), "session_id": "0d1c-chat"}, "0d1c-chat"),
            "sessionId": ({"text": json.dumps(PASS), "sessionId": "01a2-chat"}, "01a2-chat"),
            "none": (PASS, ""),
            "not an ID": ({"result": json.dumps(PASS), "session_id": "x\nVerdict: pass"}, ""),
            "not text": ({"result": json.dumps(PASS), "session_id": 7}, ""),
        }
        terminal = Mock(spec=OrcaCommandTerminal)
        reviewer = CLIReviewer(template=CLITemplate(name="grok", command="grok", terminal="orca"), terminal=terminal)
        for name, (body, session) in cases.items():
            with self.subTest(name):
                handle = f"term_{name}"
                terminal.run.return_value = orca_module.TerminalRun(handle, json.dumps(body).encode(), b"", 0)
                review = await reviewer.review(self.request, self.caps)
                self.assertEqual((review["cli_terminals"], review["cli_sessions"]),
                                 ([handle], {handle: session} if session else {}))
                shown = f"{handle} (CLI session {session})" if session else handle
                self.assertEqual(reviewer.format_review(review).splitlines()[1], f"Orca terminal: {shown}")


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

    async def test_cli_changes_requested_sends_the_blocking_items_back_for_a_fix(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "first try"), ({"calc.py": "x = 2\n"}, "zero divisor rejected"))
        self.cli.set(f"```json\n{json.dumps(CHANGES_REQUESTED)}\n```", json.dumps(PASS))
        self.caps.permissions = InMemoryPermission({MERGE_TOOL: PermissionDecision("block", "not in this test")})
        outcome = await self.executor(self.delivery()).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 2 round", outcome.messages[0])
        self.assertIn("zero divisor rejected", outcome.messages[0])
        first_dev, fix_dev = self.dev_calls()
        for part in ("calc.py:2", "divide(1, 0)", "ZeroDivisionError crashes the caller", "reject zero divisors"):
            self.assertIn(part, fix_dev["prompt"])
        first_review, rereview = self.cli.calls()
        rereview_prompt = rereview["argv"][1]
        self.assertIn("## Previous review round\n[grok] verdict=changes_requested (cli=", rereview_prompt)
        self.assertIn("## Changes since the previous review", rereview_prompt)
        self.assertIn("+x = 2", rereview_prompt)

    async def test_a_cli_review_in_round_two_is_held_to_the_same_scope(self):
        old = {"location": "old.py:1", "trigger": "y is read", "impact": "wrong value"}
        unfixed = {**CHANGES_REQUESTED["blocking"][0], "basis": "unfixed_previous", "previous_item": 1}
        late = {**old, "basis": "introduced_since_previous"}
        broke = {"location": "calc.py:1", "trigger": "import", "impact": "x is gone",
                 "basis": "introduced_since_previous"}
        self.set_dev(({"calc.py": "x = 1\n", "old.py": "y = 1\n"}, "try 1"), ({"calc.py": "x = 2\n"}, "try 2"),
                     ({"calc.py": "x = 3\n"}, "try 3"), ({"other.txt": "z\n"}, "try 4"))
        rounds = [CHANGES_REQUESTED, {**CHANGES_REQUESTED, "blocking": [unfixed, late], "notes": []},
                  {**CHANGES_REQUESTED, "blocking": [broke, late], "notes": []},
                  {**CHANGES_REQUESTED, "blocking": [late], "notes": []}]
        self.cli.set(*(f"```json\n{json.dumps(r)}\n```" for r in rounds))
        self.caps.permissions = InMemoryPermission({MERGE_TOOL: PermissionDecision("block", "not in this test")})
        executor = self.executor(self.delivery())
        executor._options = SDEOptions(review=True, reviewer="grok", max_review_rounds=4)  # pyright: ignore
        outcome = await executor.execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        for part in ("review=pass after 4 round", "round 2 changes_requested (1 blocking, 1 downgraded)",
                     "round 3 changes_requested (1 blocking, 1 downgraded)", "round 4 pass (1 downgraded)", "已降级：",
                     "[grok] verdict=pass (cli="):
            self.assertIn(part, text)
        self.assertIn("ZeroDivisionError crashes the caller", self.dev_calls()[2]["prompt"])
        self.assertIn("x is gone", self.dev_calls()[3]["prompt"])
        self.assertNotIn("old.py:1", "".join(c["prompt"] for c in self.dev_calls()))
        self.assertIn("## Incremental review", self.cli.calls()[1]["argv"][1])
        state = json.loads(self.state_file.read_text())
        followups = state["prs"][0]["body"].split("## 跟进\n")[1].split("\n\n## ")[0]
        self.assertEqual(followups.count("old.py:1"), 1)
        self.assertIn("name the helper more clearly", followups)
        self.assertEqual(text.split("跟进：\n")[1].count("old.py:1"), 1)

    def git_bare(self, *args):
        import subprocess
        return subprocess.run(["git", *args], cwd=self.bare, check=True, capture_output=True, text=True).stdout.strip()


_RUNNER = object()
ORCA_LOG = "agent_runtime.adapters.executor_cli.orca"


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


class TerminalCase(unittest.IsolatedAsyncioTestCase):
    # The CLI reviewer with terminal = "orca": FakeOrcaTerminals runs each terminal's command in a real shell, so the
    # wrapper script and the fake CLI really run, and their output lands in the terminal's screen file.
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_repo(self.root)
        self.cli = CLIPlan(self.root)
        self.enterContext(patch.dict(os.environ, self.cli.env()))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.runs = self.root / "cli-runs"
        self.terminal = OrcaCommandTerminal(root=self.runs, poll_interval=0.02, liveness_interval=0.2)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    async def asyncTearDown(self):
        await self.orca.close_all()

    def reviewer(self, runner=_RUNNER, **overrides):
        fields = {"name": "grok", "command": cli_command(self.root), "args": ("-p",), "terminal": "orca", **overrides}
        return CLIReviewer(template=CLITemplate(**fields), terminal=self.terminal if runner is _RUNNER else runner)

    def request(self, label="review of task-1 round 1"):
        return ReviewRequest(workdir=self.repo, requirements="divide must reject a zero divisor",
                             test_evidence=EVIDENCE, base_ref="main", pin=pin_head(self.repo, "test"), label=label)

    def handles(self):
        return list(self.orca.terminals)

    def alive(self, handle):
        return self.orca.terminals[handle]["proc"].returncode is None

    async def still_running(self, handle):
        # The fake cannot close a terminal whose command just ended; either way it ends at once.
        try:
            await asyncio.wait_for(self.orca.terminals[handle]["proc"].wait(), 5)
        except TimeoutError:
            return True
        return False

    def closes(self):
        return [c[3] for c in self.orca.calls if c[:2] == ("terminal", "close")]


class TestCLIReviewerCloseErrors(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_close_is_a_warning_and_changes_nothing_else(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        runs = root / "cli-runs"
        terminal = OrcaCommandTerminal(root=runs)
        reviewer = CLIReviewer(template=CLITemplate(name="grok", command="review-cli", terminal="orca",
                                                     timeout_seconds=0.5), terminal=terminal)
        request = ReviewRequest(workdir=root, requirements="divide", test_evidence=EVIDENCE, base_ref="main", pin=Mock())
        caps = Mock(permissions=InMemoryPermission())
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))
        handles = ["success", "failure", "timeout"]
        for error in (RuntimeError("Orca is not running"), AttributeError("no reply"), TimeoutError()):
            with (
                self.subTest(type(error).__name__),
                patch.object(orca_module, "_create_terminal", side_effect=[(h, None) for h in handles]),
                patch.object(terminal, "_wait", side_effect=[
                    orca_module.TerminalRun(handles[0], json.dumps(PASS).encode(), b"", 0),
                    orca_module.TerminalRun(handles[1], b"", b"auth expired", 3),
                    orca_module.ProcessTimeout("timed out after 0.5s"),
                ]),
                patch.object(orca_module, "_orca", side_effect=error) as close,
                self.assertLogs(ORCA_LOG, "WARNING") as logs,
            ):
                review = await reviewer.review(request, caps)
                self.assertEqual((review["verdict"], review["cli_terminals"]), ("pass", handles[:1]))
                with self.assertRaisesRegex(ReviewAborted, "exited with code 3: auth expired"):
                    await reviewer.review(request, caps)
                with self.assertRaisesRegex(ReviewAborted, "timed out after 0.5s"):
                    await reviewer.review(request, caps)
            self.assertEqual([c.args for c in close.await_args_list], [("terminal", "close", "--terminal", h) for h in handles])
            self.assertEqual([r.levelname for r in logs.records], ["WARNING"] * 3)
            for handle, message in zip(handles, logs.output, strict=True):
                self.assertIn(f"Could not close Orca terminal {handle}", message)
            self.assertEqual(list(runs.iterdir()), [])


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


class TestCLITerminalConfig(unittest.TestCase):
    def test_terminal_is_off_by_default_and_checked(self):
        cfg = parse_config({"executors": {"grok": {"command": "grok", "args": ["-p", "{prompt}"], "terminal": "orca"},
                                          "plain": {"command": "x"}}})
        self.assertEqual((cfg.executors["grok"].terminal, cfg.executors["plain"].terminal), ("orca", "none"))
        with self.assertRaisesRegex(ValueError, r"executors\.grok: unsupported terminal 'tmux' \(supported: none, orca\)"):
            parse_config({"executors": {"grok": {"command": "grok", "terminal": "tmux"}}})

    def test_the_assembled_cli_reviewer_gets_terminals_only_where_the_project_has_them(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        checkout = root / "checkout"
        checkout.mkdir()

        def reviewer(project):
            cfg = parse_config({
                "runtime": {"db_path": str(root / f"{project}.db"), "memory_root": str(root / "mem"),
                            "default_executor": "sde"},
                "executors": {"grok": {"command": "grok", "terminal": "orca"},
                              "sde": {"role": "sde", "reviewer": "grok"}},
                "projects": {
                    "orca": {"inbox_id": "proj:orca", "repo": "o/a", "checkout": str(checkout)},
                    "git": {"inbox_id": "proj:git", "repo": "o/b", "checkout": str(checkout), "worktree_backend": "git"},
                },
            })
            daemon = build_daemon_from_config(cfg)
            self.addCleanup(daemon.db.close)
            worker = daemon.pool._worker_factory(f"proj:{project}", "w")  # pyright: ignore
            return worker._executors["sde"]._reviewer  # pyright: ignore

        self.assertIsInstance(reviewer("orca")._terminal, OrcaCommandTerminal)  # pyright: ignore
        unavailable = reviewer("git")._terminal  # pyright: ignore
        self.assertIsInstance(unavailable, TerminalUnavailable)
        self.assertIn("project 'git' has terminal_backend = \"none\"", unavailable.reason)


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

    async def test_a_review_terminal_is_closed_before_the_next_round_starts(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "first try"), ({"calc.py": "x = 2\n"}, "zero divisor rejected"))
        self.cli.set(json.dumps({"result": json.dumps(CHANGES_REQUESTED), "session_id": "chat-1"}),
                     json.dumps({"result": json.dumps(PASS), "session_id": "chat-2"}))
        outcome = await self.executor(self.delivery()).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        first, second = self.orca.terminals
        # With close_terminal off nothing is left for the delivery, and every round's terminal and session stay known.
        verbs = [(v, c[3]) if (v := " ".join(c[:2])) == "terminal close" else v for c in self.orca.calls]
        self.assertLess(verbs.index(("terminal close", first)), len(verbs) - 1 - verbs[::-1].index("terminal create"))
        self.assertEqual(self.closed(), [first, second])
        self.assertNotIn("Review terminals:", outcome.messages[0])
        self.assertIn(f"Review runs: round 1 {first} (CLI session chat-1); round 2 {second} (CLI session chat-2)\n",
                      outcome.messages[0])

    async def test_a_failed_close_does_not_change_the_delivery(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "divide fixed"))
        self.cli.set(json.dumps(PASS))
        failing = OrcaFailingToClose(self.orca, RuntimeError("Orca is not running"))
        with patch(f"{ORCA_LOG}._orca", new=failing), self.assertLogs(ORCA_LOG, "WARNING"):
            outcome = await self.executor(self.delivery(close_terminal=True)).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (handle,) = self.orca.terminals
        # The delivery tries again.
        self.assertEqual(failing.closes, [handle, handle])
        self.assertIn("delivered (review=pass", outcome.messages[0])

    async def test_review_terminals_close_even_when_the_delivery_evidence_is_incomplete(self):
        # A review terminal is closed after its review and holds nothing to read, so missing delivery evidence does
        # not keep it; the second close only confirms that nothing of it is left.
        self.set_dev(({"calc.py": "x = 1\n"}, "divide fixed"))
        self.cli.set(json.dumps(PASS))
        outcome = await self.executor(self.delivery(close_terminal=True, open_pr=False)).execute(
            [self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (handle,) = self.orca.terminals
        self.assertIn(f"Review terminals: closed {handle}", outcome.messages[0])
        self.assertNotIn("Review terminals: kept", outcome.messages[0])
        self.assertEqual(self.closed(), [handle, handle])

    async def test_review_terminals_are_closed_once_when_closing_is_off(self):
        self.set_dev(({"calc.py": "x = 1\n"}, "divide fixed"))
        self.cli.set(json.dumps(PASS))
        outcome = await self.executor(self.delivery()).execute([self.event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (handle,) = self.orca.terminals
        # close_terminal decides about the delivery's close only.
        self.assertEqual(self.closed(), [handle])
        self.assertNotIn("Review terminals:", outcome.messages[0])
        self.assertIn(f"[grok] verdict=pass (cli={cli_command(self.root)} terminal=orca)", outcome.messages[0])
