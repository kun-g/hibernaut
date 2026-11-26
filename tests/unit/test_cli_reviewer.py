import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.delivery import DeliveryRecord
from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate, CoderReviewerExecutor
from agent_runtime.adapters.executor_cli import orca as orca_module
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, TerminalUnavailable
from agent_runtime.adapters.executor_cli.cli_reviewer import extract_verdict
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, ReviewRequest, validate_review
from agent_runtime.adapters.executor_cli.role_skills import builtin_skill
from agent_runtime.adapters.executor_cli.sde import SDEOptions
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import CHANGES_REQUESTED
from tests.support.pr_delivery import rendered_body
from tests.support.review_flow_support import DONE, REPORT, ReviewFlowCase
from tests.support.cli_reviewer import EVIDENCE, ORCA_LOG, PASS


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


class TestCLIReviewerRounds(ReviewFlowCase):
    terminal = False

    async def reviewed_rounds(self, reviews, changes):
        template = CLITemplate(name="grok", command="grok", args=("-p",), terminal="orca" if self.terminal else "none")
        terminal = Mock(spec=OrcaCommandTerminal) if self.terminal else None
        cli = CLIReviewer(template=template, terminal=terminal)
        outputs = [f"```json\n{json.dumps(review)}\n```" for review in reviews]
        self.prompts = []
        if terminal is not None:
            async def reply(**command):
                self.prompts.append(command["argv"][2])
                index = len(self.prompts) - 1
                return orca_module.TerminalRun(f"term_{index}", outputs[index].encode(), b"", 0)
            terminal.run.side_effect = reply
        else:
            async def stdout(prompt, cwd, caps):
                self.prompts.append(prompt)
                return outputs[len(self.prompts) - 1]
            self.enterContext(patch.object(cli, "_run_subprocess", new=stdout))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))
        self.executor._reviewer = cli
        self.executor._options = SDEOptions(review=True, reviewer="grok", max_review_rounds=4)
        self.delivery.config.close_terminal = False
        snaps = [str(i) for i in range(len(reviews))]
        diffs = {(a, b): (f"+++ b/{name}\n+x = 2", [name]) for a, b, name in zip(snaps[:-1], snaps[1:], changes, strict=True)}
        reports = [REPORT] * (len(reviews) - 1) + [{**REPORT, "summary": "zero divisor rejected"}]
        outcome = await self.run_flow(snapshots=snaps, diffs=diffs, reviews=reviews, reports=reports, deliveries=[DONE])
        self.assertIsInstance(outcome, Emit)
        self.assertEqual(len(self.prompts), len(reviews))
        return outcome.messages[0]

    async def test_cli_changes_requested_sends_the_blocking_items_back_for_a_fix(self):
        text = await self.reviewed_rounds([CHANGES_REQUESTED, PASS], ["calc.py"])
        self.assertIn("review=pass after 2 round", text)
        self.assertIn("zero divisor rejected", text)
        first, fix = self.develop.await_args_list
        for part in ("calc.py:2", "divide(1, 0)", "ZeroDivisionError crashes the caller", "reject zero divisors"):
            self.assertIn(part, fix.args[3])
        self.assertIn("## Previous review round\n[grok] verdict=changes_requested (cli=", self.prompts[1])
        self.assertIn("## Changes since the previous review", self.prompts[1])
        self.assertIn("+x = 2", self.prompts[1])

    async def test_a_cli_review_in_round_two_is_held_to_the_same_scope(self):
        late = {"location": "old.py:1", "trigger": "y is read", "impact": "wrong value", "basis": "introduced_since_previous"}
        unfixed = {**CHANGES_REQUESTED["blocking"][0], "basis": "unfixed_previous", "previous_item": 1}
        broke = {"location": "calc.py:1", "trigger": "import", "impact": "x is gone", "basis": "introduced_since_previous"}
        rounds = [CHANGES_REQUESTED, {**CHANGES_REQUESTED, "blocking": [unfixed, late], "notes": []},
                  {**CHANGES_REQUESTED, "blocking": [broke, late], "notes": []},
                  {**CHANGES_REQUESTED, "blocking": [late], "notes": []}]
        text = await self.reviewed_rounds(rounds, ["calc.py", "calc.py", "other.txt"])
        for part in ("review=pass after 4 round", "round 2 changes_requested (1 blocking, 1 downgraded)",
                     "round 3 changes_requested (1 blocking, 1 downgraded)", "round 4 pass (1 downgraded)", "已降级：",
                     "[grok] verdict=pass (cli="):
            self.assertIn(part, text)
        prompts = [c.args[3] for c in self.develop.await_args_list]
        self.assertIn("ZeroDivisionError crashes the caller", prompts[2])
        self.assertIn("x is gone", prompts[3])
        self.assertNotIn("old.py:1", "".join(prompts))
        self.assertIn("## Incremental review", self.prompts[1])
        request = self.delivery.deliver.call_args.args[0]
        followups = rendered_body(followups=request.followups).split("## 跟进\n")[1].split("\n\n## ")[0]
        self.assertEqual(followups.count("old.py:1"), 1)
        self.assertIn("name the helper more clearly", followups)
        self.assertEqual(text.split("跟进：\n")[1].count("old.py:1"), 1)


class TestCLIReviewerTerminalRounds(TestCLIReviewerRounds):
    terminal = True


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


class TestReviewTerminalDelivery(ReviewFlowCase):
    def setUp(self):
        super().setUp()
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        terminal = OrcaCommandTerminal(root=root)
        self.executor._reviewer = CLIReviewer(template=CLITemplate(name="grok", command="review-cli", terminal="orca"),
                                              terminal=terminal)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))
        self.create = self.enterContext(patch.object(orca_module, "_create_terminal"))
        self.wait = self.enterContext(patch.object(terminal, "_wait"))
        self.orca = self.enterContext(patch.object(orca_module, "_orca"))
        self.operations = Mock()
        self.operations.attach_mock(self.create, "create")
        self.operations.attach_mock(self.orca, "orca")

    async def finish(self, *reviews, close=False, record=DONE):
        handles = [f"term-{i}" for i in range(1, len(reviews) + 1)]
        self.create.side_effect = [(handle, None) for handle in handles]
        self.wait.side_effect = [orca_module.TerminalRun(handle, json.dumps(review).encode(), b"", 0)
                                 for handle, review in zip(handles, reviews, strict=True)]
        self.delivery.config.close_terminal = close
        outcome = await self.run_flow(snapshots=handles, reviews=reviews, deliveries=[record],
                                      diffs={("term-1", "term-2"): ("fix", ["calc.py"])})
        self.assertIsInstance(outcome, Emit)
        return outcome.messages[0]

    def closed(self):
        return [c.args[3] for c in self.orca.await_args_list if c.args[:2] == ("terminal", "close")]

    async def test_a_review_terminal_is_closed_before_the_next_round_starts(self):
        text = await self.finish({"result": json.dumps(CHANGES_REQUESTED), "session_id": "chat-1"},
                                 {"result": json.dumps(PASS), "session_id": "chat-2"})
        first, second = "term-1", "term-2"
        # With close_terminal off nothing is left for the delivery, and every round's terminal and session stay known.
        self.assertEqual([name for name, _, _ in self.operations.mock_calls], ["create", "orca", "create", "orca"])
        self.assertEqual(self.closed(), [first, second])
        self.assertNotIn("Review terminals:", text)
        self.assertIn(f"Review runs: round 1 {first} (CLI session chat-1); round 2 {second} (CLI session chat-2)\n", text)

    async def test_a_failed_close_does_not_change_the_delivery(self):
        self.orca.side_effect = RuntimeError("Orca is not running")
        with self.assertLogs(ORCA_LOG, "WARNING"):
            text = await self.finish(PASS, close=True)
        self.assertEqual(self.closed(), ["term-1", "term-1"])
        self.assertIn("delivered (review=pass", text)

    async def test_review_terminals_close_even_when_the_delivery_evidence_is_incomplete(self):
        # A review terminal is closed after its review and holds nothing to read, so missing delivery evidence does
        # not keep it; the second close only confirms that nothing of it is left.
        text = await self.finish(PASS, close=True, record=DeliveryRecord(state="local"))
        self.assertIn("Review terminals: closed term-1", text)
        self.assertNotIn("Review terminals: kept", text)
        self.assertEqual(self.closed(), ["term-1", "term-1"])

    async def test_review_terminals_are_closed_once_when_closing_is_off(self):
        text = await self.finish(PASS)
        self.assertEqual(self.closed(), ["term-1"])
        self.assertNotIn("Review terminals:", text)
        self.assertIn("[grok] verdict=pass (cli=review-cli terminal=orca)", text)
