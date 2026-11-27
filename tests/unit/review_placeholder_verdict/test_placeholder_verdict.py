from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate
from agent_runtime.adapters.executor_cli.cli_reviewer import extract_verdict
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, ReviewRequest, validate_review
from tests.support.cli_reviewer import EVIDENCE, PASS
from tests.support.coder_reviewer import CHANGES_REQUESTED

PLACEHOLDER = {
    "verdict": "pass",
    "summary": "placeholder — reviewing the diff against the delivery and flake requirements before a real verdict.",
    "blocking": [],
}
FINAL = {"verdict": "pass", "summary": "the flake fix holds: retries are bounded and CI waits on the head", "blocking": []}


def cancelled_grok(text: str) -> str:
    # What grok -p --json-schema printed on 2026-11-28 (reproduced): a tool call the headless run could not approve
    # cancelled the turn after the model's first message, and the run still exited 0.
    return json.dumps({
        "text": text, "stopReason": "cancelled", "sessionId": "01a21dce-f6d0-7a70-9882-83711ef465a2",
        "num_turns": 1, "structuredOutput": None, "structuredOutputError": "model did not produce structured output",
    }, indent=2)


class TestPlaceholderThenFinal(unittest.TestCase):
    def test_the_last_verdict_in_the_text_is_taken(self):
        text = f"{json.dumps(PLACEHOLDER)}\n\n{json.dumps(FINAL)}"
        cases = {
            "plain output": text,
            "text envelope": json.dumps({"text": text, "stopReason": "end_turn", "sessionId": "s"}),
            "result envelope": json.dumps({"type": "result", "result": text, "session_id": "s"}),
        }
        for name, stdout in cases.items():
            with self.subTest(name):
                self.assertEqual(validate_review(extract_verdict(stdout)), {**FINAL, "notes": [], "developer_messages": []})

    def test_a_later_fenced_answer_beats_an_earlier_bare_placeholder(self):
        stdout = f"{json.dumps(PLACEHOLDER)}\n\nDone reading.\n```json\n{json.dumps(CHANGES_REQUESTED)}\n```"
        self.assertEqual(validate_review(extract_verdict(stdout))["verdict"], "changes_requested")


class TestPlaceholderOnly(unittest.TestCase):
    def test_a_turn_that_did_not_end_normally_has_no_verdict(self):
        for reason in ("cancelled", "max_tokens", "refusal", "hook_stopped", "something_new"):
            with self.subTest(reason):
                stdout = json.dumps({"text": json.dumps(FINAL), "stopReason": reason, "structuredOutput": None})
                with self.assertRaisesRegex(ReviewAborted, f"turn ended '{reason}'"):
                    extract_verdict(stdout)
        with self.assertRaisesRegex(ReviewAborted, "turn ended 'cancelled'"):
            extract_verdict(cancelled_grok(json.dumps(PLACEHOLDER)))

    def test_an_error_result_has_no_verdict(self):
        stdout = json.dumps({"type": "result", "subtype": "error_during_execution", "is_error": True,
                             "result": json.dumps(FINAL)})
        with self.assertRaisesRegex(ReviewAborted, "reported an error"):
            extract_verdict(stdout)

    def test_a_placeholder_summary_is_not_a_pass(self):
        summaries = (
            PLACEHOLDER["summary"],
            "placeholder — reading the omitted diff and deploy sources before judging.",
            "Placeholder: real review follows",
            "TODO",
            "TBD - still reading",
            "WIP",
            "Pending a final verdict.",
            "Reading the sources before the final verdict.",
            "",
            "   ",
            "…",
        )
        for summary in summaries:
            with self.subTest(summary):
                with self.assertRaisesRegex(ReviewAborted, "summary"):
                    validate_review({**PLACEHOLDER, "summary": summary})


class TestNormalVerdictsUnchanged(unittest.TestCase):
    def test_real_summaries_that_mention_the_word_still_pass(self):
        # Real summaries from #33 and #34: the word alone does not make a placeholder.
        for summary in (
            "docs/architecture.md describes the new output format and placeholder rules.",
            "The Caddyfile uses the placeholder domain sb.example.com.",
            "ok",
            "zero is rejected",
            "No TODO markers remain in the change.",
        ):
            with self.subTest(summary):
                self.assertEqual(validate_review({**PASS, "summary": summary})["verdict"], "pass")

    def test_normal_outputs_parse_as_before(self):
        grok = {"text": json.dumps(PASS), "stopReason": "end_turn", "sessionId": "s", "structuredOutput": PASS}
        # claude -p --json-schema reports stop_reason "tool_use" on success (claude 2026-11-28).
        claude = {"type": "result", "subtype": "success", "is_error": False, "result": json.dumps(PASS),
                  "stop_reason": "tool_use", "structured_output": PASS}
        for name, stdout in {"bare": json.dumps(PASS), "grok": json.dumps(grok), "claude": json.dumps(claude),
                             "fenced": f"Reviewed.\n```json\n{json.dumps(PASS)}\n```"}.items():
            with self.subTest(name):
                self.assertEqual(validate_review(extract_verdict(stdout))["summary"], PASS["summary"])

    def test_a_changes_requested_verdict_keeps_its_summary_rules(self):
        self.assertEqual(validate_review({**CHANGES_REQUESTED, "summary": ""})["verdict"], "changes_requested")


class TestReviewerRetriesAPlaceholder(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request = ReviewRequest(workdir=Path("repo"), requirements="divide", test_evidence=EVIDENCE,
                                     base_ref="main", pin=Mock())
        self.caps = Mock()
        self.reviewer = CLIReviewer(template=CLITemplate(name="grok", command="grok"))
        self.run_cli = self.enterContext(patch.object(self.reviewer, "_run_subprocess", new_callable=AsyncMock))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))

    async def test_a_cancelled_placeholder_run_is_retried_and_the_real_verdict_kept(self):
        self.run_cli.side_effect = [cancelled_grok(json.dumps(PLACEHOLDER)), json.dumps(CHANGES_REQUESTED)]
        review = await self.reviewer.review(self.request, self.caps)
        self.assertEqual(review["verdict"], "changes_requested")
        retry = self.run_cli.await_args_list[1].args[0]
        self.assertIn("turn ended 'cancelled'", retry)

    async def test_two_placeholders_fail_the_review_instead_of_passing_it(self):
        self.run_cli.side_effect = [cancelled_grok(json.dumps(PLACEHOLDER)), json.dumps(PLACEHOLDER)]
        with self.assertRaisesRegex(ReviewAborted, "grok returned no valid verdict after one retry: .*summary"):
            await self.reviewer.review(self.request, self.caps)
