import dataclasses
import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.delivery import (
    MERGE_TOOL,
    DeliveryConfig,
    DeliveryRecord,
    DeliveryRequest,
    PRDelivery,
    bump_version,
    parse_delivery,
    source_issue,
)
from agent_runtime.adapters.delivery.pr_delivery import _Handback
from agent_runtime.core.models import DeliveryOutcome, Event, PermissionDecision
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import rendered_body


class TestVersionRules(unittest.TestCase):
    def test_bump_follows_semver(self):
        self.assertEqual(bump_version("0.4.0", "major"), "1.0.0")
        self.assertEqual(bump_version("0.4.2", "minor"), "0.5.0")
        self.assertEqual(bump_version("0.4.2", "patch"), "0.4.3")
        self.assertEqual(bump_version("0.4.2", "skip"), "0.4.2")
        with self.assertRaisesRegex(ValueError, "not MAJOR.MINOR.PATCH"):
            bump_version("0.4", "patch")

    def test_parse_delivery_keeps_the_two_grants_separate_and_rejects_bad_settings(self):
        cfg = parse_delivery("projects.x.delivery", {"open_pr": True, "checks": [["pytest", "-v"]]}, repo="o/r")
        self.assertEqual((cfg.repo, cfg.open_pr, cfg.auto_merge, cfg.checks), ("o/r", True, False, (("pytest", "-v"),)))
        self.assertEqual(parse_delivery("d", {}, repo="o/r").open_pr, False)
        cases = {
            "auto_merge needs open_pr = true": {"auto_merge": True},
            "unknown key": {"open_pr": True, "token": "x"},
            "version_file must be a path inside the repository": {"version_file": "../VERSION"},
            "needs a named group": {"version_pattern": "[0-9.]+"},
            "title_template may use only": {"title_template": "{title} {sha}"},
            "each a non-empty list of strings": {"checks": ["pytest -v"]},
            "merge_method must be one of": {"merge_method": "fast-forward"},
            "open_pr must be true or false": {"open_pr": "yes"},
            "max_handbacks must be a non-negative integer": {"max_handbacks": -1},
        }
        for message, raw in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_delivery("d", raw, repo="o/r")

    def test_issue_link_defaults_to_refs_and_rejects_other_values(self):
        self.assertEqual(parse_delivery("d", {}, repo="o/r").issue_link, "refs")
        for value in ("closes", "refs", "none"):
            self.assertEqual(parse_delivery("d", {"issue_link": value}, repo="o/r").issue_link, value)
        for bad in ("close", "fixes", "", True):
            with self.subTest(bad), self.assertRaisesRegex(
                ValueError, r"d: issue_link must be one of closes, refs, none"
            ):
                parse_delivery("d", {"issue_link": bad}, repo="o/r")

    def test_hand_backs_to_the_developer_default_to_two_and_can_be_turned_off(self):
        self.assertEqual(parse_delivery("d", {}, repo="o/r").max_handbacks, 2)
        self.assertEqual(parse_delivery("d", {"max_handbacks": 0}, repo="o/r").max_handbacks, 0)
        for bad in (True, 1.5, "2"):
            with self.subTest(bad), self.assertRaisesRegex(ValueError, "max_handbacks must be a non-negative integer"):
                parse_delivery("d", {"max_handbacks": bad}, repo="o/r")


def _evt(source, payload=None, key=None):
    return Event(inbox_id="proj:x", source=source, external_id=source, payload=payload or {}, correlation_key=key)


class TestSourceIssue(unittest.TestCase):
    def test_issue_events_and_issue_correlation_keys_name_the_issue(self):
        issue = {"repository": {"full_name": "o/r"}, "issue": {"number": 7, "url": "https://x/o/r/issues/7"}}
        cases = {
            "poller": ([_evt("github:issue_poll", issue, "github:o/r#7")], None),
            "webhook issues": ([_evt("github:issues", issue)], None),
            "webhook issue_comment": ([_evt("github:issue_comment", issue)], None),
            "string number": ([_evt("github:issue_poll", {**issue, "issue": {"number": "7"}})], None),
            "issue repo from the key": ([_evt("github:issue_poll", {"issue": {"number": 7}}, "github:o/r#7")], None),
            "relayed key": ([_evt("switchboard:task", {"text": "x"}, "github:o/r#7")], None),
            "session key": ([_evt("switchboard:task", {"text": "x"})], "github:o/r#7"),
        }
        for name, (events, key) in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue(events, key), "o/r#7")

    def test_other_sources_and_missing_numbers_name_no_issue(self):
        cases = {
            "/events": ([_evt("http", {"text": "x"}, "task:1")], None),
            "drive": ([_evt("gdrive:task", {"text": "x"}, "gdrive:abc")], "gdrive:abc"),
            "feishu": ([_evt("feishu:message", {"text": "x"}, "feishu:c:t")], None),
            "mcp": ([_evt("mcp", {"text": "x"})], None),
            "pull request": ([_evt("github:pull_request", {"pull_request": {"number": 5}}, "github:o/r#5")], None),
            "no number": ([_evt("github:issue_poll", {"repository": {"full_name": "o/r"}, "issue": {}})], None),
            "bad number": ([_evt("github:issues", {"repository": {"full_name": "o/r"}, "issue": {"number": "x"}})],
                           None),
            "bool number": ([_evt("github:issues", {"repository": {"full_name": "o/r"}, "issue": {"number": True}})],
                            None),
            "no repo": ([_evt("github:issues", {"issue": {"number": 7}})], None),
            "bad key": ([_evt("http", {"text": "x"}, "github:o/r#x")], None),
        }
        for name, (events, key) in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue(events, key), "")

    def test_events_about_a_pull_request_name_no_issue_even_with_the_task_key(self):
        # Called the way SDEExecutor._new_state calls it: the batch's github key as the second argument.
        key = "github:o/r#5"
        repo = {"full_name": "o/r"}
        pr_issue = {"number": 5, "pull_request": {"url": "https://api.github.example/repos/o/r/pulls/5"}}
        cases = {
            "webhook issues on a PR": _evt("github:issues", {"repository": repo, "issue": pr_issue}, key),
            "webhook issue_comment on a PR": _evt("github:issue_comment", {"repository": repo, "issue": pr_issue}, key),
            "switchboard review": _evt("sb:github", {"event_type": "conversation.message", "body": "[review approved]",
                                                     "metadata": {"github_event": "pull_request_review",
                                                                  "number": 5}}, key),
            "switchboard review comment": _evt("sb:github", {"metadata": {
                "github_event": "pull_request_review_comment", "number": 5}}, key),
            "switchboard PR closed": _evt("sb:github", {"event_type": "work.closed", "metadata": {
                "work_kind": "pull_request", "github_event": "pull_request", "number": 5}}, key),
            "switchboard comment on a PR": _evt("sb:github", {"metadata": {
                "github_event": "issue_comment", "work_kind": "pull_request", "number": 5}}, key),
        }
        for name, evt in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue([evt], key), "")

    def test_switchboard_issue_events_still_name_the_issue(self):
        key = "github:o/r#7"
        for metadata in ({"work_kind": "issue", "github_event": "issues"},
                         {"work_kind": "issue", "github_event": "issue_comment"}):
            with self.subTest(**metadata):
                self.assertEqual(source_issue([_evt("sb:github", {"metadata": metadata}, key)], key), "o/r#7")


class TestPullRequestBody(unittest.TestCase):
    def test_pr_body_opens_with_the_source_issue_line_for_each_issue_link(self):
        cases = {
            ("closes", "o/r#7"): "Closes #7\n\n## Summary",
            ("refs", "o/r#7"): "Refs #7\n\n## Summary",
            ("closes", "other/lib#12"): "Closes other/lib#12\n\n## Summary",
            ("refs", "Other/Lib#12"): "Refs Other/Lib#12\n\n## Summary",
            ("closes", "O/R#3"): "Closes #3\n\n## Summary",
            ("none", "o/r#7"): "## Summary",
            ("closes", ""): "## Summary",
        }
        for (link, issue), start in cases.items():
            with self.subTest(link=link, issue=issue):
                body = rendered_body(issue=issue, repo="o/r", link=link)
                self.assertTrue(body.startswith(start), body[:80])

    def test_without_a_configured_repo_the_issue_is_written_in_full(self):
        self.assertTrue(rendered_body(issue="o/r#7", link="closes").startswith("Closes o/r#7\n\n## Summary"))


class TestMergePolicy(unittest.TestCase):
    url = "https://github.example/o/r/pull/1"

    def setUp(self):
        self.config = DeliveryConfig(repo="o/r", open_pr=True, auto_merge=True)
        self.delivery = PRDelivery(self.config, Mock(), env={})
        self.request = DeliveryRequest(work_key="task:1", workdir=Path("repo"), title="t", summary="s", test_evidence="e",
                                       change_kind="fix", rationale="r", review="pass")
        self.record = DeliveryRecord(state="pr_open", head_sha="a" * 40, pr_number=1, pr_url=self.url)
        self.gh = self.enterContext(patch.object(self.delivery, "_gh", side_effect=AssertionError("unexpected gh call")))

    def merge(self, review="pass", permissions=None):
        self.delivery._merge(dataclasses.replace(self.request, review=review), self.record,
                             permissions or InMemoryPermission(), frozenset(), lambda: False)
        return self.delivery.outcome(self.record)

    def test_merge_needs_a_passing_independent_review(self):
        for review in ("off", "pending"):
            with self.subTest(review):
                self.record.notes.clear()
                self.merge(review)
                self.assertEqual(self.record.state, "pr_open")
                self.assertIn(f"needs a passing independent review (review={review})", self.record.notes[0])
        self.gh.assert_not_called()

    def failed_ci(self):
        self.gh.side_effect = None
        self.gh.return_value = subprocess.CompletedProcess([], 1, '[{"name":"gates","state":"FAILURE","bucket":"fail"}]', "")
        with self.assertRaises(_Handback) as raised:
            self.merge()
        # A check with no GitHub Actions run to re-run goes straight back to the developer.
        self.assertEqual([call.args[1:3] for call in self.gh.call_args_list], [("pr", "checks")])
        self.assertEqual(raised.exception.detail["kind"], "ci")
        return self.delivery.outcome(self.record)

    def test_failed_ci_does_not_merge_and_is_handed_back(self):
        self.failed_ci()
        self.assertEqual(self.record.state, "pr_open")
        self.assertIn("not merged: CI failed (gates)", self.record.notes)

    def test_outcome_names_why_an_authorized_merge_did_not_happen(self):
        self.assertEqual(self.merge("pending"), DeliveryOutcome(
            state="pr_open", pr_url=self.url, auto_merge=True, hold="review",
            hold_detail="merging needs a passing independent review (review=pending)"))
        outcome = self.failed_ci()
        self.assertEqual((outcome.state, outcome.auto_merge, outcome.hold, outcome.hold_detail, outcome.pr_url),
                         ("pr_open", True, "ci", "CI failed (gates)", self.url))
        self.record = dataclasses.replace(self.record, merge_hold="", merge_hold_detail="", notes=[])
        block = InMemoryPermission({MERGE_TOOL: PermissionDecision("block", "no merges today")})
        outcome = self.merge(permissions=block)
        self.assertEqual((outcome.state, outcome.hold, outcome.hold_detail),
                         ("pr_open", "other", "delivery.merge blocked: no merges today"))

    def test_outcome_has_no_hold_when_merging_is_not_authorized_or_done(self):
        manual = PRDelivery(dataclasses.replace(self.config, auto_merge=False), Mock(), env={})
        self.assertEqual(manual.outcome(self.record), DeliveryOutcome(state="pr_open", pr_url=self.url, auto_merge=False))
        merged = dataclasses.replace(self.record, state="merged")
        self.assertEqual(self.delivery.outcome(merged), DeliveryOutcome(state="merged", pr_url=self.url, auto_merge=True))


if __name__ == "__main__":
    unittest.main()
