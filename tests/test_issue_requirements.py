import json
import unittest

from agent_runtime.adapters.channels.github_poller import GitHubIssuePoller, PollTarget
from agent_runtime.adapters.channels.github_webhook import GitHubWebhookAdapter
from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.process import last_text
from agent_runtime.adapters.executor_cli.project_guidance import Selection
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions, _task_text
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, SessionRef, Silent, Suspend
from agent_runtime.engine import EventHandler
from tests.fakes import InMemoryInbox
from tests.test_coder_reviewer import fake_command
from tests.test_sde_delivery import PASS, SDEDeliveryCase, dev

REPO = "o/r"
INBOX = "proj:o/r"
KEY = "github:o/r#7"
TITLE = "Login breaks on Safari"
BODY = "Steps: open /login in Safari 17.\nExpected: the form submits."

# `gh issue list --json number,title,body,labels,updatedAt,url` and REST /repos/{repo}/issues, as each returns them.
GH_ISSUE = {
    "number": 7, "title": TITLE, "body": BODY, "updatedAt": "2026-11-22T08:00:00Z",
    "url": "https://github.com/o/r/issues/7",
    "labels": [{"id": "LA_kw1", "name": "hibernaut", "description": "", "color": "ededed"},
               {"id": "LA_kw2", "name": "release", "description": "", "color": "d73a4a"}],
}
API_ISSUE = {
    "number": 7, "title": TITLE, "body": BODY, "updated_at": "2026-11-22T08:00:00Z",
    "html_url": "https://github.com/o/r/issues/7", "state": "open",
    "labels": [{"id": 101, "node_id": "LA_kw1", "url": "https://api.github.com/repos/o/r/labels/hibernaut",
                "name": "hibernaut", "color": "ededed", "default": False, "description": None},
               {"id": 102, "node_id": "LA_kw2", "url": "https://api.github.com/repos/o/r/labels/release",
                "name": "release", "color": "d73a4a", "default": False, "description": None}],
}


def poll(issue, mode="gh"):
    inbox = InMemoryInbox()
    poller = GitHubIssuePoller(EventHandler(inbox=inbox), targets=[PollTarget(REPO, labels=("hibernaut",))],
                               mode=mode, fetcher=lambda _t: [issue])
    poller.poll_once()
    return inbox.fetch_pending(INBOX)


def webhook(body, event="issues", delivery="d-1"):
    inbox = InMemoryInbox()
    adapter = GitHubWebhookAdapter(EventHandler(inbox=inbox))
    resp = adapter.handle(json.dumps(body).encode("utf-8"),
                          {"X-GitHub-Event": event, "X-GitHub-Delivery": delivery})
    assert resp.status_code == 202, resp
    return inbox.fetch_pending(INBOX)


def webhook_issue(issue, action="opened"):
    return {"action": action, "repository": {"full_name": REPO}, "sender": {"login": "kun"},
            "issue": {"state": "open", "html_url": "https://github.com/o/r/issues/7", **issue}}


class TestIssueEventsCarryRequirements(unittest.TestCase):
    def assert_requirements(self, events, expected=f"# {TITLE}\n\n{BODY}"):
        self.assertEqual(len(events), 1)
        self.assertEqual(_task_text(events), expected)
        self.assertEqual(last_text(events, "title"), TITLE)
        self.assertIn("release", Selection.from_events(events).tags)
        # The nested issue object stays for consumers that read it.
        self.assertEqual(events[0].payload["issue"]["number"], 7)

    def test_gh_mode_poll(self):
        self.assert_requirements(poll(GH_ISSUE, mode="gh"))

    def test_api_mode_poll(self):
        self.assert_requirements(poll(API_ISSUE, mode="api"))

    def test_webhook_issues_event(self):
        self.assert_requirements(webhook(webhook_issue({k: API_ISSUE[k] for k in ("number", "title", "body",
                                                                                  "labels", "updated_at")})))

    def test_empty_body_uses_the_title_alone(self):
        # REST and webhooks send null for an empty body; gh sends "".
        for events in (poll({**GH_ISSUE, "body": ""}, mode="gh"), poll({**API_ISSUE, "body": None}, mode="api"),
                       webhook(webhook_issue({"number": 7, "title": TITLE, "body": None, "labels": []}))):
            self.assertEqual(_task_text(events), f"# {TITLE}")
            self.assertEqual(last_text(events, "title"), TITLE)

    def test_label_names_reach_the_guidance_selection_in_both_shapes(self):
        for labels in (GH_ISSUE["labels"], API_ISSUE["labels"], ["hibernaut", "release"]):
            events = poll({**GH_ISSUE, "labels": labels})
            self.assertEqual(events[0].payload["labels"], ["hibernaut", "release"])
            self.assertEqual(Selection.from_events(events).tags, ("hibernaut", "release"))

    def test_issue_comment_payload_is_left_as_github_sent_it(self):
        body = {"action": "created", "repository": {"full_name": REPO},
                "issue": {"number": 7, "title": TITLE, "body": BODY},
                "comment": {"id": 9, "body": "any update?"}}
        events = webhook(body, event="issue_comment")
        self.assertEqual(events[0].payload, body)
        self.assertEqual(_task_text(events), "")


class SDEIssueCase(SDEDeliveryCase):
    def setUp(self):
        super().setUp()
        self.primary = self.clone("primary", None)

    def sde(self, review=True, delivery=None):
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        self.manager = OrcaWorktreeManager(workspace_store=SQLiteWorkspaceStore(self.db),
                                           session_store=SQLiteSessionStoreAdapter(self.db), backend="git")
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=launcher),
            options=SDEOptions(review=review),
            reviewer=CoderReviewerExecutor(
                profile=ClaudeCodeProfile(name="coder-reviewer", launcher=launcher, model="claude-sonnet-5-5"),
                workspaces=self.workspaces, tasks=SQLiteTaskMetadataStore(self.db),
            ) if review else None,
            worktree_manager=self.manager,
            inbox_checkout_map={INBOX: self.primary},
            tasks=SQLiteTaskMetadataStore(self.db),
            delivery=delivery,
        )


class TestSDEFromIssues(SDEIssueCase):
    async def test_poller_issue_becomes_the_requirements(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        outcome = await self.sde(review=False).execute(poll(GH_ISSUE), None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn(f"## Task requirements\n# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])

    async def test_issue_with_an_empty_body_develops_from_its_title(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        outcome = await self.sde(review=False).execute(poll({**API_ISSUE, "body": None}, mode="api"),
                                                         None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertNotIn("development failed", outcome.messages[0])
        self.assertTrue(self.calls("sde")[0]["prompt"].endswith(f"## Task requirements\n# {TITLE}"))

    async def test_webhook_issue_selects_guidance_by_its_labels(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        events = webhook(webhook_issue({k: API_ISSUE[k] for k in ("number", "title", "body", "labels")}))
        outcome = await self.sde(review=False).execute(events, None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn(f"# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])
        record = SQLiteTaskMetadataStore(self.db).get(self.manager.workspace_key(INBOX, KEY))
        assert record is not None
        self.assertEqual(record.tags, ("hibernaut", "release"))

    async def test_a_comment_without_a_task_starts_nothing_and_does_not_fail(self):
        body = {"action": "created", "repository": {"full_name": REPO},
                "issue": {"number": 7, "title": TITLE, "body": BODY}, "comment": {"id": 9, "body": "ping"}}
        events = webhook(body, event="issue_comment")
        outcome = await self.sde().execute(events, None, self.caps)
        self.assertIsInstance(outcome, Silent)
        self.assertEqual(self.calls(), [])
        session = SessionRef(session_id="s1", inbox_id=INBOX, correlation_key=KEY, executor_id="sde",
                             status="active")
        self.assertIsInstance(await self.sde().execute(events, session, self.caps), Silent)

    async def test_a_comment_on_a_suspended_task_keeps_the_open_request(self):
        self.set_plan(sde=[{"write": {}, "output": {"status": "needs_input", "summary": "s", "test_evidence": "",
                                                   "question": "Which browser versions?", "change_kind": "fix",
                                                   "change_rationale": "r"}}])
        executor = self.sde(review=False)
        first = await executor.execute(poll(GH_ISSUE), None, self.caps)
        assert isinstance(first, Suspend), first
        body = {"action": "created", "repository": {"full_name": REPO},
                "issue": {"number": 7, "title": TITLE, "body": BODY}, "comment": {"id": 9, "body": "ping"}}
        again = await executor.execute(webhook(body, event="issue_comment"), self.resume(first, key=KEY),
                                       self.caps)
        assert isinstance(again, Suspend), again
        self.assertEqual(again.request_id, first.request_id)
        self.assertEqual(len(self.calls("sde")), 1)


class TestIssueToPullRequest(SDEIssueCase):
    async def test_polled_issue_is_developed_and_delivered_as_a_pr_titled_after_the_issue(self):
        self.set_plan(sde=[dev({"calc.py": "def divide(a, b):\n    return a / b\n"}, kind="fix")],
                      reviewer=[PASS])
        outcome = await self.sde(delivery=self.delivery()).execute(poll(GH_ISSUE), None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Delivery: pr_open - PR created", outcome.messages[0])
        self.assertIn(f"# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["title"], f"Pkg 0.4.1: {TITLE}")


if __name__ == "__main__":
    unittest.main()
