import unittest

from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardInboundBridge
from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, SessionRef, Silent, Suspend
from agent_runtime.engine import EventHandler
from switchboard.envelope import CanonicalEvent
from tests.fakes import InMemoryInbox
from tests.test_coder_reviewer import fake_command
from tests.test_sde_delivery import PASS, SDEDeliveryCase, dev

REPO = "o/r"
INBOX = "proj:o/r"
KEY = "github:o/r#7"
TITLE = "Login breaks on Safari"
BODY = "Steps: open /login in Safari 17.\nExpected: the form submits."

def relayed_issue(body=BODY):
    inbox = InMemoryInbox()
    event = CanonicalEvent(
        source="github", event_type="work.opened", scope=INBOX, correlation_key=KEY,
        external_id="issue:o/r#7:opened:t", subject=TITLE, body=body,
        metadata={"github_event": "issues", "work_kind": "issue", "state": "open", "repo": REPO,
                  "number": 7, "labels": ["hibernaut", "release"]},
    )
    bridge = SwitchboardInboundBridge(EventHandler(inbox=inbox))
    assert bridge.handle_canonical_event(event) == "acked"
    return inbox.fetch_pending(INBOX)


def queued_legacy_comment():
    return [Event(inbox_id=INBOX, correlation_key=KEY, source="github:issue_comment", external_id="d-1",
                  payload={"action": "created", "repository": {"full_name": REPO},
                           "issue": {"number": 7, "title": TITLE, "body": BODY}, "comment": {"id": 9, "body": "ping"}})]


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
    async def test_relayed_issue_becomes_the_requirements(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        outcome = await self.sde(review=False).execute(relayed_issue(), None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn(f"## Task requirements\n# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])

    async def test_issue_with_an_empty_body_develops_from_its_title(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        outcome = await self.sde(review=False).execute(relayed_issue(body=""), None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertNotIn("development failed", outcome.messages[0])
        self.assertTrue(self.calls("sde")[0]["prompt"].endswith(f"## Task requirements\n# {TITLE}"))

    async def test_relayed_issue_selects_guidance_by_its_labels(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        events = relayed_issue()
        outcome = await self.sde(review=False).execute(events, None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn(f"# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])
        record = SQLiteTaskMetadataStore(self.db).get(self.manager.workspace_key(INBOX, KEY))
        assert record is not None
        self.assertEqual(record.tags, ("hibernaut", "release"))

    async def test_a_queued_legacy_comment_without_a_task_starts_nothing_and_does_not_fail(self):
        events = queued_legacy_comment()
        outcome = await self.sde().execute(events, None, self.caps)
        self.assertIsInstance(outcome, Silent)
        self.assertEqual(self.calls(), [])
        session = SessionRef(session_id="s1", inbox_id=INBOX, correlation_key=KEY, executor_id="sde",
                             status="active")
        self.assertIsInstance(await self.sde().execute(events, session, self.caps), Silent)

    async def test_a_queued_legacy_comment_on_a_suspended_task_keeps_the_open_request(self):
        self.set_plan(sde=[{"write": {}, "output": {"status": "needs_input", "summary": "s", "test_evidence": "",
                                                   "question": "Which browser versions?", "change_kind": "fix",
                                                   "change_rationale": "r"}}])
        executor = self.sde(review=False)
        first = await executor.execute(relayed_issue(), None, self.caps)
        assert isinstance(first, Suspend), first
        again = await executor.execute(queued_legacy_comment(), self.resume(first, key=KEY),
                                       self.caps)
        assert isinstance(again, Suspend), again
        self.assertEqual(again.request_id, first.request_id)
        self.assertEqual(len(self.calls("sde")), 1)


class TestIssueToPullRequest(SDEIssueCase):
    async def test_relayed_issue_is_developed_and_delivered_as_a_pr_titled_after_the_issue(self):
        self.set_plan(sde=[dev({"calc.py": "def divide(a, b):\n    return a / b\n"}, kind="fix")],
                      reviewer=[PASS])
        outcome = await self.sde(delivery=self.delivery()).execute(relayed_issue(), None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Delivery: pr_open - PR created", outcome.messages[0])
        self.assertIn(f"# {TITLE}\n\n{BODY}", self.calls("sde")[0]["prompt"])
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["title"], f"Pkg 0.4.1: {TITLE}")


if __name__ == "__main__":
    unittest.main()
