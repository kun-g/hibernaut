import json
import tomllib
import unittest
from unittest.mock import Mock, patch

from agent_runtime.adapters.channels.gdrive_tasks import DriveTaskPoller, parse_task_markdown
from agent_runtime.adapters.delivery import MERGE_TOOL, OPEN_PR_TOOL, DeliveryConfig, DeliveryRecord, PRDelivery
from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions, _State
from agent_runtime.adapters.permissions.security import AutoModePermissionAdapter
from agent_runtime.app.config import parse_config
from agent_runtime.core.models import DeliveryOutcome, Emit
from tests.support.fakes import InMemoryPermission
from tests.support.gdrive_tasks import _FakeDriveClient
from tests.support.pr_delivery import VERSION_FILE
from tests.support.sde_delivery import REPO, event


class TestSDEDeliveryResult(unittest.IsolatedAsyncioTestCase):
    url = "https://github.example/o/r/pull/1"

    async def finish(self, record=None, *, auto_merge=False, open_pr=True):
        delivery = None
        if record is not None:
            delivery = PRDelivery(DeliveryConfig(open_pr=open_pr, auto_merge=auto_merge), Mock(), env={})
            self.enterContext(patch.object(delivery, "deliver", return_value=record))
        executor = SDEExecutor(profile=ClaudeCodeProfile(name="sde"), options=SDEOptions(review=False), reviewer=None,
                               worktree_manager=Mock(spec=OrcaWorktreeManager), inbox_checkout_map={}, tasks=Mock(),
                               delivery=delivery)
        state = _State(stage="develop", requirements="divide", base_ref="main", workdir="/test/repo", change_kind="fix")
        return await executor._finish("session", "native", state, Mock(permissions=InMemoryPermission()), review="pass")

    async def test_a_merged_delivery_closes_the_session(self):
        record = DeliveryRecord(state="merged", pr_url=self.url, history=["deleted origin/hb/task-1"])
        outcome = await self.finish(record, auto_merge=True)
        self.assertIsInstance(outcome, Emit)
        self.assertTrue(outcome.close_session)
        for part in ("Delivery: merged", "deleted origin/hb/task-1", "Worktree: the PR is merged"):
            self.assertIn(part, outcome.messages[0])

    async def test_an_open_pr_keeps_the_session(self):
        outcome = await self.finish(DeliveryRecord(state="pr_open", pr_url=self.url))
        self.assertIsInstance(outcome, Emit)
        self.assertFalse(outcome.close_session)
        self.assertNotIn("Worktree: the PR is merged", outcome.messages[0])

    async def test_the_result_carries_the_delivery_outcome_for_the_outbox(self):
        record = DeliveryRecord(state="pr_open", pr_url=self.url, merge_hold="ci", merge_hold_detail="CI failed (gates)")
        outcome = await self.finish(record, auto_merge=True)
        self.assertIsInstance(outcome, Emit)
        self.assertEqual((outcome.status, outcome.close_session), ("done", False))
        self.assertEqual(outcome.delivery, DeliveryOutcome(state="pr_open", pr_url=self.url, auto_merge=True,
                                                           hold="ci", hold_detail="CI failed (gates)"))

    async def test_a_merged_result_carries_a_merged_outcome(self):
        outcome = await self.finish(DeliveryRecord(state="merged", pr_url=self.url), auto_merge=True)
        self.assertIsInstance(outcome, Emit)
        self.assertEqual(outcome.delivery, DeliveryOutcome(state="merged", pr_url=self.url, auto_merge=True))

    async def test_without_delivery_the_result_carries_no_outcome(self):
        outcome = await self.finish()
        self.assertIsInstance(outcome, Emit)
        self.assertIsNone(outcome.delivery)

    async def test_local_delivery_keeps_the_session(self):
        outcome = await self.finish(DeliveryRecord(), open_pr=False)
        self.assertIsInstance(outcome, Emit)
        self.assertFalse(outcome.close_session)


class TestProjectAdoption(unittest.TestCase):
    def test_project_delivery_config_is_parsed_from_the_trusted_deployment_file(self):
        cfg = parse_config({
            "executors": {"sde": {"role": "sde", "review": False}},
            "projects": {"hb": {"repo": "o/r", "checkout": "/tmp/x",
                                "delivery": {"open_pr": True, "version_file": VERSION_FILE}}},
        })
        delivery = cfg.projects["hb"].delivery
        assert delivery is not None
        self.assertEqual((delivery.repo, delivery.open_pr, delivery.auto_merge), ("o/r", True, False))
        self.assertIsNone(parse_config({"projects": {"p": {"repo": "o/r", "checkout": "/x"}}}).projects["p"].delivery)
        with self.assertRaisesRegex(ValueError, "projects.hb.delivery: auto_merge needs open_pr = true"):
            parse_config({"projects": {"hb": {"repo": "o/r", "checkout": "/x", "delivery": {"auto_merge": True}}}})
        self.assertEqual(delivery.issue_link, "refs")
        closes = parse_config({"projects": {"hb": {"repo": "o/r", "checkout": "/x",
                                                   "delivery": {"issue_link": "closes"}}}}).projects["hb"].delivery
        assert closes is not None
        self.assertEqual(closes.issue_link, "closes")
        with self.assertRaisesRegex(ValueError, "projects.hb.delivery: issue_link must be one of closes, refs, none"):
            parse_config({"projects": {"hb": {"repo": "o/r", "checkout": "/x", "delivery": {"issue_link": "fix"}}}})

    def test_example_config_adopts_pr_created_as_the_default_end_point_for_this_project(self):
        raw = tomllib.loads((REPO / "hibernaut.example.toml").read_text(encoding="utf-8"))
        cfg = parse_config(raw, base_dir=REPO)
        delivery = cfg.projects["hibernaut"].delivery
        assert delivery is not None
        self.assertTrue(delivery.open_pr)
        self.assertFalse(delivery.auto_merge)
        self.assertEqual(delivery.version_file, "src/agent_runtime/__init__.py")
        pytest_gate = ("pytest", "-n", "auto", "--dist", "worksteal", "-v")
        self.assertEqual(delivery.checks, (("lint-imports",), pytest_gate, ("pyright",)))
        version = (REPO / delivery.version_file).read_text(encoding="utf-8")
        PRDelivery(delivery, Mock())._read_version(version, "check")  # the pattern matches the real file

    def test_delivery_tools_are_separate_permission_names_allowed_by_auto_mode(self):
        auto = AutoModePermissionAdapter()
        self.assertEqual(auto.check(OPEN_PR_TOOL, {}).action, "allow")
        self.assertEqual(auto.check(MERGE_TOOL, {}).action, "allow")
        self.assertNotEqual(OPEN_PR_TOOL, MERGE_TOOL)

    def test_development_dispatch_templates_route_to_sde_and_reviews_keep_their_route(self):
        drive_meta, body = parse_task_markdown((REPO / "docs/dispatch/sde-task.drive.md").read_text(encoding="utf-8"))
        self.assertEqual(drive_meta["executor_id"], "sde")
        self.assertTrue(body)
        sde_event = json.loads((REPO / "docs/dispatch/sde-task.json").read_text(encoding="utf-8"))
        self.assertEqual(sde_event["payload"]["executor_id"], "sde")
        review_event = json.loads((REPO / "docs/dispatch/review-task.json").read_text(encoding="utf-8"))
        self.assertEqual(review_event["payload"]["executor_id"], "code-reviewer")
        readme = (REPO / "README.md").read_text(encoding="utf-8")
        self.assertIn("executor_id: sde", readme)

    def test_dispatch_templates_name_an_agent_their_inbox_allows_in_the_example_config(self):
        cfg = parse_config(tomllib.loads((REPO / "hibernaut.example.toml").read_text(encoding="utf-8")), base_dir=REPO)
        drive_meta, _ = parse_task_markdown((REPO / "docs/dispatch/sde-task.drive.md").read_text(encoding="utf-8"))
        targets = [(drive_meta["scope"], drive_meta["executor_id"])]
        for name in ("sde-task.json", "review-task.json"):
            event = json.loads((REPO / "docs/dispatch" / name).read_text(encoding="utf-8"))
            targets.append((event["inbox_id"], event["payload"]["executor_id"]))
        for inbox_id, agent in targets:
            with self.subTest(inbox_id=inbox_id, agent=agent):
                binding = cfg.projects["hibernaut"].route(inbox_id).binding
                assert binding is not None
                self.assertTrue(binding.allows(agent))
        # A review finds the developer's worktree by key inside its own inbox.
        self.assertEqual(len({inbox_id for inbox_id, _ in targets}), 1)

    def test_drive_template_routes_to_sde_with_its_activities(self):
        drive = _FakeDriveClient()
        drive.add_file("f1", "sde-task.md", (REPO / "docs/dispatch/sde-task.drive.md").read_text(encoding="utf-8"))
        handler = Mock()
        handler.has_open_event.return_value = False
        handler.ingest.return_value = Mock(duplicate=False)
        DriveTaskPoller(handler, client=drive, default_inbox_id="proj:hibernaut", root_folder_id="root-1").poll_once()
        payload = handler.ingest.call_args.kwargs["payload"]
        self.assertEqual(payload["executor_id"], "sde")
        self.assertEqual(payload["activities"], "development")
        self.assertIn("## Acceptance", payload["body"])


if __name__ == "__main__":
    unittest.main()
