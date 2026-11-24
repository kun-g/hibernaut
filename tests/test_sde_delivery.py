import json
import os
import shutil
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.channels.gdrive_tasks import DriveTaskPoller, parse_task_markdown
from agent_runtime.adapters.delivery import MERGE_TOOL, OPEN_PR_TOOL, DeliveryConfig, DeliveryRecord, PRDelivery
from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.project_guidance import Selection, load_guidance, pin_head
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA, SDEExecutor, SDEOptions, _State
from agent_runtime.adapters.permissions.security import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.core.models import DeliveryOutcome, Emit, Event, PermissionDecision, SessionRef, Suspend
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import fake_command
from tests.test_gdrive_tasks import _FakeDriveClient
from tests.test_pr_delivery import VERSION_FILE, DeliveryCase, git

REPO = Path(__file__).resolve().parents[1]
EVIDENCE = "red: test_zero FAILED\ngreen: 4 passed"
PASS = {"verdict": "pass", "summary": "ok", "blocking": []}


def dev(files, kind="feature", commit="divide rejects zero", **output):
    out = {"status": "done", "summary": "divide rejects zero", "test_evidence": EVIDENCE,
           "change_kind": kind, "change_rationale": f"{kind}: divide now raises on zero"}
    out.update(output)
    return {"write": files, "commit": commit, "output": out}


def event(payload, key="task:1"):
    return Event(inbox_id="proj:x", source="test", external_id=f"e-{os.urandom(4).hex()}", payload=payload,
                 correlation_key=key)


class SDEDeliveryCase(DeliveryCase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.wt = self.clone("task-1", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    def set_plan(self, sde=(), reviewer=()):
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def executor(self, review=True, delivery=None):
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        reviewer = CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=launcher, model="claude-sonnet-5-5"),
            workspaces=self.workspaces, tasks=SQLiteTaskMetadataStore(self.db),
        )
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=launcher),
            options=SDEOptions(review=review),
            reviewer=reviewer if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={},
            tasks=SQLiteTaskMetadataStore(self.db),
            delivery=delivery,
        )

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero divisors",
                "explicit_workdir": str(self.wt), "base_ref": "origin/main", **extra}

    def calls(self, role=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if role is None or r["role"] == role]

    @staticmethod
    def resume(outcome, key="task:1"):
        return SessionRef(session_id=outcome.session_id or "s", inbox_id="proj:x", correlation_key=key,
                          executor_id="sde", executor_session_id=outcome.executor_session_id, status="suspended",
                          checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)

class TestSDEDelivery(SDEDeliveryCase):
    async def test_report_names_the_change_kind_and_the_delivery_step_opens_the_pr(self):
        self.set_plan(sde=[dev({"calc.py": "def divide(a, b):\n    return a / b\n"})], reviewer=[PASS])
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        for part in ("[sde] delivered (review=pass after 1 round", "Change: feature",
                     "feature: divide now raises on zero", "Delivery: pr_open - PR created",
                     "PR: https://github.example/o/r/pull/1", "Version: 0.4.0 -> 0.5.0 (minor for feature)",
                     "Checks: `", "passed", "not merged: this project does not authorize merging"):
            self.assertIn(part, text)
        dev_call = self.calls("sde")[0]
        schema = json.loads(dev_call["argv"][dev_call["argv"].index("--json-schema") + 1])
        self.assertEqual(schema["properties"]["change_kind"]["enum"], ["breaking", "feature", "fix", "test_docs"])
        self.assertIn("## Delivery", dev_call["prompt"])
        self.assertIn(f"sets the version in {VERSION_FILE}", dev_call["prompt"])
        self.assertIn("do not push, open a pull request or merge", dev_call["prompt"])
        self.assertEqual(self.gh_state()["prs"][0]["title"], "Pkg 0.5.0: Reject zero divisors")

    async def test_a_task_from_a_github_issue_links_its_pr_to_the_issue(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        issue = Event(inbox_id="proj:x", source="github:issue_poll", external_id="github-poll:o/r#7:t",
                      correlation_key="github:o/r#7",
                      payload={**self.payload(), "repository": {"full_name": "o/r"},
                               "issue": {"number": 7, "url": "https://github.example/o/r/issues/7"}})
        outcome = await self.executor(review=False, delivery=self.delivery(issue_link="closes")).execute(
            [issue], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertTrue(self.gh_state()["prs"][0]["body"].startswith("Closes #7\n\n## Summary"))

    async def test_a_task_from_another_source_opens_a_pr_without_an_issue_line(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        outcome = await self.executor(review=False, delivery=self.delivery(issue_link="closes")).execute(
            [event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertTrue(self.gh_state()["prs"][0]["body"].startswith("## Summary"))

    async def test_done_without_a_change_kind_is_rejected(self):
        step = dev({"calc.py": "x = 1\n"})
        del step["output"]["change_kind"]
        self.set_plan(sde=[step])
        outcome = await self.executor(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("development failed: the developer reported done without a valid change_kind",
                      outcome.messages[0])

    async def test_without_delivery_the_receipt_still_names_the_change_kind(self):
        self.set_plan(sde=[dev({"test_calc.py": "def test_x():\n    pass\n"}, kind="test_docs")])
        outcome = await self.executor(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Change: test_docs (test_docs: divide now raises on zero)", outcome.messages[0])
        self.assertNotIn("Delivery:", outcome.messages[0])
        self.assertNotIn("## Delivery", self.calls("sde")[0]["prompt"])

    async def test_a_failed_delivery_suspends_and_approval_retries_only_the_delivery(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        state = self.gh_state()
        state["fail"] = {"create": 1}
        self.state_file.write_text(json.dumps(state))
        executor = self.executor(review=False, delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] delivery pending: gh pr create failed", outcome.prompt)
        self.assertIn("Approve to retry the delivery step", outcome.prompt)
        approve = event({"approval_request_id": outcome.request_id, "approved": True})
        resumed = await executor.execute([approve], self.resume(outcome), self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("Delivery: pr_open", resumed.messages[0])
        self.assertEqual(len(self.calls("sde")), 1)
        self.assertEqual(len(self.gh_state()["prs"]), 1)

    async def test_a_follow_up_while_delivery_waits_is_developed_before_delivering_again(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"}), dev({"calc.py": "x = 2\n"}, commit="follow-up")])
        state = self.gh_state()
        state["fail"] = {"create": 1}
        self.state_file.write_text(json.dumps(state))
        executor = self.executor(review=False, delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        follow_up = event({"executor_id": "sde", "text": "also handle negative zero"})
        approve = event({"approval_request_id": outcome.request_id, "approved": True})
        resumed = await executor.execute([follow_up, approve], self.resume(outcome), self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("Delivery: pr_open", resumed.messages[0])
        self.assertIn("also handle negative zero", self.calls("sde")[1]["prompt"])
        self.assertEqual(git(self.bare, "show", "hb/task-1:calc.py"), "x = 2")

    async def test_rejecting_a_delivery_retry_delivers_the_local_state(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        defer = InMemoryPermission({OPEN_PR_TOOL: PermissionDecision("defer", "a person opens PRs")})
        self.caps.permissions = defer
        executor = self.executor(review=False, delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("delivery.open_pr needs approval: a person opens PRs", outcome.prompt)
        rejected = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": False})], self.resume(outcome), self.caps)
        assert isinstance(rejected, Emit), rejected
        self.assertIn("[sde] delivered (review=off", rejected.messages[0])
        self.assertIn("Delivery: local", rejected.messages[0])
        self.assertIn("delivery declined: delivery.open_pr needs approval", rejected.messages[0])
        self.assertEqual(self.gh_state().get("prs", []), [])

    async def test_approving_a_deferred_merge_merges_with_the_recorded_sha(self):
        (self.wt / "calc.py").write_text("x = 1\n")
        git(self.wt, "commit", "-qam", "fix divide")
        state = _State(stage="develop", workdir=str(self.wt), base_ref="origin/main", requirements="fix divide",
                       change_kind="fix", rounds=1, work_key="task:1", inbox_id="proj:x")
        self.caps.permissions = InMemoryPermission({MERGE_TOOL: PermissionDecision("defer", "merge needs a person")})
        executor = self.executor(delivery=self.delivery(auto_merge=True))
        outcome = await executor._finish("session", "native", state, self.caps, review="pass")
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("delivery.merge needs approval", outcome.prompt)
        self.assertIn("PR: https://github.example/o/r/pull/1", outcome.prompt)
        approve = event({"approval_request_id": outcome.request_id, "approved": True})
        merged = await executor.execute([approve], self.resume(outcome), self.caps)
        assert isinstance(merged, Emit), merged
        main_sha = git(self.bare, "rev-parse", "main")
        self.assertIn(f"Merge commit: {main_sha}", merged.messages[0])
        self.assertIn("Version: 0.4.0 -> 0.4.1 (patch for fix)", merged.messages[0])
        self.assertIn('"0.4.1"', self.main_file())

    async def test_drive_task_body_and_title_reach_the_developer(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        payload = {"executor_id": "sde", "title": "Reject zero", "body": "divide must raise on zero",
                   "explicit_workdir": str(self.wt)}
        outcome = await self.executor(review=False).execute([event(payload)], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("# Reject zero\n\ndivide must raise on zero", self.calls("sde")[0]["prompt"])


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


MERGE_MAIN = [["git", "merge", "--no-edit", "origin/main"]]
TASK_CALC = "def divide(a, b):\n    return a / b\n"
MAIN_CALC = "def divide(a, b):\n    return b\n"
RESOLVED = "def divide(a, b):\n    if b == 0:\n        raise ZeroDivisionError\n    return a / b\n"


class TestDeliveryHandBack(SDEDeliveryCase):
    # Merging origin/main during delivery conflicts, or the checks fail after it: the same developer session fixes it.

    async def test_a_base_conflict_goes_back_to_the_developer_then_review_and_delivery_continue(self):
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}),
                           {**dev({"calc.py": RESOLVED}, commit="merge origin/main"), "run": MERGE_MAIN}],
                      reviewer=[PASS, PASS])
        self.push_main({"calc.py": MAIN_CALC}, message="divide returns b")
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        self.assertIn("Delivery: pr_open - PR created", text)
        self.assertIn("Delivery hand-back 1 of 2: merging origin/main conflicts in calc.py; handed back to the "
                      "developer", text)
        sde_calls = self.calls("sde")
        self.assertEqual(len(sde_calls), 2)
        handback = sde_calls[1]["prompt"]
        main_sha = git(self.bare, "rev-parse", "main")
        for part in ("## Delivery handed back: merging origin/main conflicts", "- calc.py", main_sha,
                     "divide returns b", "change_kind"):
            self.assertIn(part, handback)
        reviews = self.calls("reviewer")
        self.assertEqual(len(reviews), 2)
        # The second review sees only what changed since the passed review, the conflict resolution included.
        self.assertIn("## Why this is reviewed again", reviews[1]["prompt"])
        self.assertIn("raise ZeroDivisionError", reviews[1]["prompt"].split("## Changes since the passed review")[1])
        branch_calc = git(self.bare, "show", "hb/task-1:calc.py")
        self.assertEqual(branch_calc, RESOLVED.strip())
        self.assertEqual(git(self.wt, "merge-base", "--is-ancestor", main_sha, "HEAD"), "")

    async def test_failing_checks_after_merging_the_base_go_back_to_the_developer(self):
        check = ("/bin/sh", "-c", "test ! -f main.txt || test -f ok.txt || { echo 'main.txt needs ok.txt'; exit 1; }")
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"}), dev({"ok.txt": "ok\n"}, commit="satisfy main's check")])
        self.push_main({"main.txt": "new on main\n"})
        executor = self.executor(review=False, delivery=self.delivery(checks=(check,)))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Delivery: pr_open - PR created", outcome.messages[0])
        self.assertIn("Delivery hand-back 1 of 2: repository checks failed", outcome.messages[0])
        handback = self.calls("sde")[1]["prompt"]
        self.assertIn("## Delivery handed back: repository checks failed", handback)
        self.assertIn("main.txt needs ok.txt", handback)
        self.assertEqual(git(self.bare, "show", "hb/task-1:ok.txt"), "ok")

    async def test_a_conflict_only_in_the_version_file_is_still_resolved_by_delivery(self):
        self.set_plan(sde=[dev({VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "calc.py": "y = 2\n"},
                               kind="fix")], reviewer=[PASS])
        self.push_main({VERSION_FILE: '"""pkg"""\n__version__ = "0.5.0"\n'})
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Version: 0.5.0 -> 0.5.1 (patch for fix)", outcome.messages[0])
        self.assertNotIn("hand-back", outcome.messages[0])
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (1, 1))


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

    def test_repository_guidance_selects_versioning_by_activity_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            for rel in ("AGENTS.md", "guidance.toml", "docs/process/versioning.md", "docs/inbox-conventions.md"):
                (repo / rel).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(REPO / rel, repo / rel)
            git(repo, "init", "-q", "-b", "main")
            git(repo, "-c", "user.email=t@example.invalid", "-c", "user.name=t", "add", "-A")
            git(repo, "-c", "user.email=t@example.invalid", "-c", "user.name=t", "commit", "-qm", "guidance")
            guidance = load_guidance(pin_head(repo, origin="test"))
        versioning = "docs/process/versioning.md"
        pick = lambda *acts: [s.doc.path for s in guidance.select(Selection(acts, ()))]  # noqa: E731
        self.assertIn(versioning, pick("versioning"))
        self.assertIn(versioning, pick("release"))
        self.assertNotIn(versioning, pick("testing"))
        self.assertNotIn(versioning, pick("development"))
        self.assertIn("change_kind", SDE_SCHEMA["required"])


if __name__ == "__main__":
    unittest.main()
