import json
import shutil
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery import MERGE_TOOL, OPEN_PR_TOOL
from agent_runtime.adapters.executor_cli.project_guidance import Selection, load_guidance, pin_head
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA, _State
from agent_runtime.core.models import Emit, Event, PermissionDecision, Suspend
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import VERSION_FILE, git
from tests.support.sde_delivery import (
    MAIN_CALC,
    MERGE_MAIN,
    PASS,
    REPO,
    RESOLVED,
    SDEDeliveryCase,
    TASK_CALC,
    dev,
    event,
)


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
