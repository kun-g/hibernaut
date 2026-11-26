import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from agent_runtime.adapters.delivery import MERGE_TOOL, OPEN_PR_TOOL, DeliveryRecord
from agent_runtime.core.models import PermissionDecision
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import DeliveryCase, VERSION_FILE, git


class TestOpenPullRequest(DeliveryCase):
    def test_merges_latest_main_sets_version_runs_checks_pushes_and_creates_the_pr(self):
        wt = self.task("task-a", {"calc.py": "def divide(a, b):\n    return a / b\n"})
        self.push_main({"README.md": "docs\n"})
        rec = self.delivery(launch_gh=True).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.problem, "")
        self.assertEqual(rec.state, "pr_open")
        self.assertEqual((rec.base_version, rec.bump, rec.version), ("0.4.0", "minor", "0.5.0"))
        self.assertEqual(rec.pr_url, "https://github.example/o/r/pull/1")
        self.assertEqual(self.remote_head("hb/task-a"), rec.head_sha)
        self.assertEqual(rec.head_sha, git(wt, "rev-parse", "HEAD"))
        # The latest main is part of the branch, and the version is set on top of it.
        self.assertEqual(git(wt, "merge-base", "--is-ancestor", git(self.bare, "rev-parse", "main"), "HEAD"), "")
        self.assertIn('__version__ = "0.5.0"', git(self.bare, "show", f"hb/task-a:{VERSION_FILE}"))
        self.assertTrue((wt / "README.md").exists())
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertEqual([c["exit_code"] for c in rec.checks], [0])
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["title"], "Pkg 0.5.0: divide rejects zero")
        for part in ("Kind: feature", "adds a new rejection path", "0.4.0 -> 0.5.0 (minor for feature)",
                     "passed", "review=pass", "red: 1 failed", "task:task-a"):
            self.assertIn(part, pr["body"])
        self.assertEqual(self.gh_state()["repo_flag"], "o/r")
        # Remote calls never see a token from the daemon environment.
        self.assertEqual(self.gh_state()["tokens_seen"], [])
        self.assertIn("not merged: this project does not authorize merging", "\n".join(rec.render()))
        self.assertEqual(self.store.get("task:task-a")["state"], "pr_open")

    def test_retry_reuses_the_pr_and_repeats_no_completed_step(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        first = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        second = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((second.state, second.pr_number, second.version), ("pr_open", first.pr_number, "0.5.0"))
        self.assertEqual(second.head_sha, first.head_sha)
        self.assertEqual(len(self.gh_calls("create")), 1)
        self.assertEqual(len(self.gh_state()["prs"]), 1)
        self.assertEqual(self.gh_state()["prs"][0]["edits"], 1)
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertEqual(second.attempts, 2)

    def test_a_reused_pr_keeps_the_issue_line_when_its_body_is_updated(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.delivery().deliver(self.request(wt, issue="o/r#7"), permissions=InMemoryPermission())
        self.delivery(issue_link="closes").deliver(self.request(wt, issue="o/r#7"), permissions=InMemoryPermission())
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["edits"], 1)
        self.assertTrue(pr["body"].startswith("Closes #7\n\n## Summary"), pr["body"][:80])

    def test_partial_success_push_done_pr_failed_is_resumed(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        state = self.gh_state()
        state["fail"] = {"create": 1}
        self.state_file.write_text(json.dumps(state))
        failed = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(failed.state, "local")
        self.assertIn("gh pr create failed", failed.problem)
        self.assertEqual(failed.pushed_sha, self.remote_head("hb/task-a"))
        retried = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((retried.state, retried.problem), ("pr_open", ""))
        self.assertEqual(retried.head_sha, failed.head_sha)
        self.assertEqual(len(self.gh_state()["prs"]), 1)

    def test_failing_checks_push_nothing_and_are_recoverable(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(checks=((sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"),)).deliver(
            self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "local")
        self.assertIn("repository checks failed", rec.problem)
        self.assertIn("boom", rec.problem)
        self.assertEqual(self.remote_head("hb/task-a"), "")
        self.assertEqual(self.gh_calls("create"), [])

    def test_test_and_docs_changes_skip_the_version(self):
        wt = self.task("task-a", {"test_calc.py": "def test_x():\n    pass\n"})
        rec = self.delivery().deliver(self.request(wt, kind="test_docs"), permissions=InMemoryPermission())
        self.assertEqual((rec.bump, rec.base_version, rec.version), ("skip", "0.4.0", "0.4.0"))
        self.assertEqual(self.gh_state()["prs"][0]["title"], "divide rejects zero")
        self.assertEqual(git(wt, "log", "--format=%s", "-1"), "change")
        self.assertIn("unchanged 0.4.0 (skip for test_docs)", "\n".join(rec.render()))

    def test_breaking_change_bumps_major_and_a_branch_own_bump_is_replaced(self):
        wt = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "calc.py": "y = 2\n"})
        rec = self.delivery().deliver(self.request(wt, kind="breaking"), permissions=InMemoryPermission())
        self.assertEqual(rec.version, "1.0.0")
        self.assertIn('"1.0.0"', git(self.bare, "show", f"hb/task-a:{VERSION_FILE}"))

    def test_without_open_pr_grant_the_change_stays_local(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(open_pr=False).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem, rec.bump), ("local", "", "minor"))
        self.assertIn("delivery.open_pr = false", rec.notes[0])
        self.assertEqual(self.remote_head("hb/task-a"), "")
        self.assertEqual(self.gh_state().get("calls", []), [])

    def test_open_pr_permission_defer_waits_for_approval(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        defer = InMemoryPermission({OPEN_PR_TOOL: PermissionDecision("defer", "publishing needs a person")})
        rec = self.delivery().deliver(self.request(wt), permissions=defer)
        self.assertEqual((rec.state, rec.approval), ("local", OPEN_PR_TOOL))
        self.assertIn("publishing needs a person", rec.problem)
        self.assertEqual(self.remote_head("hb/task-a"), "")
        rec = self.delivery().deliver(self.request(wt), permissions=defer, approved=frozenset({OPEN_PR_TOOL}))
        self.assertEqual(rec.state, "pr_open")
        blocked = InMemoryPermission({OPEN_PR_TOOL: PermissionDecision("block", "no publishing")})
        other = self.task("task-b", {"calc.py": "z = 1\n"})
        rec = self.delivery().deliver(self.request(other), permissions=blocked)
        self.assertEqual((rec.state, rec.problem, rec.notes), ("local", "", ["delivery.open_pr blocked: no publishing"]))

    def test_conflict_outside_the_version_file_is_reported_and_aborted(self):
        wt = self.task("task-a", {"calc.py": "def divide(a, b):\n    return a / b\n"})
        self.push_main({"calc.py": "def divide(a, b):\n    return b\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertIn("conflicts in calc.py", rec.problem)
        self.assertEqual(git(wt, "status", "--porcelain"), "")
        self.assertFalse((wt / ".git" / "MERGE_HEAD").exists())
        self.assertEqual(self.remote_head("hb/task-a"), "")
        # The developer can resolve it: the record says what conflicted and which base commits caused it.
        main_sha = git(self.bare, "rev-parse", "main")
        self.assertEqual(rec.handback, {
            "kind": "conflict", "base": "origin/main", "base_sha": main_sha, "branch": "hb/task-a",
            "files": ["calc.py"], "commits": f"{main_sha[:7]} main moves",
        })

    def test_a_conflict_only_in_the_version_file_is_resolved_without_a_hand_back(self):
        wt = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "calc.py": "y = 2\n"})
        self.push_main({VERSION_FILE: '"""pkg"""\n__version__ = "0.5.0"\n'})
        rec = self.delivery().deliver(self.request(wt, kind="fix"), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem, rec.handback, rec.version), ("pr_open", "", {}, "0.5.1"))

    def test_failing_checks_after_merging_the_base_are_handed_back(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.push_main({"main.txt": "new on main\n"})
        check = ("/bin/sh", "-c", "test ! -f main.txt || { echo 'main.txt needs ok.txt'; exit 1; }")
        rec = self.delivery(checks=(check,)).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertIn("repository checks failed", rec.problem)
        self.assertEqual(rec.handback["kind"], "checks")
        self.assertEqual(rec.handback["base_sha"], git(self.bare, "rev-parse", "main"))
        self.assertEqual(rec.handback["head"], git(wt, "rev-parse", "HEAD"))
        self.assertIn("main.txt needs ok.txt", rec.handback["failed"])
        self.assertEqual(self.remote_head("hb/task-a"), "")

    def test_a_missing_check_command_is_not_handed_back(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(checks=(("no-such-check-command",),)).deliver(self.request(wt),
                                                                       permissions=InMemoryPermission())
        self.assertIn("command not found", rec.problem)
        self.assertEqual(rec.handback, {})


class TestMerge(DeliveryCase):
    def merging(self, **overrides):
        return self.delivery(auto_merge=True, **overrides)

    def set_gh(self, **values):
        state = self.gh_state()
        state.update(values)
        self.state_file.write_text(json.dumps(state))

    def test_merge_waits_for_ci_and_records_the_merge_sha_and_version(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        self.set_gh(checks_sequence=[[], pending])
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem), ("merged", ""))
        self.assertEqual(rec.merge_sha, git(self.bare, "rev-parse", "main"))
        self.assertIn('"0.5.0"', self.main_file())
        self.assertEqual(len(self.sleeps), 2)
        merge_call = self.gh_calls("merge")[0]
        self.assertIn("--squash", merge_call)
        self.assertEqual(merge_call[merge_call.index("--match-head-commit") + 1], rec.head_sha)
        text = "\n".join(rec.render())
        for part in ("Delivery: merged", f"Merge commit: {rec.merge_sha}", "0.4.0 -> 0.5.0", "CI: passed"):
            self.assertIn(part, text)
        again = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((again.state, again.merge_sha), ("merged", rec.merge_sha))
        self.assertEqual(len(self.gh_calls("merge")), 1)

    def test_merge_is_a_separate_grant(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        request = self.request(wt)
        opened = self.delivery(auto_merge=False).deliver(request, permissions=InMemoryPermission())
        self.assertEqual(opened.state, "pr_open")
        self.assertEqual(self.gh_calls("merge"), [])
        delivery = self.merging()
        defer = InMemoryPermission({MERGE_TOOL: PermissionDecision("defer", "merge needs a person")})
        rec = delivery.record(request.work_key)
        delivery._merge(request, rec, defer, frozenset(), lambda: False)
        self.assertEqual((rec.state, rec.approval), ("pr_open", MERGE_TOOL))
        self.assertEqual(self.gh_calls("merge"), [])
        rec = delivery.record(request.work_key)
        delivery._merge(request, rec, defer, frozenset({MERGE_TOOL}), lambda: False)
        self.assertEqual(rec.state, "merged")
        (merge,) = self.gh_calls("merge")
        self.assertEqual(merge[merge.index("--match-head-commit") + 1], opened.head_sha)

    def test_ci_timeout_is_recoverable(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.set_gh(checks=[{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}])
        rec = self.merging(ci_timeout_seconds=0.01, ci_poll_seconds=0.01).deliver(
            self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "pr_open")
        self.assertIn("CI has not finished for PR #1", rec.problem)
        self.set_gh(checks=[{"name": "gates", "state": "SUCCESS", "bucket": "pass"}])
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "merged")

    def test_concurrent_prs_from_the_same_base_end_with_distinct_versions(self):
        a = self.task("task-a", {"calc.py": "a = 1\n"})
        b = self.task("task-b", {"other.py": "b = 1\n"})
        qa, qb = self.request(a, kind="feature"), self.request(b, kind="fix")
        permissions = InMemoryPermission()
        ra = self.delivery().deliver(qa, permissions=permissions)
        rb = self.delivery().deliver(qb, permissions=permissions)
        self.assertEqual((ra.state, rb.state, ra.version, rb.version), ("pr_open", "pr_open", "0.5.0", "0.4.1"))
        number = rb.pr_number
        self.merging()._merge(qa, ra, permissions, frozenset(), lambda: False)
        self.assertEqual(ra.state, "merged")
        self.merging()._merge(qb, rb, permissions, frozenset(), lambda: False)
        self.assertEqual((rb.state, rb.problem), ("merged", ""))
        self.assertEqual((rb.base_version, rb.version, rb.pr_number), ("0.5.0", "0.5.1", number))
        self.assertIn("version recomputed from 0.5.0", "\n".join(rb.history))
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(self.main_file("other.py"), "b = 1")
        self.assertEqual(self.gh_state()["prs"][1]["title"], "Pkg 0.5.1: divide rejects zero")

    def waiting_for_ci(self):
        a = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.5.0"\n', "calc.py": "a = 1\n"})
        b = self.task("task-b", {VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "other.py": "b = 1\n"})
        gh = self.delivery()._gh

        def open_pr(wt, branch):
            git(wt, "push", "-q", "origin", branch)
            return gh(wt, "pr", "create", "--base", "main", "--head", branch, "--title", "Release", "--body", "").stdout.strip()

        a_number = open_pr(a, "hb/task-a").rsplit("/", 1)[1]
        a_head = git(a, "rev-parse", "HEAD")
        b_url = open_pr(b, "hb/task-b")
        head = git(b, "rev-parse", "HEAD")
        record = DeliveryRecord(state="pr_open", branch="hb/task-b", change_kind="fix", bump="patch", version="0.4.1",
                                base_sha=git(b, "rev-parse", "origin/main"), base_version="0.4.0", pr_url=b_url,
                                pr_number=int(b_url.rsplit("/", 1)[1]), head_sha=head, checked_sha=head,
                                pushed_sha=head, verified_sha=head)

        def merge_a():
            gh(a, "pr", "merge", a_number, "--squash", "--match-head-commit", a_head)

        return self.request(b, kind="fix"), record, merge_a

    def test_main_version_moving_between_ci_and_merge_recomputes_before_merging(self):
        request, rb, merge_a = self.waiting_for_ci()
        seen_heads = []

        def passed(req, record, cwd, is_interrupted):
            seen_heads.append(record.head_sha)
            if len(seen_heads) == 1:
                merge_a()  # The old head passed CI just before the base changed.
            return "passed"

        delivery = self.merging()
        with patch.object(delivery, "_wait_for_ci", side_effect=passed):
            delivery._merge(request, rb, InMemoryPermission(), frozenset(), lambda: False)
        self.assertEqual(len(seen_heads), 2)
        self.assertNotEqual(seen_heads[0], seen_heads[1])
        self.assertEqual(seen_heads[1], rb.head_sha)
        self.assertEqual(self.gh_state()["prs"][0]["state"], "MERGED")
        self.assertEqual((rb.state, rb.problem, rb.version), ("merged", "", "0.5.1"))
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(rb.merge_sha, git(self.bare, "rev-parse", "main"))
        self.assertFalse(any("while waiting for CI" in item for item in rb.history), rb.history)

    def test_a_pr_that_conflicts_while_waiting_for_ci_is_prepared_again_without_waiting_for_the_timeout(self):
        request, rb, merge_a = self.waiting_for_ci()

        def merge_a_while_b_waits(seconds):
            self.sleeps.append(seconds)
            if len(self.sleeps) == 1:
                merge_a()

        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        # Both PRs set the version: once a merges, b conflicts and GitHub reports no checks for it.
        self.set_gh(checks_sequence=[pending], dirty_blocks_ci=True)
        delivery = self.merging(sleep=merge_a_while_b_waits, ci_timeout_seconds=600)
        delivery._merge(request, rb, InMemoryPermission(), frozenset(), lambda: False)
        self.assertEqual(self.gh_state()["prs"][0]["state"], "MERGED")
        self.assertEqual((rb.state, rb.problem, rb.version), ("merged", "", "0.5.1"))
        self.assertEqual(len(self.sleeps), 1)
        self.assertIn("PR #2 conflicts with origin/main while waiting for CI; merging origin/main again",
                      rb.history)
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(self.main_file("other.py"), "b = 1")

    def test_a_pr_merged_by_a_person_is_adopted_not_bumped_again(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        subprocess.run([str(self.root / "gh"), "pr", "merge", "1", "--squash", "--match-head-commit", rec.head_sha],
                       env=self.env, check=True, capture_output=True)
        again = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(again.state, "merged")
        self.assertEqual(again.merge_sha, git(self.bare, "rev-parse", "main"))
        self.assertEqual(git(wt, "rev-parse", "HEAD"), rec.head_sha)
        self.assertIn('"0.5.0"', self.main_file())
        self.assertEqual(len(self.gh_calls("create")), 1)


if __name__ == "__main__":
    unittest.main()
