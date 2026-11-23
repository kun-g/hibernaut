import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.base_merge import only_conflicts_resolved, without_base
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, worktree_fingerprint
from agent_runtime.adapters.executor_cli.sde import _State
from agent_runtime.core.models import Emit, Suspend
from tests.test_coder_reviewer import git as run_git
from tests.test_incremental_review import FIRST, FIX, block, verdict
from tests.test_pr_delivery import VERSION_FILE
from tests.test_sde_delivery import MAIN_CALC, MERGE_MAIN, PASS, RESOLVED, TASK_CALC, SDEDeliveryCase, dev, event

OTHER = {"main_only.py": "def from_main():\n    return 1\n"}


def out(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class TestConflictResolution(unittest.TestCase):
    BASE = "a = 1\nb = 1\nc = 1\nd = 1\ne = 1\nf = 1\ng = 1\n"
    OURS = BASE.replace("b = 1", "b = old(x)").replace("f = 1", "f = old(y)")
    THEIRS = BASE.replace("b = 1", "b = new(1)").replace("f = 1", "f = new(1)")
    MERGED = BASE.replace("b = 1", "b = new(x)").replace("f = 1", "f = new(y)")

    def setUp(self):
        self.repo = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        run_git(self.repo, "init", "-q", "-b", "main")
        run_git(self.repo, "config", "user.email", "t@example.invalid")
        run_git(self.repo, "config", "user.name", "t")
        self.commit({"f.py": self.BASE, "keep.py": "k = 1\n"}, "base")
        run_git(self.repo, "checkout", "-qb", "task")

    def commit(self, files, message="work"):
        for rel, text in files.items():
            (self.repo / rel).write_text(text)
        run_git(self.repo, "add", "-A")
        run_git(self.repo, "commit", "-q", "--allow-empty", "-m", message)

    def on_main(self, files):
        run_git(self.repo, "checkout", "-q", "main")
        self.commit(files, "main moves")
        run_git(self.repo, "checkout", "-q", "task")

    def resolve(self, files, *, ours=None, theirs=None, remove=()):
        # Reviewed on the task branch, then main moves, then the developer merges it and leaves `files`.
        self.commit(ours or {"f.py": self.OURS}, "task")
        self.reviewed = worktree_fingerprint(self.repo)
        self.on_main({**(theirs or {"f.py": self.THEIRS}), "main_only.py": "m = 1\n"})
        subprocess.run(["git", "merge", "--no-edit", "main"], cwd=self.repo, capture_output=True)
        for rel in remove:
            (self.repo / rel).unlink()
        self.commit(files, "merge main")
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, self.reviewed, current, "main")
        return only_conflicts_resolved(self.repo, since, current), since, current

    def test_resolutions_made_of_both_sides_are_only_resolutions(self):
        for name, text in {"combined": self.MERGED, "ours": self.OURS, "theirs": self.THEIRS}.items():
            with self.subTest(name):
                self.setUp()
                self.assertTrue(self.resolve({"f.py": text})[0])

    def test_any_other_change_is_not(self):
        cases = {
            "outside the conflict blocks": {"f.py": self.MERGED.replace("d = 1", "d = 2")},
            "before the first block": {"f.py": "z = 1\n" + self.MERGED},
            "after the last block": {"f.py": self.MERGED + "z = 1\n"},
            "another file": {"f.py": self.MERGED, "keep.py": "k = 2\n"},
            "a new file": {"f.py": self.MERGED, "extra.py": "n = 1\n"},
            "a file main brought in": {"f.py": self.MERGED, "main_only.py": "m = 2\n"},
            "new names inside a block": {"f.py": self.MERGED.replace("new(x)", "new(guard(x))")},
            "a new operator inside a block": {"f.py": self.MERGED.replace("new(x)", "-new(x)")},
            "conflict markers left in": {},
        }
        for name, files in cases.items():
            with self.subTest(name):
                self.setUp()
                self.assertFalse(self.resolve(files)[0])

    def test_conflicts_without_blocks_are_not_judged(self):
        # main deleted a file the task changed: restoring the task's copy changes nothing, removing it is a decision.
        for remove, expected in (((), True), (("keep.py",), False)):
            with self.subTest(remove=remove):
                self.setUp()
                self.commit({"keep.py": "k = 3\n"}, "task")
                reviewed = worktree_fingerprint(self.repo)
                run_git(self.repo, "checkout", "-q", "main")
                run_git(self.repo, "rm", "-q", "keep.py")
                run_git(self.repo, "commit", "-qm", "drop keep")
                run_git(self.repo, "checkout", "-q", "task")
                subprocess.run(["git", "merge", "--no-edit", "main"], cwd=self.repo, capture_output=True)
                for rel in remove:
                    (self.repo / rel).unlink()
                self.commit({}, "merge main")
                current = worktree_fingerprint(self.repo)
                since = without_base(self.repo, reviewed, current, "main")
                self.assertEqual(only_conflicts_resolved(self.repo, since, current), expected)

    def test_nothing_merged_is_not_a_resolution(self):
        self.commit({"f.py": self.OURS})
        reviewed = worktree_fingerprint(self.repo)
        self.commit({"f.py": self.MERGED})
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, reviewed, current, "main")
        self.assertEqual((since.tree, since.merged), (reviewed.split(":")[1], ""))
        self.assertFalse(only_conflicts_resolved(self.repo, since, current))

    def test_the_base_branchs_own_changes_are_left_out(self):
        _, since, current = self.resolve({"f.py": self.MERGED, "keep.py": "k = 2\n"})
        names = out(self.repo, "diff", "--name-only", since.tree, current.split(":")[1]).split()
        self.assertEqual(names, ["f.py", "keep.py"])

    def test_uncommitted_work_at_the_review_counts_as_reviewed(self):
        (self.repo / "keep.py").write_text("k = 9\n")
        reviewed = worktree_fingerprint(self.repo)
        self.on_main_after_commit()
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, reviewed, current, "main")
        self.assertEqual(out(self.repo, "diff", "--name-only", since.tree, current.split(":")[1]), "")

    def on_main_after_commit(self):
        self.commit({}, "commit reviewed work")
        self.on_main({"main_only.py": "m = 1\n"})
        run_git(self.repo, "merge", "-q", "--no-edit", "main")

    def test_an_unknown_base_strips_nothing_and_a_lost_commit_is_reported(self):
        reviewed = worktree_fingerprint(self.repo)
        self.commit({"f.py": self.OURS})
        current = worktree_fingerprint(self.repo)
        self.assertEqual(without_base(self.repo, reviewed, current, "no-such-ref").tree, reviewed.split(":")[1])
        with self.assertRaises(ReviewAborted):
            self.on_main({"main_only.py": "m = 1\n"})
            run_git(self.repo, "merge", "-q", "--no-edit", "main")
            without_base(self.repo, f"{'0' * 40}:{'0' * 40}", worktree_fingerprint(self.repo), "main")


class TestHandBackReview(SDEDeliveryCase):
    def merge_step(self, files, **kwargs):
        return {**dev(files, commit="merge origin/main", **kwargs), "run": MERGE_MAIN}

    def conflict(self, **files):
        self.push_main({"calc.py": MAIN_CALC, **OTHER, **files}, message="divide returns b")

    async def run_task(self, steps, reviews, **delivery):
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), *steps], reviewer=reviews)
        return await self.executor(delivery=self.delivery(**delivery)).execute([event(self.payload())], None, self.caps)

    def delta(self, call=1):
        prompt = self.calls("reviewer")[call]["prompt"]
        return prompt.split("## Changes since the p")[1].split("\n## Incremental review")[0], prompt

    async def test_a_passed_change_with_only_conflicts_resolved_is_delivered_without_another_review(self):
        self.conflict()
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC})], [PASS])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (2, 1))
        text = outcome.messages[0]
        for part in ("review=pass after 1 round", "Delivery: pr_open - PR created",
                     "Delivery hand-back 1 of 2: merging origin/main conflicts in calc.py",
                     "conflicts resolved only, not reviewed again"):
            self.assertIn(part, text)
        # The repository checks ran on the resolved head, and the PR says why it was not reviewed again.
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertIn("conflicts resolved only, not reviewed again", self.gh_state()["prs"][0]["body"])
        self.assertEqual(out(self.bare, "show", "hb/task-1:calc.py"), TASK_CALC.strip())

    async def test_failing_checks_after_a_plain_resolution_go_back_and_that_fix_is_reviewed_alone(self):
        self.conflict()
        check = ("/bin/sh", "-c", "test -f ok.txt || { echo 'needs ok.txt'; exit 1; }")
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC}), dev({"ok.txt": "ok\n"}, commit="fix")],
                                      [PASS, PASS], checks=(check,))
        assert isinstance(outcome, Emit), outcome
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (3, 2))
        self.assertIn("Delivery hand-back 2 of 2: repository checks failed", outcome.messages[0])
        self.assertIn("round 2 pass", outcome.messages[0])
        delta, prompt = self.delta()
        self.assertIn("+++ b/ok.txt", delta)
        # Neither main, nor the accepted resolution, nor Hibernaut's own version commit.
        for absent in ("main_only.py", "calc.py", VERSION_FILE):
            self.assertNotIn(absent, delta)
        self.assertIn("Files changed since the previous review:\n- ok.txt\n", prompt + "\n")

    async def test_a_second_conflict_is_judged_against_the_first_resolution(self):
        self.conflict()
        check = ("/bin/sh", "-c", "test -f ok.txt || exit 1")
        # The first attempt merges and versions, fails its checks, and main conflicts again before the fix is done.
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), self.merge_step({"calc.py": TASK_CALC}),
                           dev({"ok.txt": "ok\n"}, commit="fix"), self.merge_step({"calc.py": TASK_CALC})],
                      reviewer=[PASS, PASS])
        delivery = self.delivery(checks=(check,), max_handbacks=3)
        executor = self.executor(delivery=delivery)
        moved = []
        original = delivery.deliver

        def deliver(request, **kwargs):
            if (Path(request.workdir) / "ok.txt").exists() and not moved:
                moved.append(True)
                self.push_main({"calc.py": "def divide(a, b):\n    return b + b\n"}, message="again")
            return original(request, **kwargs)

        delivery.deliver = deliver  # type: ignore[method-assign]
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (4, 2))
        self.assertIn("hand-back 3 of 3: merging origin/main conflicts", outcome.messages[0])

    async def test_anything_beyond_the_resolution_is_reviewed_without_mains_changes(self):
        self.conflict()
        late = block("main_only.py:2", trigger="from_main()", impact="wrong", basis="introduced_since_previous")
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC, "extra.py": "n = 1\n"})],
                                      [PASS, verdict(late)])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("reviewer")), 2)
        delta, prompt = self.delta()
        self.assertIn("+++ b/extra.py", delta)
        self.assertIn("return b", delta)  # main's side of the conflict, as the block the developer resolved
        self.assertNotIn("main_only.py", delta)
        scope = prompt.split("## Incremental review\n")[1]
        self.assertIn("- calc.py\n- extra.py", scope)
        self.assertNotIn("main_only.py", scope)
        self.assertNotIn("main_only.py", prompt.split("## Code diff")[1])
        # A finding in code main brought in cannot send the task back.
        self.assertIn("round 2 pass (1 downgraded)", outcome.messages[0])

    async def test_new_logic_in_the_conflict_itself_is_reviewed(self):
        self.conflict()
        outcome = await self.run_task([self.merge_step({"calc.py": RESOLVED})], [PASS, PASS])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("reviewer")), 2)
        self.assertIn("raise ZeroDivisionError", self.delta()[0])

    async def test_a_requirement_added_during_the_hand_back_is_reviewed(self):
        self.conflict()
        question = {"output": {"status": "needs_input", "summary": "s", "test_evidence": "", "question": "which?",
                               "change_kind": "fix", "change_rationale": "r"}}
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), question, self.merge_step({"calc.py": TASK_CALC})],
                      reviewer=[PASS, PASS])
        executor = self.executor(delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        batch = [event({"text": "also log it"}), event({"approval_request_id": outcome.request_id, "approved": True})]
        done = await executor.execute(batch, self.resume(outcome), self.caps)
        assert isinstance(done, Emit), done
        self.assertEqual(len(self.calls("reviewer")), 2)

    async def test_an_answer_about_the_conflict_alone_is_not_a_new_requirement(self):
        self.conflict()
        question = {"output": {"status": "needs_input", "summary": "s", "test_evidence": "", "question": "which?",
                               "change_kind": "fix", "change_rationale": "r"}}
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), question, self.merge_step({"calc.py": TASK_CALC})],
                      reviewer=[PASS])
        executor = self.executor(delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        answer = event({"approval_request_id": outcome.request_id, "approved": True, "comment": "keep ours"})
        done = await executor.execute([answer], self.resume(outcome), self.caps)
        assert isinstance(done, Emit), done
        self.assertEqual(len(self.calls("reviewer")), 1)

    async def test_commits_nobody_reviewed_before_the_hand_back_are_reviewed(self):
        # Something was committed between the passed review and the delivery: it is not Hibernaut's, so it is shown.
        await self.sneaky_commit("sneaky.py")

    async def test_the_version_file_hides_no_other_change_made_before_the_hand_back(self):
        await self.sneaky_commit(VERSION_FILE)

    async def sneaky_commit(self, rel):
        self.conflict()
        delivery = self.delivery()
        original, done = delivery.deliver, []

        def deliver(request, **kwargs):
            if not done:
                done.append(True)
                target = self.wt / rel
                target.write_text((target.read_text() if target.exists() else "") + "s = 1\n")
                run_git(self.wt, "add", "-A")
                run_git(self.wt, "commit", "-qm", "sneaky")
            return original(request, **kwargs)

        delivery.deliver = deliver  # type: ignore[method-assign]
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), self.merge_step({"calc.py": TASK_CALC})], reviewer=[PASS, PASS])
        outcome = await self.executor(delivery=delivery).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("reviewer")), 2)
        delta = self.delta()[0]
        self.assertIn(rel, delta)
        self.assertIn("+s = 1", delta)

    async def test_main_merged_during_a_fix_round_is_left_out_too(self):
        self.push_main(OTHER, message="main adds a module")
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), {**dev(FIX), "run": [["git", "fetch", "-q"], *MERGE_MAIN]}],
                      reviewer=[FIRST, PASS])
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        delta, prompt = self.delta()
        self.assertIn("raise ValueError", delta)
        self.assertNotIn("main_only.py", delta)
        self.assertNotIn("main_only.py", prompt.split("## Incremental review\n")[1])

    async def test_an_unreviewed_change_is_not_skipped(self):
        # review=pending (a declined retry) never counts as passed.
        self.conflict()
        executor = self.executor(delivery=self.delivery())
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), self.merge_step({"calc.py": TASK_CALC})],
                      reviewer=[{"error": "reviewer crashed"}, PASS])
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        declined = await executor.execute([event({"approval_request_id": outcome.request_id, "approved": False})],
                                          self.resume(outcome), self.caps)
        assert isinstance(declined, Emit), declined
        self.assertEqual(len(self.calls("reviewer")), 2)
        self.assertNotIn("not reviewed again", declined.messages[0])

    def test_a_checkpoint_from_before_the_hand_back_record_still_loads(self):
        fields = _State(stage="review_pending", workdir="/w", base_ref="", requirements="r").fields()
        del fields["handback"]
        state = _State.load(json.dumps({"sde": fields}))
        assert state is not None
        self.assertEqual(state.handback, {})


if __name__ == "__main__":
    unittest.main()
