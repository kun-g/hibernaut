import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.base_merge import without_base
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, worktree_fingerprint
from agent_runtime.adapters.executor_cli.sde import _State, _review_delta
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
        run_git(self.repo, "tag", "fixture-base")
        run_git(self.repo, "checkout", "-qb", "task")

    def reset_branches(self):
        # Subcases end with all files committed. Restore both tips before building the next history.
        run_git(self.repo, "reset", "--hard", "fixture-base")
        run_git(self.repo, "branch", "-f", "main", "fixture-base")

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
        return since, current

    def assert_delta(self, before, after):
        # Test the exact patch given to the reviewer, including unchanged files named in a conflict.
        path = self.repo / "f.py"
        def tree(text):
            path.write_text(text)
            run_git(self.repo, "add", "f.py")
            return out(self.repo, "write-tree")

        old, new = tree(before), tree(after)
        delta, scope = _review_delta(self.repo, old, f":{new}", ("f.py",))
        self.assertEqual(scope, ["f.py"])
        if before == after:
            self.assertEqual(delta, "")
        else:
            subprocess.run(["git", "apply", "-R", "--whitespace=nowarn"], cwd=self.repo, input=delta, text=True, check=True)
            self.assertEqual(path.read_text(), before)

    def test_resolutions_from_either_side_are_in_scope(self):
        for name, text in {"combined": self.MERGED, "ours": self.OURS, "theirs": self.THEIRS}.items():
            with self.subTest(name):
                self.assert_delta(self.OURS, text)

    def test_changes_outside_conflict_blocks_are_shown(self):
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
                self.reset_branches()
                _, current = self.resolve(files)
                delta, paths = _review_delta(self.repo, self.reviewed.split(":")[1], current, ("f.py",))
                self.assertEqual(set(paths), {"main_only.py", "f.py", *files})
                self.assertIn("+++ b/f.py", delta)

    def block(self, base, ours, theirs, resolved):
        # One conflict block in the middle of a file.
        def text(lines):
            return "".join(f"{ln}\n" for ln in ["top = 1", "", *lines, "", "end = 1"])

        self.assert_delta(text(ours), text(resolved))

    def test_a_block_may_combine_what_the_sides_have(self):
        for name, args in {
            "both calls kept": ([], ["notify(a)"], ["notify(b)"], ["notify(a)", "notify(b)"]),
            "the other order": ([], ["notify(a)"], ["notify(b)"], ["notify(b)", "notify(a)"]),
            "both functions kept": ([], ["def f():", "    return 1"], ["def g():", "    return 1"],
                                    ["def f():", "    return 1", "def g():", "    return 1"]),
            "arguments combined": (["call(x)"], ["call(x, a)"], ["call(x, b)"], ["call(x, a, b)"]),
            "one side deleted it": (["call(x)"], ["call(x, a)"], [], []),
            "documents": (["说明一。"], ["说明一。补充甲。"], ["说明一。补充乙。"], ["说明一。补充甲。补充乙。"]),
        }.items():
            with self.subTest(name):
                self.block(*args)

    def test_a_block_that_the_sides_do_not_explain_is_reviewed(self):
        two = ([], ["notify(a)"], ["notify(b)"])
        for name, args in {
            "a call of one side twice": (*two, ["notify(a)", "notify(a)"]),
            "a call repeated beyond both sides": (*two, ["notify(a)", "notify(b)", "notify(b)"]),
            "a call repeated with words of the other side": ([], ["notify(a, b)"], ["notify(b)"],
                                                             ["notify(b)", "notify(b)"]),
            "lines that look like a separator": ([], ["Title", "======="], ["other"], ["Title", "=======", "other"]),
            "spaces in a string": ([], ["notify('a b')"], ["notify(b)"], ["notify('a  b')", "notify(b)"]),
            "a changed call kept in both versions": (["notify(x)"], ["notify(a)"], ["notify(b)"],
                                                     ["notify(a)", "notify(b)"]),
            "words in another order": ([], ["send(a, b)"], ["send(c)"], ["send(b, a)"]),
            "both sides dropped": (*two, []),
            "part of one side": ([], ["check(a)", "notify(a)"], ["notify(b)"], ["notify(a)"]),
            "what both sides kept dropped": (["if ok:", "    run(x)"], ["if ok:", "    run(a)"], ["if ok:", "    run(b)", "    log()"],
                                             ["run(a)", "log()"]),
            "another indentation": ([], ["if a:", "    f()", "g()"], ["h()"], ["if a:", "    f()", "    g()", "h()"]),
            "quotes": ([], ["notify(a)"], ["notify(b)"], ['notify("a")', "notify(b)"]),
        }.items():
            with self.subTest(name):
                self.block(*args)

    TEXT = (["T = \"\"\"", "head", "old", "\"\"\""], ["T = \"\"\"", "head", "ours", "\"\"\""],
            ["T = \"\"\"", "head", "theirs", "\"\"\""])

    def test_strings_taken_from_either_side_are_in_scope(self):
        _, ours, theirs = self.TEXT
        for name, resolved in {"ours": ours, "theirs": theirs}.items():
            with self.subTest(name):
                self.block(*self.TEXT, resolved)

    def test_blank_lines_and_trailing_spaces_are_part_of_a_string(self):
        base, ours, theirs = self.TEXT
        for name, resolved in {
            "a blank line in one side": ["T = \"\"\"", "head", "", "ours", "\"\"\""],
            "a trailing space in one side": ["T = \"\"\"", "head", "ours ", "\"\"\""],
            "spaces on a blank line": ["T = \"\"\"", "head", "ours", "  ", "\"\"\""],
        }.items():
            with self.subTest(name):
                self.block(base, ours, theirs, resolved)
        sides = ["T = \"\"\"", "ours", "\"\"\""], ["U = \"\"\"", "theirs", "\"\"\""]
        both = [*sides[0], *sides[1]]
        self.block([], *sides, both)
        for name, resolved in {
            "a blank line in both combined": [both[0], "", *both[1:]],
            "a trailing space in both combined": [both[0], "ours ", *both[2:]],
        }.items():
            with self.subTest(name):
                self.block([], *sides, resolved)

    def test_a_blank_line_is_not_made_of_another_line_that_was_dropped(self):
        self.block([], ["f(a)", "f(b)"], ["g(a)", "g(b)"], ["f(a)", "", "f(b)", "g(b)"])

    def test_blank_lines_of_both_sides_may_be_combined(self):
        self.block([], ["f()", "", "f()"], ["g()", "", "g()"], ["f()", "", "f()", "g()", "", "g()"])

    def test_a_change_taken_from_another_block_is_shown(self):
        # new(y) belongs to the second block; using it in the first one is another change.
        swapped = self.MERGED.replace("new(x)", "new(y)")
        self.assert_delta(self.OURS, swapped)

    def test_conflicts_without_blocks_are_not_judged(self):
        # main deleted a file the task changed: restoring the task's copy changes nothing, removing it is a decision.
        for remove, expected in (((), True), (("keep.py",), False)):
            with self.subTest(remove=remove):
                self.reset_branches()
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
                delta, paths = _review_delta(self.repo, reviewed.split(":")[1], current, ("keep.py",))
                self.assertEqual(paths, ["keep.py"])
                self.assertEqual(delta == "", expected)

    def test_nothing_merged_is_not_a_resolution(self):
        self.commit({"f.py": self.OURS})
        reviewed = worktree_fingerprint(self.repo)
        self.commit({"f.py": self.MERGED})
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, reviewed, current, "main")
        self.assertEqual(since.tree, reviewed.split(":")[1])

    def test_the_base_branchs_own_changes_are_left_out(self):
        since, current = self.resolve({"f.py": self.MERGED, "keep.py": "k = 2\n"})
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

    async def test_a_plain_conflict_resolution_is_reviewed_before_delivery(self):
        self.conflict()
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC})], [PASS, PASS])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (2, 2))
        delta, prompt = self.delta()
        self.assertIn("+++ b/main_only.py", delta)
        self.assertNotIn("+++ b/calc.py", delta)  # kept the reviewed side
        self.assertIn("calc.py", prompt.split("## Incremental review\n")[1])
        self.assertIn("round 2 pass", outcome.messages[0])
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertIn("round 2 pass", self.gh_state()["prs"][0]["body"])
        self.assertEqual(out(self.bare, "show", "hb/task-1:calc.py"), TASK_CALC.strip())

    async def test_failing_checks_after_a_plain_resolution_go_back_and_that_fix_is_reviewed_alone(self):
        self.conflict()
        check = ("/bin/sh", "-c", "test -f ok.txt || { echo 'needs ok.txt'; exit 1; }")
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC}), dev({"ok.txt": "ok\n"}, commit="fix")],
                                      [PASS, PASS, PASS], checks=(check,))
        assert isinstance(outcome, Emit), outcome
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (3, 3))
        self.assertIn("Delivery hand-back 2 of 2: repository checks failed", outcome.messages[0])
        self.assertIn("round 2 pass", outcome.messages[0])
        delta, prompt = self.delta(2)
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
                      reviewer=[PASS] * 4)
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
        self.assertEqual((len(self.calls("sde")), len(self.calls("reviewer"))), (4, 4))
        self.assertIn("hand-back 3 of 3: merging origin/main conflicts", outcome.messages[0])

    async def test_reviewer_can_block_a_problem_in_the_merged_result(self):
        self.conflict()
        late = block("main_only.py:2", trigger="from_main()", impact="wrong", basis="introduced_since_previous")
        outcome = await self.run_task([self.merge_step({"calc.py": TASK_CALC, "extra.py": "n = 1\n"}),
                                       dev({"main_only.py": "m = 2\n"})], [PASS, verdict(late), PASS])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("reviewer")), 3)
        delta, prompt = self.delta()
        for name in ("extra.py", "main_only.py"):
            self.assertIn(f"+++ b/{name}", delta)
        self.assertIn("- calc.py\n- extra.py\n- main_only.py", prompt)
        self.assertIn("main_only.py:2", self.calls("sde")[2]["prompt"])

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
                      reviewer=[PASS, PASS])
        executor = self.executor(delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        answer = event({"approval_request_id": outcome.request_id, "approved": True, "comment": "keep ours"})
        done = await executor.execute([answer], self.resume(outcome), self.caps)
        assert isinstance(done, Emit), done
        self.assertEqual(len(self.calls("reviewer")), 2)

    async def test_commits_nobody_reviewed_before_the_hand_back_are_reviewed(self):
        # Something was committed between the passed review and the delivery: it is not Hibernaut's, so it is shown.
        await self.sneaky_commit("sneaky.py")

    async def test_the_version_file_hides_no_other_change_made_before_the_hand_back(self):
        await self.sneaky_commit(VERSION_FILE)

    async def test_the_version_file_hides_no_mode_change_made_before_the_hand_back(self):
        await self.sneaky_commit(VERSION_FILE, mode=0o755, shown="new mode 100755")

    async def sneaky_commit(self, rel, mode=0, shown="+s = 1"):
        self.conflict()
        delivery = self.delivery()
        original, done = delivery.deliver, []

        def deliver(request, **kwargs):
            if not done:
                done.append(True)
                target = self.wt / rel
                if mode:
                    target.chmod(mode)
                else:
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
        self.assertIn(shown, delta)

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

    async def test_a_repeated_call_in_a_conflict_is_reviewed(self):
        self.push_main({"calc.py": "def divide(a, b):\n    notify(b)\n    return a\n"}, message="notify b")
        self.set_plan(sde=[dev({"calc.py": "def divide(a, b):\n    notify(a)\n    return a\n"}),
                           self.merge_step({"calc.py": "def divide(a, b):\n    notify(a)\n    notify(a)\n    return a\n"})],
                      reviewer=[PASS, PASS])
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("reviewer")), 2)
        self.assertNotIn("not reviewed again", outcome.messages[0])

    async def merged_in_a_fix_round(self, delivery, base_ref):
        # The task's local main stays behind; the developer merges origin/main while fixing round 1's item.
        self.push_main(OTHER, message="main adds a module")
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), {**dev(FIX), "run": [["git", "fetch", "-q"], *MERGE_MAIN]}],
                      reviewer=[FIRST, PASS])
        outcome = await self.executor(delivery=delivery).execute(
            [event(self.payload(base_ref=base_ref))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertNotEqual(out(self.wt, "rev-parse", "main"), out(self.wt, "rev-parse", "origin/main"))
        delta, prompt = self.delta()
        self.assertIn("raise ValueError", delta)
        self.assertNotIn("main_only.py", delta)
        self.assertNotIn("main_only.py", prompt.split("## Code diff")[1])

    async def test_a_delivery_without_prs_still_knows_the_remote_base(self):
        await self.merged_in_a_fix_round(self.delivery(open_pr=False), "main")

    async def test_without_delivery_a_local_base_stands_for_its_upstream(self):
        await self.merged_in_a_fix_round(None, "main")

    async def test_without_delivery_or_upstream_the_remote_branch_of_the_same_name_counts(self):
        run_git(self.wt, "branch", "--unset-upstream", "main")
        await self.merged_in_a_fix_round(None, "main")

    async def test_the_default_base_is_resolved_the_same_way(self):
        # Newer git recreates origin/HEAD on fetch.
        run_git(self.wt, "config", "remote.origin.followRemoteHEAD", "never")
        run_git(self.wt, "update-ref", "--no-deref", "-d", "refs/remotes/origin/HEAD")
        await self.merged_in_a_fix_round(None, "")

    async def another_default(self, delivery=None):
        # The repository's default branch is trunk; the developer merges main, which is ahead of it.
        run_git(self.wt, "push", "-q", "origin", "origin/main:refs/heads/trunk")
        run_git(self.wt, "fetch", "-q")
        run_git(self.wt, "remote", "set-head", "origin", "trunk")
        self.push_main(OTHER, message="main adds a module")
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), {**dev(FIX), "run": [["git", "fetch", "-q"], *MERGE_MAIN]}],
                      reviewer=[FIRST, PASS])
        outcome = await self.executor(delivery=delivery).execute(
            [event(self.payload(base_ref=""))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(out(self.wt, "rev-parse", "--symbolic-full-name", "origin/HEAD"), "refs/remotes/origin/trunk")
        return self.delta()

    async def test_a_branch_that_is_not_the_tasks_base_is_shown_when_merged(self):
        delta, prompt = await self.another_default()
        self.assertIn("main_only.py", delta)
        self.assertIn("- main_only.py", prompt.split("## Incremental review\n")[1])

    async def test_the_delivery_base_counts_whatever_the_tasks_base_is(self):
        delta, _ = await self.another_default(self.delivery(open_pr=False))
        self.assertNotIn("main_only.py", delta)

    async def ahead_locally(self, delivery):
        # Commits only the local main has, merged from there.
        before = out(self.wt, "rev-parse", "origin/main")
        self.push_main(OTHER, message="main adds a module")
        run_git(self.wt, "fetch", "-q")
        run_git(self.wt, "branch", "-f", "main", "origin/main")
        run_git(self.wt, "update-ref", "refs/remotes/origin/main", before)
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}), {**dev(FIX), "run": [["git", "merge", "--no-edit", "main"]]}],
                      reviewer=[FIRST, PASS])
        outcome = await self.executor(delivery=delivery).execute(
            [event(self.payload(base_ref="main"))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertTrue((self.wt / "main_only.py").exists())
        self.assertNotIn("main_only.py", self.delta()[0])

    async def test_a_local_base_ahead_of_its_upstream_is_left_out(self):
        await self.ahead_locally(None)

    async def test_a_local_base_ahead_of_the_delivery_base_is_left_out(self):
        await self.ahead_locally(self.delivery(open_pr=False))

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
