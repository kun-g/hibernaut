import difflib
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.delivery import DeliveryRecord
from agent_runtime.adapters.executor_cli import sde
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted
from agent_runtime.adapters.executor_cli.sde import _State
from agent_runtime.core.models import Emit, Suspend
from tests.review_flow_support import CHECKS, CONFLICT, DONE, FIX, MAIN, QUESTION, REPORT, ReviewFlowCase
from tests.test_incremental_review import block, verdict
from tests.test_pr_delivery import VERSION_FILE
from tests.test_sde_delivery import PASS, SDEDeliveryCase, event


def setUpModule():
    global process_guard
    process_guard = patch.object(subprocess, "Popen", side_effect=AssertionError("unexpected subprocess"))
    process_guard.start()


def tearDownModule():
    process_guard.stop()


class TestConflictPatchText(unittest.TestCase):
    BASE = "a = 1\nb = 1\nc = 1\nd = 1\ne = 1\nf = 1\ng = 1\n"
    OURS = BASE.replace("b = 1", "b = old(x)").replace("f = 1", "f = old(y)")
    THEIRS = BASE.replace("b = 1", "b = new(1)").replace("f = 1", "f = new(1)")
    MERGED = BASE.replace("b = 1", "b = new(x)").replace("f = 1", "f = new(y)")

    def assert_delta(self, before, after):
        evidence = "".join(difflib.unified_diff(before.splitlines(True), after.splitlines(True), "a/f.py", "b/f.py"))
        with (patch.object(sde, "snapshot_diff", return_value=evidence),
              patch.object(sde, "changed_paths", return_value=["f.py"] if evidence else [])):
            delta, paths = sde._review_delta(Path("repo"), "before", "head:after", ("f.py",))
        self.assertEqual(paths, ["f.py"])
        self.assertEqual(delta, evidence)
        section = sde._recheck_section("conflict", "pass", delta, conflict=True)
        self.assertIn(f"```diff\n{evidence}\n```", section)

    def test_resolutions_from_either_side_are_in_scope(self):
        for name, text in {"combined": self.MERGED, "ours": self.OURS, "theirs": self.THEIRS}.items():
            with self.subTest(name):
                self.assert_delta(self.OURS, text)

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

class TestHandBackReview(ReviewFlowCase):
    async def simple_conflict(self, diff=MAIN, paths=("main_only.py",)):
        outcome = await self.run_flow(snapshots=["first", "resolved"], diffs={("first", "resolved"): (diff, paths)})
        self.assertIsInstance(outcome, Emit)
        self.assertEqual((self.develop.await_count, self.reviewer.review.await_count, self.delivery.deliver.call_count),
                         (2, 2, 2))
        self.assertIn(f"```diff\n{diff}\n```", self.request().previous_round)
        return outcome

    def persistent_conflict(self, limit):
        record = DeliveryRecord(problem="merging origin/main into hb/task-1 conflicts in calc.py",
                                handback=CONFLICT.handback)
        self.delivery.config.max_handbacks = limit
        self.delivery.record.return_value = record
        return record

    async def test_hand_backs_stop_at_the_limit_and_wait_for_a_person(self):
        conflict = self.persistent_conflict(2)
        outcome = await self.run_flow(snapshots=["first", "retry1", "retry2"],
                                      diffs={("first", "retry1"): ("tried", ["notes.txt"]),
                                             ("retry1", "retry2"): ("", [])},
                                      reviews=[PASS] * 3, deliveries=[conflict] * 3)
        self.assertIsInstance(outcome, Suspend)
        self.assertEqual((self.develop.await_count, self.delivery.deliver.call_count), (3, 3))
        for part in ("[sde] delivery pending: merging origin/main into hb/task-1 conflicts in calc.py",
                     "Handed back to the developer 2 time(s) already (limit 2); a person decides now.",
                     "Delivery hand-back 1 of 2", "Delivery hand-back 2 of 2"):
            self.assertIn(part, outcome.prompt)
        self.assertNotIn("PR created", outcome.prompt)

    async def test_a_limit_of_zero_suspends_as_before(self):
        conflict = self.persistent_conflict(0)
        outcome = await self.run_flow(snapshots=["first"], diffs={}, reviews=[PASS], deliveries=[conflict])
        self.assertIsInstance(outcome, Suspend)
        self.assertIn("conflicts in calc.py", outcome.prompt)
        self.assertEqual(self.develop.await_count, 1)

    async def test_a_question_from_the_developer_during_a_hand_back_suspends(self):
        conflict = self.persistent_conflict(2)
        question = {"status": "needs_input", "question": "keep main's divide or ours?"}
        outcome = await self.run_flow(snapshots=["first"], diffs={}, reviews=[PASS], deliveries=[conflict],
                                      reports=[REPORT, question])
        self.assertIsInstance(outcome, Suspend)
        self.assertIn("[sde] needs input: keep main's divide or ours?", outcome.prompt)
        self.assertIn("Delivery hand-back 1 of 2: merging origin/main conflicts in calc.py", outcome.prompt)
        self.assertEqual(self.delivery.deliver.call_count, 1)
        self.assertNotIn("PR created", outcome.prompt)

    async def test_a_plain_conflict_resolution_is_reviewed_before_delivery(self):
        outcome = await self.simple_conflict()
        self.assertEqual(self.request().scope.changed, ("calc.py", "main_only.py"))
        self.assertIn("round 2 pass", self.delivery.deliver.call_args.args[0].review_detail)
        self.assertIn("round 2 pass", outcome.messages[0])

    async def test_failing_checks_after_a_plain_resolution_go_back_and_that_fix_is_reviewed_alone(self):
        self.delivery.config.max_handbacks = 2
        outcome = await self.run_flow(snapshots=["first", "merged", "fixed"],
                                      diffs={("first", "merged"): (MAIN, ["main_only.py"]),
                                             ("versioned", "fixed"): (FIX, ["ok.txt"])},
                                      deliveries=[CONFLICT, CHECKS, DONE], reviews=[PASS] * 3)
        self.assertIsInstance(outcome, Emit)
        self.assertIn("Delivery hand-back 2 of 2: repository checks failed", outcome.messages[0])
        self.assertEqual(self.develop.await_count, 3)
        self.assertEqual(self.request().scope.changed, ("ok.txt",))
        self.assertIn(FIX, self.request().previous_round)
        self.assertNotIn(MAIN, self.request().previous_round)

    async def test_a_second_conflict_is_judged_against_the_last_passed_snapshot(self):
        last = "--- a/calc.py\n+++ b/calc.py\n@@ -1 +1 @@\n-return b\n+return b + b\n"
        outcome = await self.run_flow(snapshots=["first", "merged", "fixed", "last"],
                                      diffs={("first", "merged"): (MAIN, ["main_only.py"]),
                                             ("versioned", "fixed"): (FIX, ["ok.txt"]),
                                             ("fixed", "last"): (last, ["calc.py"])},
                                      deliveries=[CONFLICT, CHECKS, CONFLICT, DONE], reviews=[PASS] * 4)
        self.assertIsInstance(outcome, Emit)
        self.assertEqual((self.develop.await_count, self.reviewer.review.await_count, self.delivery.deliver.call_count),
                         (4, 4, 4))
        self.assertEqual(self.request(0).previous_round, "")
        for i, diff in enumerate((MAIN, FIX, last), start=1):
            self.assertIn(f"```diff\n{diff}\n```", self.request(i).previous_round)
        self.assertIn("hand-back 3 of 3: merging origin/main conflicts", outcome.messages[0])

    async def test_reviewer_can_block_a_problem_in_the_merged_result(self):
        late = block("main_only.py:1", trigger="from_main()", impact="wrong", basis="introduced_since_previous")
        outcome = await self.run_flow(snapshots=["first", "merged", "fixed"],
                                      diffs={("first", "merged"): (MAIN, ["main_only.py"]),
                                             ("merged", "fixed"): (FIX, ["ok.txt"])}, reviews=[PASS, verdict(late), PASS])
        self.assertIsInstance(outcome, Emit)
        self.assertEqual(self.request(1).scope.changed, ("calc.py", "main_only.py"))
        self.assertIn("main_only.py:1", self.develop.await_args_list[2].args[3])
        self.assertEqual(self.reviewer.review.await_count, 3)

    async def test_new_logic_in_the_conflict_itself_is_reviewed(self):
        await self.simple_conflict("+++ b/calc.py\n+    raise ZeroDivisionError\n", ["calc.py"])

    async def answer(self, *extra_events, comment=""):
        outcome = await self.run_flow(snapshots=["first", "merged"], diffs={("first", "merged"): (MAIN, ["main_only.py"])},
                                      reports=[REPORT, QUESTION, REPORT])
        self.assertIsInstance(outcome, Suspend)
        done = await self.executor.execute([*extra_events, event({"approval_request_id": outcome.request_id,
                                                                   "approved": True, "comment": comment})],
                                           SDEDeliveryCase.resume(outcome), self.caps)
        self.assertIsInstance(done, Emit)
        self.assertEqual(self.reviewer.review.await_count, 2)

    async def test_a_requirement_added_during_the_hand_back_is_reviewed(self):
        await self.answer(event({"text": "also log it"}))
        self.assertIn("also log it", self.request().requirements)

    async def test_an_answer_about_the_conflict_alone_is_not_a_new_requirement(self):
        await self.answer(comment="keep ours")
        self.assertIn("keep ours", self.develop.await_args.args[3])
        self.assertNotIn("keep ours", self.request().requirements)

    async def test_commits_nobody_reviewed_before_the_hand_back_are_reviewed(self):
        await self.simple_conflict("+++ b/sneaky.py\n+s = 1\n", ["sneaky.py"])

    async def test_the_version_file_hides_no_other_change_made_before_the_hand_back(self):
        await self.simple_conflict(f"+++ b/{VERSION_FILE}\n+s = 1\n", [VERSION_FILE])

    async def test_the_version_file_hides_no_mode_change_made_before_the_hand_back(self):
        await self.simple_conflict(f"diff --git a/{VERSION_FILE} b/{VERSION_FILE}\nold mode 100644\nnew mode 100755\n",
                                   [VERSION_FILE])

    async def test_a_repeated_call_in_a_conflict_is_reviewed(self):
        await self.simple_conflict("+++ b/calc.py\n     notify(a)\n+    notify(a)\n", ["calc.py"])

    async def test_an_unreviewed_change_is_not_skipped(self):
        outcome = await self.run_flow(snapshots=["first", "merged"], diffs={}, reviews=[ReviewAborted("crashed"), PASS])
        self.assertIsInstance(outcome, Suspend)
        declined = await self.executor.execute([event({"approval_request_id": outcome.request_id, "approved": False})],
                                               SDEDeliveryCase.resume(outcome), self.caps)
        self.assertIsInstance(declined, Emit)
        self.assertEqual(self.reviewer.review.await_count, 2)
        self.assertIsNone(self.request().scope)


class TestHandBackCheckpoint(unittest.TestCase):
    def test_a_checkpoint_from_before_the_hand_back_record_still_loads(self):
        fields = _State(stage="review_pending", workdir="/w", base_ref="", requirements="r").fields()
        del fields["handback"]
        state = _State.load(json.dumps({"sde": fields}))
        assert state is not None
        self.assertEqual(state.handback, {})


if __name__ == "__main__":
    unittest.main()
