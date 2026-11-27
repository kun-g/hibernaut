import json
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery.pr_delivery import _followup_list
from agent_runtime.adapters.executor_cli.review_scope import ReviewScope, merge_notes
from agent_runtime.adapters.executor_cli.reviewer import REVIEW_SCHEMA, plain_review_contract, validate_review
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, review_prompt
from agent_runtime.adapters.executor_cli.sde import _State
from tests.support.review_flow_support import CONFLICT, DONE, QUESTION, REPORT, ReviewFlowCase
from agent_runtime.core.models import Emit, Suspend
from tests.support.pr_delivery import rendered_body
from tests.support.sde_delivery import PASS, SDEDeliveryCase, event
from tests.support.incremental_review import FIRST, OLD_CODE, block, verdict


class TestReviewScope(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for rel in ("calc.py", "old.py", "pkg/mod.py", "tests/test_calc.py", "my docs/说明.md"):
            (self.root / rel).parent.mkdir(parents=True, exist_ok=True)
            (self.root / rel).write_text("x\n")

    def scope(self, previous=(block("calc.py:2"),), changed=("pkg/mod.py",), workdir=None):  # noqa: B008 -- read, never mutated
        return ReviewScope(root=self.root, workdir=workdir or self.root, previous=tuple(previous), changed=tuple(changed))

    def kept(self, *items, **scope):
        narrowed = self.scope(**scope).narrow(validate_review(verdict(*items)))
        self.assertEqual(narrowed["verdict"], "changes_requested" if narrowed["blocking"] else "pass")
        self.assertEqual(len(narrowed["blocking"]) + len(narrowed["notes"]), len(items))
        return [i["location"] for i in narrowed["blocking"]]

    def test_a_new_problem_blocks_only_in_a_file_changed_since(self):
        new = {"basis": "introduced_since_previous"}
        self.assertEqual(self.kept(block("pkg/mod.py:9", **new), block("old.py:2", **new)), ["pkg/mod.py:9"])
        # Naming a previous item does not turn unchanged code into changed code.
        self.assertEqual(self.kept(block("calc.py:2", previous_item=1, **new)), [])

    def test_a_numbered_unfixed_item_blocks_wherever_it_is(self):
        unfixed = {"basis": "unfixed_previous", "previous_item": 1}
        # Reported at a place the item did not name (a missing test), or at no file: still that item.
        for location in ("calc.py:7", "tests/test_calc.py:1", "commit history"):
            with self.subTest(location):
                self.assertEqual(self.kept(block(location, trigger="other words", **unfixed)), [location])
        # A number without its label says the same.
        self.assertEqual(self.kept({**OLD_CODE, "previous_item": 1}), ["old.py:2"])

    def test_one_previous_item_backs_one_finding_outside_the_changes(self):
        unfixed = {"basis": "unfixed_previous", "previous_item": 1}
        # The closest one: the same place, else the same file, else the first.
        for locations, kept in ((("old.py:2", "calc.py:2"), "calc.py:2"), (("calc.py:80", "calc.py:2"), "calc.py:2"),
                                (("old.py:2", "calc.py:80"), "calc.py:80"), (("old.py:2", "commits"), "old.py:2"),
                                (("calc.py:80", "calc.py:90", "calc.py:100"), "calc.py:80")):
            with self.subTest(locations):
                items = [block(loc, trigger=f"case {i}", **unfixed) for i, loc in enumerate(locations)]
                self.assertEqual(self.kept(*items), [kept])
        # A finding in a changed file that stands for the item leaves none over, except at the item's own place.
        there = block("pkg/mod.py:1", trigger="t", **unfixed)
        self.assertEqual(self.kept(there, block("calc.py:80", trigger="u", **unfixed)), ["pkg/mod.py:1"])
        self.assertEqual(self.kept(there, block("calc.py:2", trigger="u", **unfixed)), ["pkg/mod.py:1", "calc.py:2"])

    def test_explicit_references_keep_their_item_whatever_the_order(self):
        # Two previous items in one file with the same trigger: either report fits both by its words alone.
        previous = [block("calc.py:2"), block("calc.py:40")]
        second = block("calc.py:40", basis="unfixed_previous", previous_item=2)
        first = block("old.py:2", basis="unfixed_previous", previous_item=1)  # at a caller
        for items in ((second, first), (first, second)):
            with self.subTest([i["location"] for i in items]):
                self.assertEqual(self.kept(*items, previous=previous), [i["location"] for i in items])
        # A report without a reference takes what is left, and does not push out the one that names its item.
        bare = block("calc.py:2")
        self.assertEqual(self.kept(bare, block("old.py:2", previous_item=1), previous=previous),
                         ["calc.py:2", "old.py:2"])

    def test_reports_without_a_reference_do_not_crowd_each_other_out(self):
        # calc.py:2 fits both previous items, calc.py:40 (other words) only the second.
        previous = [block("calc.py:2", trigger="a"), block("calc.py:2-50", trigger="b")]
        items = [block("calc.py:2", trigger="b"), block("calc.py:40", trigger="c")]
        for order in (items, items[::-1]):
            with self.subTest([i["location"] for i in order]):
                self.assertEqual(self.kept(*order, previous=previous), [i["location"] for i in order])

    def test_a_second_report_of_one_item_does_not_take_another_items_place(self):
        previous = [block("calc.py:2"), block("calc.py:40")]
        named = {"basis": "unfixed_previous", "previous_item": 1}
        items = [block("calc.py:2", **named), block("calc.py:2", **named), block("calc.py:40", trigger="other words")]
        for order in (items, items[::-1]):
            with self.subTest([i["location"] for i in order]):
                kept = self.kept(*order, previous=previous)
                self.assertEqual(sorted(kept), ["calc.py:2", "calc.py:40"])

    def test_without_a_reference_another_problem_in_the_same_file_is_a_note(self):
        for label in ({}, {"basis": "unfixed_previous"}, {"basis": "unfixed_previous", "previous_item": 4}):
            for location in ("calc.py:80", "calc.py", "calc.py:1-99", "old.py:2", "tests/test_calc.py:3"):
                with self.subTest(label=label, location=location):
                    previous = [block("calc.py:2", fix="add a case to tests/test_calc.py")]
                    item = block(location, trigger="total is read before it is set", **label)
                    self.assertEqual(self.kept(item, previous=previous), [])

    def test_without_a_reference_the_same_place_or_trigger_identifies_the_previous_item(self):
        # The file is unchanged, so its lines have not moved.
        for previous, location in (("calc.py:2", "calc.py:2"), ("calc.py:2", "./calc.py#L2"), ("calc.py:2-6", "calc.py:4"),
                                   ("my docs/说明.md:3", "my docs/说明.md:3")):
            with self.subTest(location):
                item = block(location, trigger="worded differently")
                self.assertEqual(self.kept(item, previous=[block(previous)]), [location])
        self.assertEqual(self.kept(block("calc.py:80", trigger="Divide(1,  0)")), ["calc.py:80"])
        self.assertEqual(self.kept(block("missing tests"), previous=[block("missing tests")]), ["missing tests"])
        # The same trigger in an unrelated file is another finding, and each previous item is matched once.
        self.assertEqual(self.kept(block("old.py:2")), [])
        self.assertEqual(self.kept(block("calc.py:2"), block("calc.py:2")), ["calc.py:2"])
        pair = [block("calc.py:2", trigger="a"), block("calc.py:2", trigger="b")]
        self.assertEqual(self.kept(*pair, previous=pair), ["calc.py:2", "calc.py:2"])

    def test_unusable_numbers_and_file_names_do_not_break_a_review(self):
        huge = block("old.py:2", basis="unfixed_previous", previous_item="9" * 5000)
        self.assertEqual(self.kept(huge), [])
        self.assertEqual(self.kept(block("\0 " + "x" * 5000, basis="introduced_since_previous")), [])
        scope = self.scope(changed=[b"caf\xe9.py".decode("utf-8", "surrogateescape"), *(f"f{i}" for i in range(400))])
        scope.render().encode("utf-8")
        self.assertIn("- ... and 101 more", scope.render())

    def test_after_a_passed_review_only_changed_files_block(self):
        items = (block("pkg/mod.py:1", basis="introduced_since_previous"),
                 block("old.py:2", basis="unfixed_previous", previous_item=1))
        self.assertEqual(self.kept(*items, previous=()), ["pkg/mod.py:1"])

    def test_locations_are_read_the_ways_reviewers_write_them(self):
        new = {"basis": "introduced_since_previous"}
        for location in ("pkg/mod.py", "pkg/mod.py:10-20", "`pkg/mod.py:10`", "./pkg/mod.py:1", "mod.py:3",
                         f"{self.root}/pkg/mod.py:3", "old.py:2, pkg/mod.py:3", "pkg/mod.py#L3", "pkg/mod.py（第 3 行）",
                         "In pkg/mod.py."):
            with self.subTest(location):
                self.assertEqual(self.kept(block(location, **new)), [location])
        for location in ("old.py:2", "pkg/mod.pyi:1", "other/pkg/mod.py", "mod", "pkg", "(general)"):
            with self.subTest(location):
                self.assertEqual(self.kept(block(location, **new)), [])
        self.assertEqual(self.kept(block("my docs/说明.md:1", **new), changed=["my docs/说明.md"]), ["my docs/说明.md:1"])
        # A deleted file is a change; an existing root file is not mistaken for one of the same name elsewhere.
        self.assertEqual(self.kept(block("gone.py:1", **new), changed=["gone.py"]), ["gone.py:1"])
        self.assertEqual(self.kept(block("calc.py:1", **new), changed=["pkg/calc.py"]), [])
        # Paths may be relative to a workdir below the repository root.
        self.assertEqual(self.kept(block("mod.py:1", **new), workdir=self.root / "pkg"), ["mod.py:1"])

    def test_downgraded_items_keep_their_content_and_say_why(self):
        review = validate_review(verdict(block("old.py:2", trigger="total(None)", impact="TypeError", fix="check xs"),
                                         notes=["n1"]))
        narrowed = self.scope().narrow(review)
        self.assertEqual((narrowed["verdict"], narrowed["reviewer_verdict"], narrowed["downgraded"]),
                         ("pass", "changes_requested", 1))
        self.assertEqual(narrowed["notes"][0], "n1")
        for part in ("old.py:2", "total(None)", "TypeError", "check xs", "已降级："):
            self.assertIn(part, narrowed["notes"][1])
        # Nothing to downgrade: the review is what the reviewer said.
        self.assertEqual(self.scope().narrow(validate_review(PASS)), validate_review(PASS))

    def test_notes_merge_without_repeats(self):
        down = self.scope().narrow(validate_review(verdict(OLD_CODE)))["notes"]
        again = [f"{down[0].split('（已降级：')[0]}（已降级：别的原因）"]
        self.assertEqual(merge_notes(["a  b"], ["A b\n", "c", "", *down, *again]), ["a  b", "c", *down])


class TestReviewContract(unittest.TestCase):
    def test_the_verdict_schema_and_contract_name_both_bases(self):
        item = REVIEW_SCHEMA["properties"]["blocking"]["items"]
        self.assertEqual(item["properties"]["basis"]["enum"], ["unfixed_previous", "introduced_since_previous"])
        self.assertEqual(item["properties"]["previous_item"]["type"], "integer")
        self.assertEqual(item["required"], ["location", "trigger", "impact"])
        for part in ("## Incremental review", "unfixed_previous", "introduced_since_previous", "previous_item"):
            self.assertIn(part, plain_review_contract())

    def test_previous_item_is_read_as_a_whole_number(self):
        for raw, read in ((2, "2"), ("2", "2"), (2.0, "2"), (True, None), (1.5, None), ([1], None)):
            with self.subTest(raw):
                (item,) = validate_review(verdict(block("calc.py:2", previous_item=raw)))["blocking"]
                self.assertEqual(item.get("previous_item"), read)


class TestRoleReviewRounds(ReviewFlowCase):
    def prompt(self, index):
        return review_prompt(self.request(index), "", "+++ b/calc.py\n+++ b/old.py")

    def developer_prompt(self, index):
        return self.develop.await_args_list[index].args[3]

    def pr_body(self):
        req = self.delivery.deliver.call_args.args[0]
        return rendered_body(followups=req.followups, summary=req.summary, evidence=req.test_evidence)

    async def rounds(self, *reviews, changes=()):
        snapshots = [f"round-{i}" for i in range(len(reviews))]
        diffs = {(before, after): (f"+++ b/{path}", [path]) for before, after, path in
                 zip(snapshots[:-1], snapshots[1:], changes, strict=True)}
        return await self.run_flow(snapshots=snapshots, diffs=diffs, deliveries=[DONE],
                                   reviews=[validate_review(r) for r in reviews])

    async def test_the_first_round_may_block_on_anything(self):
        outcome = await self.rounds(verdict(OLD_CODE), PASS, changes=["calc.py"])
        self.assertIsInstance(outcome, Emit)
        self.assertEqual(self.develop.await_count, 2)
        self.assertIn("old.py:2", self.developer_prompt(1))
        self.assertNotIn("## Incremental review", self.prompt(0))

    async def test_an_unfixed_previous_item_is_sent_back_again(self):
        unfixed = block("calc.py:2", impact="still crashes", basis="unfixed_previous", previous_item=1)
        outcome = await self.rounds(FIRST, verdict(unfixed, OLD_CODE), PASS, changes=["readme.txt", "calc.py"])
        self.assertIsInstance(outcome, Emit)
        self.assertIn("review=pass after 3 round", outcome.messages[0])
        self.assertIn("still crashes", self.developer_prompt(2))
        self.assertNotIn("old.py:2", self.developer_prompt(2))
        self.assertIn("round 2 changes_requested (1 blocking, 1 downgraded)", outcome.messages[0])
        self.assertIn("old.py:2", self.pr_body().split("## 跟进\n")[1])

    async def test_a_problem_the_fix_introduced_is_sent_back(self):
        introduced = block("calc.py:3", impact="wrong exception type", basis="introduced_since_previous")
        outcome = await self.rounds(FIRST, verdict(introduced), PASS, changes=["calc.py", "calc.py"])
        self.assertIsInstance(outcome, Emit)
        self.assertIn("review=pass after 3 round", outcome.messages[0])
        self.assertIn("wrong exception type", self.developer_prompt(2))

    async def test_the_second_round_is_told_its_scope_and_still_gets_the_full_diff(self):
        await self.rounds(FIRST, PASS, changes=["calc.py"])
        prompt = self.prompt(1)
        scope = prompt.split("## Incremental review\n")[1]
        for part in ("unfixed_previous", "introduced_since_previous", "- calc.py\n", "1 blocking item",
                     "rename divide to safe_divide"):
            self.assertIn(part, scope)
        self.assertNotIn("- old.py", scope)
        self.assertIn("old.py", prompt.split("## Code diff")[1].split("## Previous review round")[0])

    async def test_a_recheck_after_a_hand_back_blocks_only_on_changes_since_the_pass(self):
        outcome = await self.run_flow(snapshots=["first", "resolved"], diffs={("first", "resolved"): ("delta", ["calc.py"])},
                                      reviews=[PASS, validate_review(verdict({**OLD_CODE, "basis": "unfixed_previous",
                                                                             "previous_item": 1}))])
        self.assertIsInstance(outcome, Emit)
        self.assertIn("round 2 pass (1 downgraded)", outcome.messages[0])
        self.assertIn("## Incremental review", self.prompt(1))
        self.assertIn("old.py:2", self.pr_body().split("## 跟进\n")[1])

    async def test_a_recheck_still_blocks_on_the_conflict_resolution(self):
        bad = block("calc.py:3", impact="lost main's change", basis="introduced_since_previous")
        outcome = await self.run_flow(snapshots=["first", "merged", "fixed"],
                                      diffs={("first", "merged"): ("resolution", ["calc.py"]),
                                             ("merged", "fixed"): ("fix", ["calc.py"])},
                                      reviews=[PASS, verdict(bad), PASS])
        self.assertIsInstance(outcome, Emit)
        self.assertIn("lost main's change", self.developer_prompt(2))

    async def test_a_follow_up_is_developed_and_the_next_review_stays_incremental(self):
        # Each subcase starts a new task and may resume it through a human decision.
        for follow_up, recheck in (("carry on with the original requirements", False), ("keep main's divide", True)):
            with self.subTest(follow_up):
                await self.follow_up_round(follow_up, recheck)

    async def follow_up_round(self, follow_up, recheck):
        self.state = _State(stage="develop", workdir="/test/repo", base_ref="main", requirements="divide",
                            project={"workdir": "/test/repo", "commit": "initial"})
        self.develop.reset_mock()
        self.reviewer.review.reset_mock()
        late = verdict({**OLD_CODE, "basis": "introduced_since_previous"})
        outcome = await self.run_flow(snapshots=["first", "fixed"], diffs={("first", "fixed"): ("fix", ["calc.py"])},
                                      deliveries=[CONFLICT, DONE] if recheck else [DONE], reviews=[PASS if recheck else FIRST, late],
                                      reports=[REPORT, QUESTION, REPORT])
        self.assertIsInstance(outcome, Suspend)
        batch = [event({"text": follow_up}), event({"approval_request_id": outcome.request_id, "approved": True})]
        done = await self.executor.execute(batch, SDEDeliveryCase.resume(outcome), self.caps)
        self.assertIsInstance(done, Emit)
        self.assertIn("round 2 pass (1 downgraded)", done.messages[0])
        self.assertIn(follow_up, self.developer_prompt(2))
        prompt = self.prompt(1)
        self.assertIn(follow_up, prompt)
        scope = prompt.split("## Incremental review\n")[1]
        self.assertIn("A requirement a person added since that review", scope)
        self.assertIn("keep its real location and put it in notes", scope)
        self.assertNotIn("closest", scope)
        self.assertEqual("## Why this is reviewed again" in prompt, recheck)

    async def test_a_follow_up_with_the_approval_of_a_spent_review_stays_incremental(self):
        self.executor._options = type(self.executor._options)(max_review_rounds=1)
        outcome = await self.rounds(FIRST, verdict(OLD_CODE), changes=["calc.py"])
        self.assertIsInstance(outcome, Suspend)
        batch = [event({"text": "fix it as originally required"}),
                 event({"approval_request_id": outcome.request_id, "approved": True})]
        done = await self.executor.execute(batch, SDEDeliveryCase.resume(outcome), self.caps)
        self.assertIsInstance(done, Emit)
        self.assertIn("fix it as originally required", self.developer_prompt(1))
        self.assertIn("round 2 pass (1 downgraded)", done.messages[0])

    async def test_rounds_still_run_out_and_wait_for_a_person(self):
        unfixed = block("calc.py:2", basis="unfixed_previous", previous_item=1)
        outcome = await self.rounds(FIRST, verdict(unfixed, OLD_CODE), verdict(unfixed, OLD_CODE), changes=["a.txt", "a.txt"])
        self.assertIsInstance(outcome, Suspend)
        self.assertIn("still changes_requested after 3 round(s)", outcome.prompt)
        self.assertEqual(self.reviewer.review.await_count, 3)

    async def test_notes_survive_a_declined_review_retry(self):
        outcome = await self.run_flow(snapshots=["first", "fixed"], diffs={("first", "fixed"): ("fix", ["calc.py"])},
                                      reviews=[FIRST, ReviewAborted("reviewer crashed")], deliveries=[DONE])
        self.assertIsInstance(outcome, Suspend)
        declined = await self.executor.execute([event({"approval_request_id": outcome.request_id, "approved": False})],
                                               SDEDeliveryCase.resume(outcome), self.caps)
        self.assertIsInstance(declined, Emit)
        self.assertIn("跟进：\n- rename divide to safe_divide", declined.messages[0])
        self.assertIn("## 跟进\n- rename divide to safe_divide", self.pr_body())

    async def test_without_notes_there_is_no_follow_up_section(self):
        outcome = await self.rounds(PASS)
        self.assertIsInstance(outcome, Emit)
        self.assertNotIn("跟进", outcome.messages[0])
        self.assertNotIn("## 跟进", self.pr_body())


class TestReviewCheckpoint(unittest.TestCase):
    def test_a_checkpoint_from_before_notes_were_kept_still_loads(self):
        fields = _State(stage="review_pending", workdir="/w", base_ref="", requirements="r").fields()
        del fields["review_notes"]
        state = _State.load(json.dumps({"sde": fields}))
        assert state is not None
        self.assertEqual(state.review_notes, [])


class TestFollowUpSection(unittest.TestCase):
    def body(self, *followups, summary="s", evidence="e"):
        return rendered_body(followups=followups, summary=summary, evidence=evidence)

    def test_notes_are_one_line_each_and_ping_or_close_nothing(self):
        section = self.body("first\nline", "use @property; fixes #12").split("## 跟进\n")[1].split("\n\n## ")[0]
        self.assertEqual(section, "- first line\n- use @\u200bproperty; fixes #\u200b12")

    def section(self, body):
        return body.split("## 跟进\n")[1].split("\n\n## ")[0]

    def test_notes_are_whole_while_the_body_fits(self):
        note = "n" * 15_001
        self.assertEqual(self.section(self.body(note, "second")), f"- {note}\n- second")

    def test_the_room_left_by_the_rest_of_the_body_is_used_to_the_last_character(self):
        overhead = len(self.body("x")) - 1
        exact = "n" * (65_536 - overhead)
        body = self.body(exact)
        self.assertEqual((len(body), self.section(body)), (65_536, f"- {exact}"))
        over = self.body(exact + "n")
        self.assertLessEqual(len(over), 65_536)
        self.assertGreater(len(over), 65_000)
        lines = self.section(over).splitlines()
        self.assertTrue(lines[0].startswith("- nnnn") and lines[0].endswith("…"), lines[0][-20:])
        self.assertEqual(lines[-1], "- 已截断，另有 0 条未列；全文见 Hibernaut 完成消息")

    def test_a_list_that_does_not_fit_says_how_many_are_missing(self):
        body = self.body(*(f"note {i} " + "x" * 900 for i in range(100)), summary="s" * 20_000)
        self.assertLessEqual(len(body), 65_536)
        lines = self.section(body).splitlines()
        self.assertEqual(lines[-1], f"- 已截断，另有 {100 - (len(lines) - 1)} 条未列；全文见 Hibernaut 完成消息")
        self.assertGreater(len(lines), 40)
        self.assertIn("## Test evidence", body)

    def test_a_body_with_no_room_left_gives_some_to_the_follow_ups(self):
        # The rest of the body, heading included, leaves 10 characters: fewer than the notice itself takes.
        for name, evidence in (("long evidence", "e" * 20_000), ("short evidence", "e")):
            with self.subTest(name):
                fixed = len(self.body("x", evidence=evidence)) - 2  # without the one-character summary and note
                summary = "s" * (65_526 - fixed)
                body = self.body("n" * 5_000, summary=summary, evidence=evidence)
                self.assertLessEqual(len(body), 65_536)
                lines = self.section(body).splitlines()
                self.assertTrue(lines[0].startswith("- nnnn"), lines[0][:20])
                self.assertIn("全文见 Hibernaut 完成消息", lines[-1])
                if len(evidence) > 1:
                    self.assertIn(summary, body)
                for heading in ("## Summary", "## Review", "## Test evidence", "Delivered by Hibernaut"):
                    self.assertIn(heading, body)

    def test_a_section_that_cannot_fit_its_notice_is_left_out(self):
        self.assertEqual(_followup_list(["n" * 100], 10), "")
        self.assertEqual(_followup_list(["n" * 100], -5), "")
        for room in range(0, 110):
            self.assertLessEqual(len(_followup_list(["n" * 60, "m" * 60], room)), room)


if __name__ == "__main__":
    unittest.main()
