import json
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery import DeliveryRequest
from agent_runtime.adapters.executor_cli.review_scope import ReviewScope, merge_notes
from agent_runtime.adapters.executor_cli.reviewer import REVIEW_SCHEMA, plain_review_contract, validate_review
from agent_runtime.adapters.executor_cli.sde import _State
from agent_runtime.core.models import Emit, Suspend
from tests.test_coder_reviewer import git
from tests.test_pr_delivery import DeliveryCase
from tests.test_sde_delivery import MAIN_CALC, MERGE_MAIN, PASS, RESOLVED, TASK_CALC, SDEDeliveryCase, dev, event

BUGGY = {"calc.py": "def divide(a, b):\n    return a / b\n", "old.py": "def total(xs):\n    return sum(xs)\n"}
FIX = {"calc.py": "def divide(a, b):\n    if b == 0:\n        raise ValueError('b')\n    return a / b\n"}


def block(location, trigger="divide(1, 0)", impact="crashes the caller", **extra):
    return {"location": location, "trigger": trigger, "impact": impact, **extra}


def verdict(*blocking, notes=(), summary="reviewed"):
    return {"verdict": "changes_requested" if blocking else "pass", "summary": summary, "blocking": list(blocking),
            "notes": list(notes)}


FIRST = verdict(block("calc.py:2"), notes=["rename divide to safe_divide"])
OLD_CODE = block("old.py:2", trigger="total(None)", impact="TypeError")


class TestReviewScope(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for rel in ("calc.py", "old.py", "pkg/mod.py", "tests/test_calc.py", "my docs/说明.md"):
            (self.root / rel).parent.mkdir(parents=True, exist_ok=True)
            (self.root / rel).write_text("x\n")

    def scope(self, previous=(block("calc.py:2"),), changed=("pkg/mod.py",), workdir=None):
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

    def test_an_unfixed_previous_item_blocks_wherever_its_file_is(self):
        unfixed = {"basis": "unfixed_previous"}
        self.assertEqual(self.kept(block("calc.py:7", previous_item=1, **unfixed)), ["calc.py:7"])
        # Numbered, but reported at a place the item did not name (here a missing test): still that item.
        self.assertEqual(self.kept(block("tests/test_calc.py:1", previous_item=1, **unfixed)), ["tests/test_calc.py:1"])
        self.assertEqual(self.kept(block("commit history", previous_item=1, **unfixed)), ["commit history"])
        # A number without its label says the same.
        self.assertEqual(self.kept(block("old.py:2", previous_item=1)), ["old.py:2"])
        # Without a usable number the file decides.
        self.assertEqual(self.kept(block("calc.py:7", **unfixed)), ["calc.py:7"])
        self.assertEqual(self.kept(block("calc.py:7", previous_item=4, **unfixed)), ["calc.py:7"])
        self.assertEqual(self.kept(block("old.py:2", previous_item=4, **unfixed)), [])

    def test_one_previous_item_does_not_carry_findings_from_other_files(self):
        unfixed = {"basis": "unfixed_previous", "previous_item": 1}
        for order in (("old.py:2", "calc.py:7"), ("calc.py:7", "old.py:2")):
            with self.subTest(order):
                self.assertEqual(self.kept(*(block(loc, **unfixed) for loc in order)), ["calc.py:7"])
        self.assertEqual(self.kept(block("old.py:2", **unfixed), block("tests/test_calc.py:1", **unfixed)), ["old.py:2"])

    def test_files_the_previous_item_names_in_its_text_count_as_its_files(self):
        previous = block("calc.py:2", fix="add a case to tests/test_calc.py")
        self.assertEqual(self.kept(block("tests/test_calc.py:3", basis="unfixed_previous"), previous=[previous]),
                         ["tests/test_calc.py:3"])

        # A caller it only mentions as the trigger does not.
        self.assertEqual(self.kept(OLD_CODE, previous=[block("calc.py:2", trigger="old.py calls divide")]), [])

    def test_unusable_numbers_and_file_names_do_not_break_a_review(self):
        huge = block("old.py:2", basis="unfixed_previous", previous_item="9" * 5000)
        self.assertEqual(self.kept(huge), [])
        self.assertEqual(self.kept(block("\0 " + "x" * 5000, basis="introduced_since_previous")), [])
        scope = self.scope(changed=[b"caf\xe9.py".decode("utf-8", "surrogateescape"), *(f"f{i}" for i in range(400))])
        scope.render().encode("utf-8")
        self.assertIn("- ... and 101 more", scope.render())

    def test_without_a_basis_the_location_decides(self):
        self.assertEqual(self.kept(block("pkg/mod.py:1"), block("calc.py:2"), OLD_CODE), ["pkg/mod.py:1", "calc.py:2"])
        self.assertEqual(self.kept(block("missing tests"), previous=[block("missing tests")]), ["missing tests"])

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


class TestRoleReviewRounds(SDEDeliveryCase):
    async def run_task(self, *reviews, fixes=(FIX,), **delivery):
        self.set_plan(sde=[dev(BUGGY), *(dev(f) for f in fixes)], reviewer=reviews)
        return await self.executor(delivery=self.delivery(**delivery)).execute([event(self.payload())], None, self.caps)

    async def test_the_first_round_may_block_on_anything(self):
        outcome = await self.run_task(verdict(OLD_CODE), PASS)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(len(self.calls("sde")), 2)
        self.assertIn("old.py:2", self.calls("sde")[1]["prompt"])
        self.assertNotIn("## Incremental review", self.calls("reviewer")[0]["prompt"])

    async def test_an_old_code_finding_in_round_two_becomes_a_note_and_the_review_passes(self):
        late = {**OLD_CODE, "basis": "introduced_since_previous"}
        outcome = await self.run_task(FIRST, verdict(late, notes=["rename divide to  safe_divide", "split calc.py"]))
        assert isinstance(outcome, Emit), outcome
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer", "sde", "reviewer"])
        text = outcome.messages[0]
        for part in ("review=pass after 2 round", "round 2 pass (1 downgraded)", "已降级：",
                     "reviewer verdict: changes_requested"):
            self.assertIn(part, text)
        body = self.gh_state()["prs"][0]["body"]
        section = body.split("## 跟进\n")[1].split("\n\n## ")[0]
        outbox = text.split("跟进：\n")[1]
        for where in (section, outbox):
            for part in ("- rename divide to safe_divide", "- split calc.py", "- old.py:2", "total(None)", "已降级："):
                self.assertIn(part, where)
        self.assertEqual(section.count("safe_divide"), 1)

    async def test_an_unfixed_previous_item_is_sent_back_again(self):
        unfixed = block("calc.py:2", impact="still crashes", basis="unfixed_previous", previous_item=1)
        outcome = await self.run_task(FIRST, verdict(unfixed, OLD_CODE), PASS, fixes=({"readme.txt": "x\n"}, FIX))
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 3 round", outcome.messages[0])
        second_fix = self.calls("sde")[2]["prompt"]
        self.assertIn("still crashes", second_fix)
        # The downgraded item is not part of the fix round.
        self.assertNotIn("old.py:2", second_fix)
        self.assertIn("round 2 changes_requested (1 blocking, 1 downgraded)", outcome.messages[0])
        self.assertIn("old.py:2", self.gh_state()["prs"][0]["body"].split("## 跟进\n")[1])

    async def test_a_problem_the_fix_introduced_is_sent_back(self):
        introduced = block("calc.py:3", impact="wrong exception type", basis="introduced_since_previous")
        outcome = await self.run_task(FIRST, verdict(introduced), PASS, fixes=(FIX, {"calc.py": "x = 1\n"}))
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 3 round", outcome.messages[0])
        self.assertIn("wrong exception type", self.calls("sde")[2]["prompt"])

    async def test_the_second_round_is_told_its_scope_and_still_gets_the_full_diff(self):
        await self.run_task(FIRST, PASS)
        prompt = self.calls("reviewer")[1]["prompt"]
        scope = prompt.split("## Incremental review\n")[1]
        for part in ("unfixed_previous", "introduced_since_previous", "- calc.py\n", "1 blocking item",
                     "rename divide to safe_divide"):
            self.assertIn(part, scope)
        self.assertNotIn("- old.py", scope)
        self.assertIn("old.py", prompt.split("## Code diff")[1].split("## Previous review round")[0])

    async def test_a_recheck_after_a_hand_back_blocks_only_on_changes_since_the_pass(self):
        self.push_main({"calc.py": MAIN_CALC}, message="divide returns b")
        self.set_plan(sde=[dev({"calc.py": TASK_CALC, "old.py": BUGGY["old.py"]}),
                           {**dev({"calc.py": RESOLVED}, commit="merge origin/main"), "run": MERGE_MAIN}],
                      reviewer=[PASS, verdict({**OLD_CODE, "basis": "unfixed_previous", "previous_item": 1})])
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("round 2 pass (1 downgraded)", outcome.messages[0])
        self.assertIn("## Incremental review", self.calls("reviewer")[1]["prompt"])
        self.assertIn("old.py:2", self.gh_state()["prs"][0]["body"].split("## 跟进\n")[1])

    async def test_a_recheck_still_blocks_on_the_conflict_resolution(self):
        self.push_main({"calc.py": MAIN_CALC}, message="divide returns b")
        bad = block("calc.py:3", impact="lost main's change", basis="introduced_since_previous")
        self.set_plan(sde=[dev({"calc.py": TASK_CALC}),
                           {**dev({"calc.py": RESOLVED}, commit="merge origin/main"), "run": MERGE_MAIN},
                           dev({"calc.py": RESOLVED + "# both\n"})],
                      reviewer=[PASS, verdict(bad), PASS])
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("lost main's change", self.calls("sde")[2]["prompt"])

    async def test_new_requirements_after_a_review_are_reviewed_in_full(self):
        # Code that does not meet a requirement a person added since is unchanged code; it must still block.
        self.set_plan(sde=[dev(BUGGY), dev(FIX), dev({"old.py": "def total(xs):\n    return sum(xs or [])\n"})],
                      reviewer=[FIRST, verdict(OLD_CODE), PASS])
        executor = self.executor(delivery=self.delivery())
        executor._options = type(executor._options)(max_review_rounds=1)
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        batch = [event({"text": "total must accept None"}),
                 event({"approval_request_id": outcome.request_id, "approved": True})]
        resumed = await executor.execute(batch, self.resume(outcome), self.caps)
        assert isinstance(resumed, Suspend), resumed
        self.assertIn("old.py:2", resumed.prompt)
        prompt = self.calls("reviewer")[1]["prompt"]
        self.assertIn("## Previous review round", prompt)
        self.assertNotIn("## Incremental review", prompt)
        # With no further requirement the next round is incremental again.
        done = await executor.execute([event({"approval_request_id": resumed.request_id, "approved": True})],
                                      self.resume(resumed), self.caps)
        assert isinstance(done, Emit), done
        self.assertIn("## Incremental review", self.calls("reviewer")[2]["prompt"])

    async def test_rounds_still_run_out_and_wait_for_a_person(self):
        unfixed = block("calc.py:2", basis="unfixed_previous", previous_item=1)
        outcome = await self.run_task(FIRST, verdict(unfixed, OLD_CODE), fixes=({"a.txt": "1\n"}, {"a.txt": "2\n"}))
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("still changes_requested after 3 round(s)", outcome.prompt)
        self.assertEqual(len(self.calls("reviewer")), 3)

    async def test_notes_reach_the_outbox_when_no_pr_is_opened(self):
        for name, delivery in (("no PR", self.delivery(open_pr=False)), ("no delivery step", None)):
            with self.subTest(name):
                self.set_plan(sde=[dev(BUGGY)], reviewer=[verdict(notes=["split calc.py"])])
                outcome = await self.executor(delivery=delivery).execute([event(self.payload())], None, self.caps)
                assert isinstance(outcome, Emit), outcome
                self.assertIn("跟进：\n- split calc.py", outcome.messages[0])
                self.assertEqual(self.gh_state().get("prs", []), [])

    async def test_notes_survive_a_declined_review_retry(self):
        executor = self.executor(delivery=self.delivery())
        self.set_plan(sde=[dev(BUGGY)], reviewer=[FIRST, {"error": "reviewer crashed"}])
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        declined = await executor.execute([event({"approval_request_id": outcome.request_id, "approved": False})],
                                          self.resume(outcome), self.caps)
        assert isinstance(declined, Emit), declined
        self.assertIn("跟进：\n- rename divide to safe_divide", declined.messages[0])
        self.assertIn("## 跟进\n- rename divide to safe_divide", self.gh_state()["prs"][0]["body"])

    async def test_without_notes_there_is_no_follow_up_section(self):
        outcome = await self.run_task(PASS)
        assert isinstance(outcome, Emit), outcome
        self.assertNotIn("跟进", outcome.messages[0])
        self.assertNotIn("## 跟进", self.gh_state()["prs"][0]["body"])

    def test_a_checkpoint_from_before_notes_were_kept_still_loads(self):
        fields = _State(stage="review_pending", workdir="/w", base_ref="", requirements="r").fields()
        for new in ("review_notes", "new_requirements"):
            del fields[new]
        state = _State.load(json.dumps({"sde": fields}))
        assert state is not None
        self.assertEqual((state.review_notes, state.new_requirements), ([], False))


class TestFollowUpSection(DeliveryCase):
    def body(self, *followups):
        wt = self.clone("task-1", "hb/task-1")
        (wt / "calc.py").write_text("x = 1\n")
        git(wt, "commit", "-qam", "work")
        request = DeliveryRequest(
            work_key="task:1", workdir=wt, title="t", summary="s", test_evidence="e", change_kind="fix",
            rationale="r", review="pass", followups=followups)
        self.delivery().deliver(request, permissions=self.allow())
        return self.gh_state()["prs"][0]["body"]

    def allow(self):
        from tests.fakes import InMemoryPermission
        return InMemoryPermission()

    def test_notes_are_one_line_each_and_ping_or_close_nothing(self):
        section = self.body("first\nline", "use @property; fixes #12").split("## 跟进\n")[1].split("\n\n## ")[0]
        self.assertEqual(section, "- first line\n- use @​property; fixes #​12")

    def test_a_long_list_is_cut_at_an_item_and_says_so(self):
        body = self.body(*(f"note {i} " + "x" * 900 for i in range(40)))
        section = body.split("## 跟进\n")[1].split("\n\n## ")[0]
        self.assertLess(len(section), 16_000)
        self.assertRegex(section.splitlines()[-1], r"^- 另有 \d+ 条，见 Hibernaut 完成消息$")
        self.assertIn("## Test evidence", body)


if __name__ == "__main__":
    unittest.main()
