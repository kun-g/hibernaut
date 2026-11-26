import unittest

from agent_runtime.core.models import Emit
from tests.support.sde_delivery import SDEDeliveryCase, dev, event
from tests.support.incremental_review import FIRST, OLD_CODE, verdict


BUGGY = {"calc.py": "def divide(a, b):\n    return a / b\n", "old.py": "def total(xs):\n    return sum(xs)\n"}
FIX = {"calc.py": "def divide(a, b):\n    if b == 0:\n        raise ValueError('b')\n    return a / b\n"}


class TestDeliveredReviewNotes(SDEDeliveryCase):
    async def run_task(self, *reviews, fixes=(FIX,), **delivery):
        self.set_plan(sde=[dev(BUGGY), *(dev(f) for f in fixes)], reviewer=reviews)
        return await self.executor(delivery=self.delivery(**delivery)).execute([event(self.payload())], None, self.caps)

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

    async def test_notes_reach_the_outbox_when_no_pr_is_opened(self):
        for name, delivery in (("no PR", self.delivery(open_pr=False)), ("no delivery step", None)):
            with self.subTest(name):
                self.set_plan(sde=[dev(BUGGY)], reviewer=[verdict(notes=["split calc.py"])])
                outcome = await self.executor(delivery=delivery).execute([event(self.payload())], None, self.caps)
                assert isinstance(outcome, Emit), outcome
                self.assertIn("跟进：\n- split calc.py", outcome.messages[0])
                self.assertEqual(self.gh_state().get("prs", []), [])


if __name__ == "__main__":
    unittest.main()
