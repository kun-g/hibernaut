from agent_runtime.core.models import Emit
from tests.support.ci_failure import AGAIN, TEST, CIStates, check
from tests.support.pr_delivery import git
from tests.support.sde_delivery import SDEDeliveryCase, dev, event


class TestCIHandBack(CIStates, SDEDeliveryCase):
    async def test_a_ci_failure_goes_back_to_the_same_developer_and_delivery_continues(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"}), dev({"calc.py": "x = 2\n"}, commit="make the race certain")])
        self.ci_fails(after_rerun=[[check("fail", AGAIN)]])
        executor = self.executor(review=False, delivery=self.delivery(auto_merge=True, require_review=False))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        self.assertIn("Delivery: merged", text)
        self.assertIn("Delivery hand-back 1 of 2: CI failed on", text)
        sde = self.calls("sde")
        self.assertEqual(len(sde), 2)
        # One session: the hand-back resumes the developer's conversation.
        self.assertIn("--resume", sde[1]["argv"])
        handback = sde[1]["prompt"]
        for part in ("## Delivery handed back: CI failed", "gates", AGAIN, TEST, "JSONDecodeError",
                     "re-ran the failed jobs once", "change_kind"):
            self.assertIn(part, handback)
        self.assertEqual(git(self.bare, "show", "main:calc.py"), "x = 2")
