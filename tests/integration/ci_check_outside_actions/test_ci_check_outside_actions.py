from tests.support.ci_failure import CIStates, check
from tests.support.pr_delivery import DeliveryCase


class TestCICheckOutsideActions(CIStates, DeliveryCase):
    def test_a_check_outside_github_actions_is_not_re_run(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.set_gh(checks=[check("fail", "https://ci.example/build/7", name="external")])
        rec = self.deliver(wt)
        self.assertEqual((rec.handback["kind"], rec.handback["rerun"]), ("ci", "skipped"))
        self.assertEqual(rec.handback["checks"], [{"name": "external", "link": "https://ci.example/build/7"}])
        self.assertEqual(self.gh_calls("rerun"), [])
