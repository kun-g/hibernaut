from tests.support.ci_failure import CIStates, check
from tests.support.pr_delivery import DeliveryCase


class TestCIRerunNoPermission(CIStates, DeliveryCase):
    def test_without_permission_to_re_run_it_goes_straight_back(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.ci_fails(after_rerun=[[check("pass")]],
                      rerun_error="HTTP 403: Must have admin rights to Repository. (https://api.github.com/...)\n")
        rec = self.deliver(wt)
        self.assertEqual((rec.state, rec.handback["kind"], rec.handback["rerun"]), ("pr_open", "ci", "skipped"))
        self.assertIn("no permission to re-run", rec.handback["rerun_note"])
        self.assertTrue(any("no permission to re-run" in h for h in rec.history), rec.history)
        self.assertNotIn("reruns", self.gh_state())
        self.assertEqual(len(self.gh_calls("checks")), 1)
        self.assertEqual(self.sleeps, [])
