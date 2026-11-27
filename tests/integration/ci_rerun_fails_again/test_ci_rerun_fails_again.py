from tests.support.ci_failure import AGAIN, RUN, TEST, CIStates, check
from tests.support.pr_delivery import DeliveryCase


class TestCIRerunFailsAgain(CIStates, DeliveryCase):
    def test_a_failure_that_fails_again_when_re_run_goes_back_to_the_developer(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.ci_fails(after_rerun=[[check("fail", AGAIN)]])
        rec = self.deliver(wt)
        self.assertEqual((rec.state, rec.merge_hold), ("pr_open", "ci"))
        self.assertEqual(self.gh_calls("merge"), [])
        self.assertEqual(self.gh_state()["reruns"], [RUN])
        self.assertIn("CI failed", rec.problem)
        self.assertEqual(rec.problem_summary, "合并前的 CI 检查未通过")
        self.assertEqual(rec.handback["kind"], "ci")
        self.assertEqual(rec.handback["head"], rec.head_sha)
        self.assertEqual(rec.handback["checks"], [{"name": "gates", "link": AGAIN}])
        self.assertEqual(rec.handback["tests"], [TEST])
        self.assertEqual(rec.handback["rerun"], "done")
        self.assertIn("JSONDecodeError: Unterminated string", rec.handback["failed"])
        self.assertNotIn("\t", rec.handback["failed"])
        # The same head is not re-run a second time when the delivery is tried again.
        self.set_gh(checks=[check("fail", AGAIN)])
        again = self.deliver(wt)
        self.assertEqual((again.handback["kind"], self.gh_state()["reruns"]), ("ci", [RUN]))
