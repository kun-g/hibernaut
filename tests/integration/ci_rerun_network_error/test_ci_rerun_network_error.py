from tests.support.ci_failure import AGAIN, RUN, CIStates, check
from tests.support.pr_delivery import DeliveryCase, git


class TestCIRerunNetworkError(CIStates, DeliveryCase):
    def test_a_network_error_on_re_run_is_retried_and_keeps_the_re_run(self):
        # Review of PR #134: one dropped connection used up the head's only re-run and sent a flaky failure back.
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.ci_fails(after_rerun=[[check("pending", AGAIN)], [check("pass", AGAIN)]],
                      rerun_error="error connecting to api.github.com\n")
        rec = self.deliver(wt)
        self.assertEqual((rec.state, rec.handback, rec.merge_hold), ("pr_open", {}, ""))
        self.assertTrue(rec.transient, rec.problem)
        self.assertIn("error connecting to api.github.com", rec.problem)
        self.assertEqual(rec.problem_summary, "重跑失败的 CI 检查时，远端服务暂时不可用")
        self.assertNotIn("reruns", self.gh_state())
        # The retry the transient error schedules re-runs the same head once, as if nothing had happened.
        self.set_gh(rerun_error="", checks_sequence=[[check("fail")]])
        again = self.deliver(wt)
        self.assertEqual((again.state, again.problem), ("merged", ""))
        self.assertEqual(self.gh_state()["reruns"], [RUN])
        self.assertEqual(again.merge_sha, git(self.bare, "rev-parse", "main"))
