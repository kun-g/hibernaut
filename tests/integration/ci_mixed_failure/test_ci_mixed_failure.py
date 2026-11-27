from tests.support.ci_failure import AGAIN, FIRST, RUN, LOG, CIStates, check
from tests.support.pr_delivery import DeliveryCase

EXTERNAL = "https://ci.example/build/7"


class TestCIMixedFailure(CIStates, DeliveryCase):
    def test_a_failed_check_a_re_run_cannot_repeat_goes_back_at_once(self):
        # Review of PR #134: gates re-ran and passed, but the external check and the check without a link kept their
        # old links, were taken as re-run, and counted as pending until the CI wait timed out instead of going back.
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        failed = [check("fail"), check("fail", EXTERNAL, name="external"), check("fail", "", name="status")]
        later = [check("pass", AGAIN), check("fail", EXTERNAL, name="external"), check("fail", "", name="status")]
        self.set_gh(checks_sequence=[failed], after_rerun=[later], checks=later, run_logs={RUN: LOG})
        rec = self.deliver(wt)
        self.assertEqual((rec.state, rec.merge_hold, rec.problem_summary),
                         ("pr_open", "ci", "合并前的 CI 检查未通过"))
        self.assertEqual(rec.handback["kind"], "ci")
        self.assertEqual(rec.handback["checks"], [{"name": "gates", "link": FIRST},
                                                  {"name": "external", "link": EXTERNAL},
                                                  {"name": "status", "link": ""}])
        self.assertEqual(rec.handback["rerun"], "skipped")
        self.assertIn("external", rec.handback["rerun_note"])
        self.assertEqual(self.gh_calls("rerun"), [])
        self.assertEqual(self.sleeps, [])
