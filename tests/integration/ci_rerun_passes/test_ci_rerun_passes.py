from tests.support.ci_failure import AGAIN, RUN, CIStates, check
from tests.support.pr_delivery import DeliveryCase, git


class TestCIRerunPasses(CIStates, DeliveryCase):
    def test_a_failure_that_passes_when_re_run_once_is_merged(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        # Until GitHub registers the re-run it still shows the old failed job; that one is not read as a new failure.
        self.ci_fails(after_rerun=[[check("fail")], [check("pending", AGAIN)], [check("pass", AGAIN)]])
        rec = self.deliver(wt)
        self.assertEqual((rec.state, rec.problem), ("merged", ""))
        self.assertEqual(self.gh_state()["reruns"], [RUN])
        rerun = self.gh_calls("rerun")
        self.assertEqual(rerun, [["run", "rerun", RUN, "--failed", "-R", "o/r"]])
        self.assertTrue(any(f"re-ran the failed jobs of run {RUN} once" in h for h in rec.history), rec.history)
        self.assertEqual(rec.merge_sha, git(self.bare, "rev-parse", "main"))
