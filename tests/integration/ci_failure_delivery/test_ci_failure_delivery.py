import json

from agent_runtime.core.models import Emit, Suspend
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import DeliveryCase, git
from tests.support.sde_delivery import SDEDeliveryCase, dev, event

RUN = "46007850893"
FIRST = f"https://github.com/o/r/actions/runs/{RUN}/job/1"
AGAIN = f"https://github.com/o/r/actions/runs/{RUN}/job/2"
TEST = "tests/integration/test_x.py::EndToEndTests::test_c5_race"
LOG = (f"gates\tRun gates\t2026-11-27T13:33:01.1Z [gw1] [ 37%] FAILED {TEST}\n"
       "gates\tRun gates\t2026-11-27T13:33:01.2Z =================================== FAILURES ====\n"
       "gates\tRun gates\t2026-11-27T13:33:01.3Z E   json.decoder.JSONDecodeError: Unterminated string\n"
       f"gates\tRun gates\t2026-11-27T13:33:01.4Z FAILED {TEST} - json.decoder.JSONDecodeError: Unterminated\n"
       "gates\tRun gates\t2026-11-27T13:33:01.5Z ======= 1 failed, 2546 passed in 79.67s =======\n")


def check(bucket, link=FIRST, name="gates"):
    state = {"pass": "SUCCESS", "fail": "FAILURE", "pending": "IN_PROGRESS"}[bucket]
    return {"name": name, "state": state, "bucket": bucket, "link": link}


class CIStates:
    def set_gh(self, **values):
        state = self.gh_state()
        state.update(values)
        self.state_file.write_text(json.dumps(state))

    def ci_fails(self, *, after_rerun, rerun_error=""):
        self.set_gh(checks_sequence=[[check("fail")]], after_rerun=after_rerun, run_logs={RUN: LOG},
                    **({"rerun_error": rerun_error} if rerun_error else {}))


class TestCIFailureDelivery(CIStates, DeliveryCase):
    def deliver(self, wt):
        return self.delivery(auto_merge=True).deliver(self.request(wt), permissions=InMemoryPermission())

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

    def test_a_check_outside_github_actions_is_not_re_run(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.set_gh(checks=[check("fail", "https://ci.example/build/7", name="external")])
        rec = self.deliver(wt)
        self.assertEqual((rec.handback["kind"], rec.handback["rerun"]), ("ci", "skipped"))
        self.assertEqual(rec.handback["checks"], [{"name": "external", "link": "https://ci.example/build/7"}])
        self.assertEqual(self.gh_calls("rerun"), [])


class TestCIFailureHandBack(CIStates, SDEDeliveryCase):
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

    async def test_ci_hand_backs_stop_at_the_limit_with_an_approval_card(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        self.ci_fails(after_rerun=[[check("fail", AGAIN)]])
        executor = self.executor(review=False, delivery=self.delivery(auto_merge=True, require_review=False, max_handbacks=0))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        card = outcome.prompt
        for part in ("（PR #1）：合并前的 CI 检查未通过。", "批准：", "拒绝：", "失败的检查：gates", f"日志：{AGAIN}",
                     f"失败的用例：{TEST}", "重跑：已在同一提交上重跑失败的检查一次，仍失败",
                     "已交回开发 0 次（上限 0）"):
            self.assertIn(part, card)
        self.assertEqual(len(self.calls("sde")), 1)
