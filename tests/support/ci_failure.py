import json

from tests.support.fakes import InMemoryPermission

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
    # Mixed into a DeliveryCase or SDEDeliveryCase: GitHub's checks and re-runs as the fake gh reports them.
    def set_gh(self, **values):
        state = self.gh_state()
        state.update(values)
        self.state_file.write_text(json.dumps(state))

    def ci_fails(self, *, after_rerun, rerun_error=""):
        self.set_gh(checks_sequence=[[check("fail")]], after_rerun=after_rerun, run_logs={RUN: LOG},
                    **({"rerun_error": rerun_error} if rerun_error else {}))

    def deliver(self, wt):
        return self.delivery(auto_merge=True).deliver(self.request(wt), permissions=InMemoryPermission())
