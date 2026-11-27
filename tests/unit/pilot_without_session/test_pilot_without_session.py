# ExecutorPort.execute may be called without a session; the pilot's executors answer as RunnerExecutorBridge does.
import asyncio

from agent_runtime.adapters.runner_pilot import ClosedRouteExecutor, ProtocolCheckExecutor
from agent_runtime.core.models import Emit, Event
from tests.support.runner_pilot import KEY, OTHER, PILOT
from tests.unit.test_runner_pilot import COMMIT, _Caps, _FakeRunner, _result


def test_a_check_without_a_session_runs_in_a_new_one_of_its_inbox() -> None:
    runner = _FakeRunner(_result)
    executor = ProtocolCheckExecutor(runner=runner, inbox_id=PILOT, project_key="hibernaut", timeout_seconds=9.0,
                                     node_wait_seconds=0)
    events = [Event(inbox_id=PILOT, source="github", external_id="1", payload={"candidate_commit": COMMIT},
                    correlation_key=KEY)]
    outcome = asyncio.run(executor.execute(events, None, _Caps()))
    assert isinstance(outcome, Emit) and outcome.status != "failed" and outcome.close_session
    (req,) = runner.requests
    assert outcome.session_id is not None and outcome.session_id.startswith("sess")
    assert (req.owner_scope, req.work_key) == (PILOT, outcome.session_id)


def test_the_closed_route_without_a_session_still_runs_nothing() -> None:
    events = [Event(inbox_id=OTHER, source="github", external_id="1", payload={}, correlation_key=KEY)]
    outcome = asyncio.run(ClosedRouteExecutor().execute(events, None, _Caps()))
    assert isinstance(outcome, Emit) and (outcome.status, outcome.close_session) == ("failed", True)
