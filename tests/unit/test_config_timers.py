import pytest
from agent_runtime.app.config import parse_config


PROJECT = {"repo": "fixture/p", "checkout": "/tmp/p", "inbox_id": "proj:p"}
TIMER = {"name": "health", "inbox_id": "proj:p", "interval_seconds": 60}


@pytest.mark.parametrize("declarations", [
    {}, [{}], [TIMER, TIMER], [dict(TIMER, name=" ")], [dict(TIMER, inbox_id="unknown")],
    [dict(TIMER, interval_seconds=True)], [dict(TIMER, interval_seconds=0)],
    [dict(TIMER, interval_seconds=0.5)], [dict(TIMER, payload=[])], [dict(TIMER, enabled="false")],
    [dict(TIMER, payload={"date": object()})], [dict(TIMER, cron="*")],
])
def test_invalid_declarations_fail_at_load(declarations):
    with pytest.raises(ValueError, match="timers"):
        parse_config({"projects": {"p": PROJECT}, "timers": declarations})


def test_system_tools_target_is_refused_even_when_disabled(tmp_path):
    with pytest.raises(ValueError, match=r"timers.*system_tools"):
        parse_config({"executors": {"ops": {"role": "steward"}},
                      "runtime": {"inbox_bindings": [{"pattern": "sys:ops", "workdir": str(tmp_path),
                          "checkout_access": "readonly", "system_tools": True, "default_agent": "ops"}]},
                      "timers": [dict(TIMER, inbox_id="sys:ops", enabled=False)]})
