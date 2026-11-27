import json

import pytest

from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from switchboard.addressing import GITHUB_OUTBOUND_MARKER
from tests.support.switchboard_support import make_harness


@pytest.fixture
def harness():
    harness = make_harness(start_http=False)
    try:
        yield harness
    finally:
        harness.close()


@pytest.mark.parametrize("metadata", [{}, {"title": "状态"}], ids=["untitled", "titled"])
@pytest.mark.parametrize("channel", ["feishu", "github"])
def test_reply_contains_only_body(harness, channel, metadata):
    key = "feishu:p2p:oc_chat" if channel == "feishu" else "github:example/project#42"
    report = SwitchboardChannelAdapter(harness.board.bus.publish).build_report(
        "user:main", "正文", {"session_id": "s", "correlation_key": key, "outcome_kind": "emit", **metadata}
    )
    assert report is not None and report.subject.strip()  # Required by older Switchboards.
    assert harness.board.bus.publish(report).status == "accepted"
    harness.board.worker.run_once()
    assert harness.board.gateway.run_once() == 1
    payload = harness.http.calls[0]["json"]
    if channel == "feishu":
        assert json.loads(payload["content"])["text"] == "正文"
    else:
        assert payload["body"] == f"正文\n\n{GITHUB_OUTBOUND_MARKER}"


@pytest.mark.parametrize(("metadata", "expected"), [({}, "待审批"), ({"title": "批准交付"}, "批准交付")])
def test_approval_card_keeps_header(harness, metadata, expected):
    report = SwitchboardChannelAdapter(harness.board.bus.publish).build_report("user:main", "继续？", {
        "session_id": "s", "outcome_kind": "suspend", "approval_request_id": "req", **metadata,
    })
    assert report is not None
    assert harness.board.bus.publish(report).status == "accepted"
    harness.board.worker.run_once()
    assert harness.board.gateway.run_once() == 1
    card = json.loads(harness.http.calls[0]["json"]["content"])
    assert card["header"]["title"]["content"] == expected
