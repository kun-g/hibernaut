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


@pytest.mark.parametrize(("metadata", "title"), [
    ({}, None),
    ({"title": "状态"}, "状态"),
    ({"subject": "Hibernaut update"}, "Hibernaut update"),
    ({"title": "  ", "subject": "状态"}, "状态"),
], ids=["untitled", "titled", "literal-fallback-title", "blank-title-with-subject"])
@pytest.mark.parametrize("channel", ["feishu", "github"])
def test_reply_title(harness, channel, metadata, title):
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
        assert json.loads(payload["content"])["text"] == (f"{title}\n正文" if title else "正文")
    else:
        text = f"**{title}**\n\n正文" if title else "正文"
        assert payload["body"] == f"{text}\n\n{GITHUB_OUTBOUND_MARKER}"


def test_untitled_approval_card_names_the_action(harness):
    report = SwitchboardChannelAdapter(harness.board.bus.publish).build_report("user:main", "继续？", {
        "session_id": "s", "outcome_kind": "suspend", "approval_request_id": "req",
    })
    assert report is not None
    assert harness.board.bus.publish(report).status == "accepted"
    harness.board.worker.run_once()
    assert harness.board.gateway.run_once() == 1
    card = json.loads(harness.http.calls[0]["json"]["content"])
    assert card["header"]["title"]["content"] == "待审批"
