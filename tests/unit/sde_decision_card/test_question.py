import json

import pytest

from agent_runtime.adapters.executor_cli.peer_handoff import parse_result
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA, _validate_report
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError
from agent_runtime.core.models import Suspend
from switchboard.feishu_cards import render_approval_card
from tests.support.input_decision import DECISION
from tests.support.review_flow_support import REPORT, ReviewFlowCase


@pytest.mark.parametrize("field", DECISION)
@pytest.mark.parametrize("value", [None, "   "])
def test_incomplete_decision_is_returned_to_the_developer(field, value):
    decision = dict(DECISION)
    if value is None:
        del decision[field]
    else:
        decision[field] = value
    report = {**REPORT, "status": "needs_input", "question": "要扩大权限吗？", "decision": decision}
    message = json.dumps({**report, "task_id": "t", "attempt_id": "a"})
    accepted, error = parse_result(message, SDE_SCHEMA, task_id="t", attempt_id="a")
    assert accepted is None and field in error
    with pytest.raises(ClaudeCodeError, match=field):
        _validate_report(report)


def test_question_only_receipt_is_not_accepted_as_a_decision():
    report = {**REPORT, "status": "needs_input", "question": "要扩大权限吗？"}
    accepted, error = parse_result(json.dumps({**report, "task_id": "t", "attempt_id": "a"}),
                                   SDE_SCHEMA, task_id="t", attempt_id="a")
    assert accepted is None and "result.decision is required" in error
    with pytest.raises(ClaudeCodeError, match="decision is required"):
        _validate_report(report)


class TestDecisionCard(ReviewFlowCase):
    async def test_complete_report_renders_only_the_decision_and_keeps_the_button_route(self):
        self.state.title = "升级文件读取权限"
        self.state.work_key = "proxy-upgrade-20261127"
        self.state.handback_log = ["Delivery hand-back 1 of 2"]
        report = {**REPORT, "status": "needs_input", "question": "要扩大权限吗？", "decision": DECISION}
        outcome = await self.run_flow(snapshots=[], diffs={}, reports=[report])
        assert isinstance(outcome, Suspend)
        card = render_approval_card(title="待审批", body_md=outcome.prompt, approval_request_id=outcome.request_id,
                                    correlation_key=self.state.work_key, scope="proj:test", session_id="session")
        body, buttons = card["elements"]
        assert body["content"].splitlines() == [
            "任务「升级文件读取权限」", "背景：" + DECISION["background"], "为什么需要你决定：" + DECISION["reason"],
            "问题：要扩大权限吗？", "建议：拒绝", "如有补充，请先回复任务，再点批准。",
            "批准 = " + DECISION["approve"] + "，开发继续。", "拒绝 = " + DECISION["reject"] + "，任务停止。",
        ]
        assert [b["value"]["correlation_key"] for b in buttons["actions"]] == [self.state.work_key] * 2
