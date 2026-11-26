from __future__ import annotations

import json
import unittest

from switchboard.envelope import CanonicalEvent
from tests.support.switchboard_support import (
    Harness,
    issue_payload,
    make_harness,
    mcp_call,
    post,
    send_github,
    tool_result,
)
from tests.support.switchboard_inbound import feishu_message
from tests.support.switchboard_approval_flow import card_click


class CrossSourceApprovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness(routing={"feishu_chats": {"oc_approvals": "proj:chat"}})
        self.hibernaut: list[CanonicalEvent] = []
        self.h.board.bus.subscribe("hibernaut", lambda e: self.hibernaut.append(e) or None)
        self.h.board.bus.subscribe("taskboard", lambda e: None)

    def tearDown(self) -> None:
        self.h.close()

    def test_originless_mcp_suspend_uses_the_sent_card_as_its_return_route(self) -> None:
        board = self.h.board
        scope = "proj:local"
        _, rpc = mcp_call(self.h.url, "report_execution", {
            "external_id": "outbox:local", "scope": scope, "subject": "Continue?",
            "metadata": {"run_id": "run", "outcome_kind": "suspend",
                         "session_id": "session-local", "approval_request_id": "req-local"},
        })
        self.assertEqual(tool_result(rpc)["status"], "accepted")
        board.worker.run_once()
        board.gateway.run_once()
        (send,) = self.h.http.calls
        self.assertEqual(send["json"]["receive_id"], "oc_approvals")
        card = json.loads(send["json"]["content"])
        value = next(el for el in card["elements"] if el["tag"] == "action")["actions"][1]["value"]
        url = f"{self.h.url}/webhooks/feishu"
        for field, wrong in (("session_id", "other"), ("approval_request_id", "other"), ("decision", "unknown")):
            with self.subTest(field=field):
                self.assertEqual(post(url, card_click({**value, field: wrong}))[0], 400)
        for field in ("open_chat_id", "open_message_id"):
            with self.subTest(field=field):
                click = card_click(value)
                click["event"]["context"][field] = "other"
                self.assertEqual(post(url, click)[0], 400)
        self.assertEqual(post(url, card_click(value, operator=""))[0], 400)
        click = card_click(value)
        click["header"]["token"] = "invalid"
        self.assertEqual(post(url, click)[0], 401)
        self.assertEqual(board.store.count_inbound(), 1)

        status, result = post(url, card_click({**value, "scope": "other", "correlation_key": "other"}))
        self.assertEqual((status, result["toast"]["type"]), (200, "success"))
        board.worker.run_once()
        (decision,) = self.hibernaut
        self.assertEqual((decision.scope, decision.correlation_key, decision.actor), (scope, None, "ou_kun"))
        self.assertEqual((decision.metadata["session_id"], decision.metadata["approval_request_id"],
                          decision.metadata["decision"]), ("session-local", "req-local", "reject"))
        _, posted = post(url, feishu_message(chat_id="oc_approvals"))
        message = board.store.get_inbound(posted["event_id"])
        assert message is not None
        self.assertEqual((message.scope, message.correlation_key), ("proj:chat", "feishu:p2p:oc_approvals"))

    def test_github_suspend_to_feishu_card_and_single_decision(self) -> None:
        board = self.h.board
        _, opened = send_github(self.h.url, issue_payload())
        work = board.store.get_inbound(opened["event_id"])
        assert work is not None
        key = work.correlation_key
        self.assertEqual(key, "github:kun-g/hibernaut#42")

        report = CanonicalEvent(
            source="hibernaut",
            event_type="execution.reported",
            external_id="run:r1:suspend",
            scope=work.scope,
            correlation_key=key,
            subject="Need approval to push",
            body="Push branch `hb/task-42`?",
            metadata={
                "run_id": "r1",
                "session_id": "s1",
                "outcome_kind": "suspend",
                "approval_request_id": "apr_1",
            },
        )
        published = board.bus.publish(report)
        self.assertEqual(published.consumers, ("taskboard", "switchboard.outbound"))
        board.worker.run_once()
        outbound_row = board.store.get_delivery(report.event_id, "switchboard.outbound")
        assert outbound_row is not None
        self.assertEqual(outbound_row.status, "acked")

        board.gateway.run_once()
        self.assertEqual(len(self.h.http.calls), 1)
        call = self.h.http.calls[0]
        self.assertEqual(call["headers"]["Authorization"], "Bearer ephemeral-feishu")
        self.assertEqual(call["json"]["receive_id"], "oc_approvals")
        self.assertEqual(call["json"]["msg_type"], "interactive")
        card = json.loads(call["json"]["content"])
        buttons = [a for el in card["elements"] if el["tag"] == "action" for a in el["actions"]]
        self.assertEqual([b["value"]["decision"] for b in buttons], ["approve", "reject"])
        approve_value = buttons[0]["value"]
        self.assertEqual(
            approve_value,
            {
                "approval_request_id": "apr_1",
                "decision": "approve",
                "correlation_key": "github:kun-g/hibernaut#42",
                "scope": "proj:hibernaut",
                "session_id": "s1",
            },
        )
        sent = board.store.list_outbound()[0]
        self.assertEqual((sent.status, sent.platform_msg_id), ("sent", "om_out"))

        status, response = post(f"{self.h.url}/webhooks/feishu", card_click(approve_value))
        self.assertEqual(status, 200)
        resolved = response["card"]["data"]
        self.assertEqual(response["card"]["type"], "raw")
        self.assertIn("已批准", resolved["header"]["title"]["content"])
        self.assertFalse(any(el["tag"] == "action" for el in resolved["elements"]))

        decided = board.store.find_inbound("feishu", "approval:apr_1")
        assert decided is not None
        self.assertEqual(decided.event_type, "approval.decided")
        self.assertEqual(decided.correlation_key, "github:kun-g/hibernaut#42")
        self.assertEqual(decided.scope, "proj:hibernaut")
        self.assertEqual(decided.metadata["session_id"], "s1")
        self.assertEqual(decided.actor, "ou_kun")

        before = board.store.count_inbound()
        reject_value = buttons[1]["value"]
        status, second = post(f"{self.h.url}/webhooks/feishu", card_click(reject_value, operator="ou_other"))
        self.assertEqual(status, 200)
        self.assertEqual(board.store.count_inbound(), before)
        self.assertIn("已处理", second["toast"]["content"])
        self.assertIn("已批准", second["card"]["data"]["header"]["title"]["content"])

        board.worker.run_once()
        decisions = [e for e in self.hibernaut if e.event_type == "approval.decided"]
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].metadata["decision"], "approve")


if __name__ == "__main__":
    unittest.main()
