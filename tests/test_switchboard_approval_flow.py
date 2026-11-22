from __future__ import annotations

import json
import unittest
from typing import Any

from switchboard.envelope import CanonicalEvent
from tests.switchboard_support import FEISHU_TOKEN, Harness, issue_payload, make_harness, post, send_github


def card_click(value: dict[str, Any], operator: str = "ou_kun") -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {"event_type": "card.action.trigger", "token": FEISHU_TOKEN},
        "event": {
            "operator": {"open_id": operator},
            "action": {"tag": "button", "value": value},
            "context": {"open_message_id": "om_card", "open_chat_id": "oc_approvals"},
        },
    }


class CrossSourceApprovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness()
        self.hibernaut: list[CanonicalEvent] = []
        self.h.board.bus.subscribe("hibernaut", lambda e: self.hibernaut.append(e) or None)
        self.h.board.bus.subscribe("taskboard", lambda e: None)

    def tearDown(self) -> None:
        self.h.close()

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
