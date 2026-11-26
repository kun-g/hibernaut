from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter, SwitchboardInboundBridge
from agent_runtime.adapters.channels.switchboard_relay import SwitchboardRelayLink
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.app.bootstrap import build_runtime
from agent_runtime.core.models import Emit, SessionRef, Suspend
from switchboard.credentials import EnvCredentialBroker
from switchboard.http_client import HttpResponse
from tests.support.fakes import RecordingChannel
from tests.support.switchboard_support import MCP_TOKEN, RELAY_TOKEN, make_harness, post
from tests.support.switchboard_approval_flow import card_click

SCOPE = "proj:local-approval"
CHAT = "oc_local_approvals"


class KeylessRelayApprovalTests(unittest.IsolatedAsyncioTestCase):
    def start_stack(self, target: str | None) -> None:
        self.h = make_harness(relay={"enabled": True, "transport": "relay"}, outbound={"approval_target": target})
        self.addCleanup(self.h.close)
        board = self.h.board
        assert board.relay is not None
        # Drive delivery and outbound sends one round at a time while the HTTP and WebSocket listeners run.
        board.relay.start()
        self.addCleanup(board.relay.stop)
        self.addCleanup(board.worker.stop)
        root = Path(self.h.tmp.name)
        link = SwitchboardRelayLink(
            relay_url=board.relay_url, mcp_url=f"{self.h.url}/mcp", consumer_id="hibernaut",
            credentials=EnvCredentialBroker({"SWITCHBOARD_MCP_TOKEN": MCP_TOKEN, "SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN}),
        )
        self.fallback = RecordingChannel()
        self.app = build_runtime(db_path=root / "runtime.db", memory_dir=root / "memory",
                                 channel=SwitchboardChannelAdapter(link.publish, fallback=self.fallback))
        self.addCleanup(self.app.close)
        self.sessions = SQLiteSessionStoreAdapter(self.app.db)
        link.subscribe("hibernaut", SwitchboardInboundBridge(self.app.handler).handle_canonical_event)
        self.addCleanup(link.stop)
        self.assertTrue(link.agent.connected.wait(5), "Hibernaut did not connect to the local relay")

    async def suspend(self, name: str) -> SessionRef:
        # The demo executor's send_email tool only formats a string. Its permission check creates a real checkpoint.
        self.app.handler.ingest(inbox_id=SCOPE, source="local", external_id=name,
                                payload={"action": "send_email", "to": f"{name}@example.invalid"})
        (run,) = await self.app.worker.process_inbox(SCOPE)
        assert isinstance(run.outcome, Suspend)
        session = self.sessions.find_by_request_id(run.outcome.request_id)
        assert session is not None
        self.assertEqual((session.session_id, session.status, session.correlation_key), (run.session_id, "suspended", None))
        self.assertEqual(await self.app.dispatcher.dispatch_once(), 1)
        self.assertEqual(self.fallback.sent, [])
        self.h.board.worker.run_once()
        return session

    def send_card(self, session: SessionRef, message_id: str) -> list[dict[str, Any]]:
        self.h.http.responses.append(HttpResponse(200, json.dumps({"code": 0, "data": {"message_id": message_id}}).encode()))
        self.assertEqual(self.h.board.gateway.run_once(), 1)
        call = self.h.http.calls[-1]
        self.assertEqual((call["method"], call["json"]["receive_id"], call["json"]["msg_type"]), ("POST", CHAT, "interactive"))
        card = json.loads(call["json"]["content"])
        values = [button["value"] for el in card["elements"] if el["tag"] == "action" for button in el["actions"]]
        self.assertEqual({v["decision"] for v in values}, {"approve", "reject"})
        for value in values:
            self.assertEqual((value["scope"], value["session_id"], value["approval_request_id"]),
                             (SCOPE, session.session_id, session.pending_request_id))
            self.assertFalse(value.get("correlation_key"))
        return values

    async def click(self, value: dict[str, Any], message_id: str, session: SessionRef) -> str:
        click = card_click(value)
        click["event"]["context"] = {"open_message_id": message_id, "open_chat_id": CHAT}
        status, response = post(f"{self.h.url}/webhooks/feishu", click)
        self.assertEqual(status, 200, response)
        board = self.h.board
        decided = board.store.find_inbound("feishu", f"approval:{session.pending_request_id}")
        assert decided is not None, response
        self.assertEqual((decided.event_type, decided.correlation_key, decided.metadata["session_id"]),
                         ("approval.decided", None, session.session_id))
        board.worker.run_once()
        delivery = board.store.get_delivery(decided.event_id, "hibernaut")
        assert delivery is not None
        self.assertEqual(delivery.status, "acked")
        (run,) = await self.app.worker.process_inbox(SCOPE)
        self.assertEqual(run.session_id, session.session_id)
        assert isinstance(run.outcome, Emit)
        resumed = self.sessions.get(session.session_id)
        assert resumed is not None
        self.assertEqual((resumed.status, resumed.pending_request_id, resumed.checkpoint), ("active", None, None))
        return run.outcome.messages[0]

    async def test_keyless_card_clicks_resume_their_own_checkpoint(self) -> None:
        self.start_stack(CHAT)
        first = await self.suspend("first")
        first_buttons = self.send_card(first, "om_first")
        second = await self.suspend("second")
        second_buttons = self.send_card(second, "om_second")
        self.assertNotEqual(first.session_id, second.session_id)
        self.assertNotEqual(first.pending_request_id, second.pending_request_id)

        approve = next(v for v in first_buttons if v["decision"] == "approve")
        approve["correlation_key"] = ""
        result = await self.click(approve, "om_first", first)
        self.assertIn("[APPROVED]", result)
        self.assertIn("first@example.invalid", result)
        self.assertEqual(self.sessions.get(second.session_id), second)

        reject = next(v for v in second_buttons if v["decision"] == "reject")
        reject.pop("correlation_key", None)
        self.assertIn("[REJECTED BY USER]", await self.click(reject, "om_second", second))

    async def test_worker_checks_pending_request_scope_and_session(self) -> None:
        self.start_stack(None)
        session = await self.suspend("pending")
        for scope, sid, request in (
            ("proj:other", session.session_id, session.pending_request_id),
            (SCOPE, "other-session", session.pending_request_id),
            (SCOPE, session.session_id, "stale-request"),
        ):
            result = self.app.handler.ingest(
                inbox_id=scope, source="sb:feishu", session_id=sid, external_id=f"{scope}:{sid}:{request}",
                payload={"event_type": "approval.decided", "approval_request_id": request, "approved": True},
            )
            self.assertEqual(await self.app.worker.process_inbox(scope), [])
            self.assertEqual(self.sessions.get(session.session_id), session)
            row = self.app.db.fetchone("SELECT status FROM event_inbox WHERE event_id = ?", (result.event_id,))
            self.assertEqual(row["status"], "failed")

    async def test_missing_target_retains_suspend_report_without_sending_a_card(self) -> None:
        self.start_stack(None)
        session = await self.suspend("unrouted")
        board = self.h.board
        (delivery,) = [d for d in board.store.list_deliveries() if d.consumer_id == "switchboard.outbound"]
        report = board.store.get_inbound(delivery.event_id)
        assert report is not None
        self.assertEqual((delivery.status, report.event_type, report.scope, report.correlation_key),
                         ("acked", "execution.reported", SCOPE, None))
        self.assertEqual((report.metadata["outcome_kind"], report.metadata["session_id"], report.metadata["approval_request_id"]),
                         ("suspend", session.session_id, session.pending_request_id))
        self.assertEqual(board.gateway.run_once(), 0)
        self.assertEqual(board.store.list_outbound(), [])
        self.assertEqual(self.h.http.calls, [])
        self.assertEqual(self.sessions.get(session.session_id), session)
