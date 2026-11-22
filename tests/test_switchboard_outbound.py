from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from switchboard.config import OutboundConfig
from switchboard.envelope import CanonicalEvent
from switchboard.http_client import HttpResponse
from switchboard.outbound.feishu_adapter import FeishuOutboundAdapter
from switchboard.outbound.gateway import OutboundGateway
from switchboard.addressing import GITHUB_OUTBOUND_MARKER
from switchboard.outbound.github_adapter import GitHubOutboundAdapter
from switchboard.ports import OutboundMessage
from switchboard.store_sqlite import SwitchboardStore
from tests.switchboard_support import FakeClock, FakeHttpClient, StaticCredentials


def text_msg(msg_id: str, chat_id: str = "oc_1", key: str | None = None) -> OutboundMessage:
    return OutboundMessage(
        msg_id=msg_id,
        idempotency_key=key or f"k-{msg_id}",
        channel="feishu",
        kind="text",
        target={"chat_id": chat_id},
        correlation_key=None,
        title="t",
        body_md="b",
    )


class GatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SwitchboardStore(str(Path(self.tmp.name) / "sb.db"))
        self.clock = FakeClock()
        self.http = FakeHttpClient()
        creds = StaticCredentials()
        self.adapters = {
            "feishu": FeishuOutboundAdapter(self.http, creds),
            "github": GitHubOutboundAdapter(self.http, creds),
        }

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def gateway(self, **config: object) -> OutboundGateway:
        return OutboundGateway(self.store, self.adapters, config=OutboundConfig(**config), clock=self.clock)  # type: ignore[arg-type]

    def test_idempotency_key_prevents_resend(self) -> None:
        gateway = self.gateway()
        first = gateway.deliver(text_msg("m1", key="same"))
        second = gateway.deliver(text_msg("m2", key="same"))
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(second.msg_id, "m1")
        gateway.run_once()
        gateway.run_once()
        self.assertEqual(len(self.http.calls), 1)

    def test_per_target_token_bucket(self) -> None:
        gateway = self.gateway(rate_limit_per_target_rpm=2)
        for i in range(3):
            gateway.deliver(text_msg(f"a{i}"))
        gateway.deliver(text_msg("b0", chat_id="oc_2"))
        self.assertEqual(gateway.run_once(), 3)
        waiting = self.store.get_outbound("a2")
        assert waiting is not None
        self.assertEqual((waiting.status, waiting.attempts), ("pending", 0))
        self.assertEqual(waiting.next_retry_at, "2026-11-22T08:00:30.000000Z")
        self.clock.advance(30)
        self.assertEqual(gateway.run_once(), 1)

    def test_retry_then_failed(self) -> None:
        gateway = self.gateway(max_retry_attempts=3)
        self.http.responses = [HttpResponse(503, b"{}")] * 3
        gateway.deliver(text_msg("m1"))
        gateway.run_once()
        row = self.store.get_outbound("m1")
        assert row is not None
        self.assertEqual((row.status, row.attempts, row.next_retry_at), ("pending", 1, "2026-11-22T08:00:05.000000Z"))
        self.clock.advance(5)
        gateway.run_once()
        self.clock.advance(15)
        gateway.run_once()
        row = self.store.get_outbound("m1")
        assert row is not None
        self.assertEqual((row.status, row.attempts), ("failed", 3))

    def test_client_error_fails_without_retry(self) -> None:
        gateway = self.gateway()
        self.http.responses = [HttpResponse(400, json.dumps({"code": 230001, "msg": "bad"}).encode())]
        gateway.deliver(text_msg("m1"))
        gateway.run_once()
        row = self.store.get_outbound("m1")
        assert row is not None
        self.assertEqual((row.status, row.attempts), ("failed", 1))
        self.assertIn("230001", row.last_error or "")

    def test_emit_report_becomes_github_comment_with_labels(self) -> None:
        gateway = self.gateway()
        report = CanonicalEvent(
            source="hibernaut",
            event_type="execution.reported",
            external_id="run:r2:emit",
            scope="proj:hibernaut",
            correlation_key="github:kun-g/hibernaut#42",
            subject="Done",
            body="PR opened",
            metadata={
                "run_id": "r2",
                "session_id": "s2",
                "outcome_kind": "emit",
                "add_labels": ["review"],
                "remove_labels": ["in progress"],
            },
        )
        self.assertIsNone(gateway.consume_event(report))
        gateway.consume_event(report)
        self.assertEqual(len(self.store.list_outbound()), 1)
        gateway.run_once()
        self.assertEqual(
            [(c["method"], c["url"]) for c in self.http.calls],
            [
                ("POST", "https://api.github.com/repos/kun-g/hibernaut/issues/42/labels"),
                ("DELETE", "https://api.github.com/repos/kun-g/hibernaut/issues/42/labels/in%20progress"),
                ("POST", "https://api.github.com/repos/kun-g/hibernaut/issues/42/comments"),
            ],
        )
        self.assertEqual(self.http.calls[0]["json"], {"labels": ["review"]})
        self.assertEqual(
            self.http.calls[2]["json"]["body"], f"**Done**\n\nPR opened\n\n{GITHUB_OUTBOUND_MARKER}"
        )
        self.assertEqual(self.http.calls[2]["headers"]["Authorization"], "Bearer ephemeral-github")
        row = self.store.list_outbound()[0]
        self.assertEqual((row.status, row.platform_msg_id), ("sent", "9001"))

    def test_feishu_thread_reply_and_untargeted_reports(self) -> None:
        gateway = self.gateway()
        base = {
            "source": "hibernaut",
            "event_type": "execution.reported",
            "scope": "user:main",
            "subject": "s",
            "body": "b",
        }
        thread = CanonicalEvent(
            external_id="r3",
            correlation_key="feishu:thread:om_root",
            metadata={"run_id": "r3", "session_id": "s", "outcome_kind": "done"},
            **base,  # type: ignore[arg-type]
        )
        gateway.consume_event(thread)
        gateway.run_once()
        self.assertEqual(self.http.calls[0]["url"], "https://open.feishu.cn/open-apis/im/v1/messages/om_root/reply")
        self.assertEqual(self.http.calls[0]["json"]["msg_type"], "text")

        orphan = CanonicalEvent(
            external_id="r4",
            correlation_key="timer:cron:x",
            metadata={"run_id": "r4", "session_id": "s", "outcome_kind": "error"},
            **base,  # type: ignore[arg-type]
        )
        self.assertIsNone(gateway.message_for_report(orphan))


if __name__ == "__main__":
    unittest.main()
