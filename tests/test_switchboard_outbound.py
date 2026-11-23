from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
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

    def _github_report(self, **metadata: object) -> CanonicalEvent:
        return CanonicalEvent(
            source="hibernaut",
            event_type="execution.reported",
            external_id="run:r2:emit",
            scope="proj:hibernaut",
            correlation_key="github:kun-g/hibernaut#42",
            subject="Done",
            body="PR opened",
            metadata={"run_id": "r2", "session_id": "s2", "outcome_kind": "emit", **metadata},
        )

    def test_reports_do_not_go_back_to_their_github_issue_unless_targeted(self) -> None:
        for outcome in ("emit", "error", "suspend"):
            self.assertIsNone(self.gateway().message_for_report(self._github_report(outcome_kind=outcome)))
        opted_in = self.gateway(reply_to_origin=("github", "feishu")).message_for_report(self._github_report())
        assert opted_in is not None
        self.assertEqual((opted_in.channel, dict(opted_in.target)), ("github", {"repo": "kun-g/hibernaut", "number": 42}))

    def test_approval_card_without_a_message_id_retries_with_same_uuid(self) -> None:
        gateway = self.gateway(approval_target="oc_approvals")
        gateway.consume_event(self._github_report(outcome_kind="suspend", approval_request_id="req"))
        self.http.responses = [HttpResponse(200, b'{"code":0,"data":{}}')]
        self.assertEqual(gateway.run_once(), 0)
        self.clock.advance(5)
        self.assertEqual(gateway.run_once(), 1)
        self.assertEqual(self.http.calls[0]["json"]["uuid"], self.http.calls[1]["json"]["uuid"])
        self.assertEqual(self.store.list_outbound()[0].platform_msg_id, "om_out")

    def test_emit_report_becomes_github_comment_with_labels(self) -> None:
        gateway = self.gateway()
        report = self._github_report(
            reply_channel="github",
            reply_target="kun-g/hibernaut#42",
            add_labels=["review"],
            remove_labels=["in progress"],
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
            metadata={"run_id": "r3", "session_id": "s", "outcome_kind": "emit"},
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


class TaskboardReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SwitchboardStore(str(Path(self.tmp.name) / "sb.db"))

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def gateway(self, **config: object) -> OutboundGateway:
        return OutboundGateway(self.store, {}, config=OutboundConfig(**config))  # type: ignore[arg-type]

    def report(
        self, *, source: str = "hibernaut", correlation_key: str | None = "feishu:thread:om_root", **metadata: object
    ) -> CanonicalEvent:
        return CanonicalEvent(
            source=source,
            event_type="execution.reported",
            external_id=f"r:{sorted(metadata.items())}",
            scope="proj:hibernaut",
            correlation_key=correlation_key,
            subject="s",
            body="b",
            metadata={"run_id": "r1", "session_id": "s1", "outcome_kind": "emit", **metadata},
        )

    def test_internal_task_sync_reports_never_reach_a_person(self) -> None:
        gateway = self.gateway()
        silenced = [
            {"outcome_kind": "done"},
            {"outcome_kind": "done", "task_action": "complete", "task_id": "t1"},
            {"outcome_kind": "done", "task_action": "claim", "task_id": "t1"},
            {"outcome_kind": "error", "task_action": "release", "stage": "failed", "task_id": "t1"},
            {"outcome_kind": "error", "task_action": "claim", "task_id": "t1"},
            {"outcome_kind": "error", "task_action": "update_stage", "task_id": "t1"},
            {"outcome_kind": "done", "reply_channel": "feishu", "reply_target": "oc_ops"},
        ]
        for metadata in silenced:
            with self.subTest(metadata):
                self.assertIsNone(gateway.message_for_report(self.report(**metadata)))

    def test_results_that_carry_a_task_action_are_still_delivered(self) -> None:
        gateway = self.gateway()
        delivered = [
            {"outcome_kind": "emit", "task_action": "complete", "task_id": "t1"},
            {"outcome_kind": "emit", "task_action": "release", "stage": "failed", "task_id": "t1"},
            {"outcome_kind": "emit", "task_action": "update_stage", "stage": "waiting_pr_merge", "task_id": "t1"},
            {"outcome_kind": "error"},
        ]
        for metadata in delivered:
            with self.subTest(metadata):
                msg = gateway.message_for_report(self.report(**metadata))
                assert msg is not None
                self.assertEqual((msg.channel, dict(msg.target)), ("feishu", {"reply_to_message_id": "om_root"}))

    def test_suspend_with_a_task_action_still_sends_the_approval_card(self) -> None:
        msg = self.gateway(approval_target="oc_approvals").message_for_report(self.report(
            outcome_kind="suspend", approval_request_id="apr_1", task_action="update_stage",
            stage="awaiting_approval", task_id="t1",
        ))
        assert msg is not None
        self.assertEqual(msg.kind, "approval_card")

    def notice(self, correlation_key: str | None, **metadata: object) -> CanonicalEvent:
        return self.report(
            source="taskboard", correlation_key=correlation_key, notify_kind="human_todo", task_id="t1",
            target_assignee="human:kun", reason="ready", trace_id="tr", hop_count=1, **metadata,
        )

    def target(self, gateway: OutboundGateway, event: CanonicalEvent) -> tuple[str, dict[str, object]] | None:
        msg = gateway.message_for_report(event)
        return None if msg is None else (msg.channel, dict(msg.target))

    def test_taskboard_notices_route_explicit_then_origin_then_notify_then_approval_target(self) -> None:
        full = self.gateway(notify_channel="feishu", notify_target="oc_todo", approval_target="oc_approvals")
        github_key = "github:kun-g/hibernaut#42"
        self.assertEqual(
            self.target(full, self.notice(github_key, reply_channel="feishu", reply_target="oc_explicit")),
            ("feishu", {"chat_id": "oc_explicit"}),
        )
        self.assertEqual(
            self.target(full, self.notice("feishu:thread:om_root")), ("feishu", {"reply_to_message_id": "om_root"})
        )
        # A GitHub origin is not a reply target by default, so the notice falls through to notify_target.
        self.assertEqual(self.target(full, self.notice(github_key)), ("feishu", {"chat_id": "oc_todo"}))
        self.assertEqual(self.target(full, self.notice(None)), ("feishu", {"chat_id": "oc_todo"}))
        approvals_only = self.gateway(approval_target="oc_approvals")
        self.assertEqual(self.target(approvals_only, self.notice(github_key)), ("feishu", {"chat_id": "oc_approvals"}))
        self.assertIsNone(self.target(self.gateway(), self.notice(github_key)))

    def test_every_taskboard_notice_kind_is_routed(self) -> None:
        gateway = self.gateway(notify_target="oc_todo")
        for kind in ("human_todo", "upstream_cancelled", "dependency_invalidated"):
            with self.subTest(kind):
                event = self.notice("taskboard:t1")
                event = replace(event, metadata={**event.metadata, "notify_kind": kind})
                self.assertEqual(self.target(gateway, event), ("feishu", {"chat_id": "oc_todo"}))

    def test_notify_target_does_not_reroute_ordinary_reports(self) -> None:
        gateway = self.gateway(notify_target="oc_todo", approval_target="oc_approvals")
        self.assertIsNone(gateway.message_for_report(self.report(correlation_key="github:kun-g/hibernaut#42")))
        untagged = self.report(source="taskboard", correlation_key="github:kun-g/hibernaut#42")
        self.assertIsNone(gateway.message_for_report(untagged))

    def test_notify_settings_parse_from_the_outbound_section(self) -> None:
        from switchboard.config import parse_config

        parsed = parse_config({"outbound": {"notify_channel": "feishu", "notify_target": "oc_todo"}}).outbound
        self.assertEqual((parsed.notify_channel, parsed.notify_target), ("feishu", "oc_todo"))
        defaults = parse_config({}).outbound
        self.assertEqual((defaults.notify_channel, defaults.notify_target), (None, None))


if __name__ == "__main__":
    unittest.main()
