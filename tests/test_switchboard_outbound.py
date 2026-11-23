from __future__ import annotations

import json
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from switchboard.config import OutboundConfig
from switchboard.envelope import CanonicalEvent, parse_utc
from switchboard.feishu_auth import FeishuTenantTokenSource
from switchboard.http_client import HttpResponse, UrllibHttpClient
from switchboard.outbound.feishu_adapter import FeishuOutboundAdapter
from switchboard.outbound.gateway import OutboundGateway
from switchboard.addressing import GITHUB_OUTBOUND_MARKER
from switchboard.outbound.github_adapter import GitHubOutboundAdapter
from switchboard.ports import OutboundMessage, SendResult
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


def rejected(status: int, code: int, reset: str | None = None, *, header: str = "x-ogw-ratelimit-reset") -> HttpResponse:
    headers = {} if reset is None else {header: reset}
    return HttpResponse(status, json.dumps({"code": code, "msg": "frequency limit"}).encode(), headers)


class FeishuRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SwitchboardStore(str(Path(self.tmp.name) / "sb.db"))
        self.clock = FakeClock()
        self.http = FakeHttpClient()
        self.gateway = self.make_gateway(FeishuOutboundAdapter(self.http, StaticCredentials()))
        self.sequence = 0

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def make_gateway(self, adapter: object) -> OutboundGateway:
        return OutboundGateway(self.store, {"feishu": adapter}, config=OutboundConfig(), clock=self.clock)  # type: ignore[dict-item]

    def send(self, *responses: HttpResponse) -> str:
        self.sequence += 1
        msg_id = f"m{self.sequence}"
        self.http.calls.clear()
        self.http.responses = list(responses)
        self.gateway.deliver(text_msg(msg_id, chat_id=f"oc_{msg_id}"))
        self.gateway.run_once()
        return msg_id

    def state(self, msg_id: str) -> tuple[str, int]:
        row = self.store.get_outbound(msg_id)
        assert row is not None
        return row.status, row.attempts

    def assert_resent_once_after(self, msg_id: str, seconds: float) -> None:
        self.assertEqual(self.state(msg_id), ("pending", 1))
        self.clock.advance(seconds - 0.001)
        self.assertEqual(self.gateway.run_once(), 0)
        self.assertEqual(len(self.http.calls), 1)
        self.clock.advance(0.001)
        self.assertEqual(self.gateway.run_once(), 1)
        self.clock.advance(3600)
        self.assertEqual(self.gateway.run_once(), 0)
        self.assertEqual(self.state(msg_id), ("sent", 2))
        self.assertEqual([call["json"]["uuid"] for call in self.http.calls], [msg_id, msg_id])
        self.assertEqual(self.http.calls[0], self.http.calls[1])

    def test_rate_limit_codes_wait_for_the_backoff_then_resend_with_the_same_uuid(self) -> None:
        for status, code in ((400, 230020), (400, 99991400), (429, 230020), (429, 99991400)):
            with self.subTest(status=status, code=code):
                self.assert_resent_once_after(self.send(rejected(status, code)), 5)

    def test_reply_and_approval_card_are_resent_with_the_same_uuid(self) -> None:
        reply = replace(text_msg("r1"), target={"reply_to_message_id": "om_root"})
        card = replace(text_msg("c1"), kind="approval_card", card_payload={"elements": []})
        for msg in (reply, card):
            with self.subTest(msg.msg_id):
                self.http.calls.clear()
                self.http.responses = [rejected(400, 230020)]
                self.gateway.deliver(msg)
                self.gateway.run_once()
                self.assert_resent_once_after(msg.msg_id, 5)

    def test_reset_header_longer_than_the_backoff_decides_the_wait(self) -> None:
        for status, code in ((429, 99991400), (429, 230020), (400, 99991400), (400, 230020)):
            with self.subTest(status=status, code=code):
                self.assert_resent_once_after(self.send(rejected(status, code, "42")), 42)

    def test_reset_header_name_is_case_insensitive(self) -> None:
        limited = rejected(429, 99991400, "42", header="X-Ogw-Ratelimit-Reset")
        self.assert_resent_once_after(self.send(limited), 42)

    def test_reset_header_shorter_than_the_backoff_keeps_the_backoff(self) -> None:
        self.assert_resent_once_after(self.send(rejected(429, 99991400, "2")), 5)

    def test_the_reset_is_compared_with_the_backoff_step_of_each_attempt(self) -> None:
        msg_id = self.send(*[rejected(400, 230020, "20")] * 3)
        for expected_attempts, wait in ((1, 20), (2, 20), (3, 45)):
            self.assertEqual(self.state(msg_id), ("pending", expected_attempts))
            row = self.store.get_outbound(msg_id)
            assert row is not None
            self.assertEqual(parse_utc(row.next_retry_at), self.clock.now + timedelta(seconds=wait))
            self.clock.advance(wait)
            self.gateway.run_once()
        self.assertEqual(self.state(msg_id), ("sent", 4))
        self.assertEqual({call["json"]["uuid"] for call in self.http.calls}, {msg_id})

    def test_an_invalid_reset_header_falls_back_to_the_backoff(self) -> None:
        # 1e300 is a number but no date that far ahead exists; it must not break the retry either.
        for value in ("", "soon", "0", "-30", "nan", "inf", "1e300", "999999999999"):
            with self.subTest(value):
                self.assert_resent_once_after(self.send(rejected(429, 99991400, value)), 5)

    def test_the_reset_counts_from_the_response_not_from_the_start_of_the_request(self) -> None:
        class SlowAdapter:
            def send(_, msg: OutboundMessage) -> SendResult:
                self.clock.advance(8)
                return FeishuOutboundAdapter(self.http, StaticCredentials()).send(msg)

        self.gateway = self.make_gateway(SlowAdapter())
        msg_id = self.send(rejected(429, 99991400, "42"))
        row = self.store.get_outbound(msg_id)
        assert row is not None
        self.assertEqual(parse_utc(row.next_retry_at), self.clock.now + timedelta(seconds=42))

    def test_exhausted_attempts_fail_with_the_feishu_code_and_msg(self) -> None:
        for code in (230020, 99991400):
            with self.subTest(code):
                msg_id = self.send(*[rejected(400, code)] * 5)
                for wait in (5, 15, 45, 135):
                    self.assertEqual(self.state(msg_id)[0], "pending")
                    self.clock.advance(wait)
                    self.gateway.run_once()
                self.assertEqual(self.state(msg_id), ("failed", 5))
                row = self.store.get_outbound(msg_id)
                assert row is not None
                self.assertIn(f"code={code} msg=frequency limit", row.last_error or "")
                self.assertEqual([call["json"]["uuid"] for call in self.http.calls], [msg_id] * 5)
                self.clock.advance(3600)
                self.gateway.run_once()
                self.assertEqual(len(self.http.calls), 5)

    def test_other_client_errors_still_fail_at_once_even_with_a_reset_header(self) -> None:
        for status, code in ((400, 230001), (403, 230020), (200, 230020)):
            with self.subTest(status=status, code=code):
                msg_id = self.send(rejected(status, code, "42"))
                self.assertEqual(self.state(msg_id), ("failed", 1))

    def test_a_rate_limited_token_exchange_waits_then_sends_the_message(self) -> None:
        for limited, wait in (
            (rejected(400, 99991400), 5),
            (rejected(400, 99991400, "42"), 42),
            (rejected(429, 99991400, "42"), 42),
            (rejected(400, 99991400, "soon"), 5),
        ):
            with self.subTest(status=limited.status, headers=limited.headers):
                tokens = FeishuTenantTokenSource(self.http, app_id="cli_1", app_secret="s3cret")
                self.gateway = self.make_gateway(FeishuOutboundAdapter(self.http, StaticCredentials(), tokens=tokens))
                granted = HttpResponse(200, b'{"code":0,"tenant_access_token":"t-one","expire":7200}')
                msg_id = self.send(limited, granted)
                row = self.store.get_outbound(msg_id)
                assert row is not None
                self.assertEqual(self.state(msg_id), ("pending", 1))
                self.assertIn("code=99991400 msg=frequency limit", row.last_error or "")
                self.clock.advance(wait - 0.001)
                self.assertEqual(self.gateway.run_once(), 0)
                self.clock.advance(0.001)
                self.assertEqual(self.gateway.run_once(), 1)
                posts = [call for call in self.http.calls if "/im/v1/messages" in call["url"]]
                self.assertEqual([call["json"]["uuid"] for call in posts], [msg_id])

    def test_the_reset_header_survives_the_real_http_client(self) -> None:
        class Limited(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.rfile.read(int(self.headers["Content-Length"]))
                body = b'{"code":99991400,"msg":"request trigger frequency limit"}'
                self.send_response(429)
                self.send_header("X-Ogw-Ratelimit-Reset", "42")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Limited)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            adapter = FeishuOutboundAdapter(
                UrllibHttpClient(), StaticCredentials(), api_base=f"http://127.0.0.1:{server.server_address[1]}"
            )
            self.gateway = self.make_gateway(adapter)
            msg_id = self.send()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        row = self.store.get_outbound(msg_id)
        assert row is not None
        self.assertEqual((row.status, row.attempts), ("pending", 1))
        self.assertEqual(parse_utc(row.next_retry_at), self.clock.now + timedelta(seconds=42))


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
