from __future__ import annotations

import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from switchboard.app import build_switchboard
from switchboard.config import ConfigError, RelayConfig, parse_config
from switchboard.envelope import CanonicalEvent, parse_utc, utc_now
from switchboard.pipeline import CompositeTransport, InProcessTransport
from switchboard.ports import DeliverOutcome, DeliverStatus, RecordingAuditSink
from switchboard.relay_agent import RelayAgent, backoff_delay
from switchboard.relay_protocol import (
    CLOSE_AUTH_FAILED,
    CLOSE_SUPERSEDED,
    OP_PING,
    OP_TEXT,
    Ack,
    Close,
    Hello,
    Nack,
    Ping,
    Pong,
    Push,
    RelayMessage,
    RelayProtocolError,
    WebSocketConnection,
    WebSocketProtocolError,
    Welcome,
    accept_key,
    connect,
    decode_message,
    encode_close_payload,
    encode_frame,
    encode_message,
    parse_close_payload,
    read_frame,
)
from switchboard.store_sqlite import SwitchboardStore
from tests.switchboard_support import RELAY_TOKEN, FakeHttpClient, StaticCredentials, make_config

REPO_ROOT = Path(__file__).resolve().parents[1]
FAST_BACKOFF = (0.05, 0.1, 0.2)


def wait_until(predicate: Callable[[], bool], timeout: float = 45.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def ready_event(number: int, scope: str) -> CanonicalEvent:
    return CanonicalEvent(
        source="github",
        event_type="work.ready",
        external_id=f"ready:{scope}:{number}:{time.monotonic_ns()}",
        scope=scope,
        correlation_key=f"github:kun-g/hibernaut#{number}",
        subject=f"task {number}",
        metadata={"work_kind": "issue", "state": "open", "task_id": f"T{number}"},
    )


class TokenBroker:
    def __init__(self, token: str | None) -> None:
        self.token = token

    def get_webhook_secret(self, channel: str) -> str | None:
        return None

    def issue_ephemeral(self, channel: str) -> str | None:
        return self.token if channel == "relay" else None


class RelayHarness:
    def __init__(
        self,
        relay: Mapping[str, Any] | None = None,
        delivery: Mapping[str, Any] | None = None,
    ) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        sections: dict[str, Mapping[str, Any]] = {
            "relay": {"enabled": True, "transport": "hybrid", **(relay or {})}
        }
        if delivery is not None:
            sections["delivery"] = delivery
        config = make_config(self.tmp.name, **sections)
        config = replace(config, http=replace(config.http, port=0))
        self.audit = RecordingAuditSink()
        self.board = build_switchboard(
            config,
            credentials=StaticCredentials(),
            audit=self.audit,
            http_client=FakeHttpClient(),
        )
        self.board.start()
        hub = self.board.relay
        assert hub is not None
        self.hub = hub
        self.agents: list[RelayAgent] = []

    @property
    def url(self) -> str:
        return self.board.relay_url

    def agent(
        self,
        handler: Callable[[CanonicalEvent], DeliverStatus | None],
        *,
        url: str | None = None,
        token: str | None = RELAY_TOKEN,
        consumer_id: str = "hibernaut",
        **options: Any,
    ) -> RelayAgent:
        agent = RelayAgent(
            url or self.url,
            consumer_id,
            handler,
            TokenBroker(token),
            backoff_seconds=FAST_BACKOFF,
            **options,
        )
        agent.start()
        self.agents.append(agent)
        return agent

    def delivery(self, event_id: str, consumer: str = "hibernaut") -> tuple[str, int]:
        row = self.board.store.get_delivery(event_id, consumer)
        assert row is not None
        return row.status, row.attempts

    def relay_audits(self, outcome: str) -> list[dict[str, Any]]:
        return [r for r in self.audit.records if r["stage"] == "relay" and r["outcome"] == outcome]

    def close(self) -> None:
        for agent in self.agents:
            agent.stop()
        self.board.stop()
        self.tmp.cleanup()


def recv_message(ws: WebSocketConnection) -> RelayMessage | Close:
    while True:
        received = ws.recv()
        if isinstance(received, Close):
            return received
        message = decode_message(received)
        if not isinstance(message, (Ping, Pong)):
            return message


def handshake(url: str, token: str = RELAY_TOKEN, consumer_id: str = "hibernaut") -> tuple[WebSocketConnection, Welcome]:
    ws = connect(url, timeout=5)
    ws.send_text(encode_message(Hello(consumer_id=consumer_id, token=token)))
    welcome = recv_message(ws)
    assert isinstance(welcome, Welcome), welcome
    return ws, welcome


class WebSocketCodecTests(unittest.TestCase):
    def test_accept_key_matches_rfc6455_example(self) -> None:
        self.assertEqual(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")

    def test_frame_roundtrip_for_every_length_encoding(self) -> None:
        for size in (0, 125, 126, 65535, 65536):
            for mask in (False, True):
                payload = bytes(range(256)) * (size // 256) + bytes(size % 256)
                data = encode_frame(OP_TEXT, payload, mask=mask)
                frame = read_frame(io.BytesIO(data).read, max_payload=1 << 20)
                self.assertEqual((frame.opcode, frame.payload, frame.masked, frame.fin), (OP_TEXT, payload, mask, True))

    def test_oversized_frame_is_rejected_before_reading_it(self) -> None:
        data = encode_frame(OP_TEXT, b"x" * 200, mask=False)
        with self.assertRaises(WebSocketProtocolError) as ctx:
            read_frame(io.BytesIO(data).read, max_payload=100)
        self.assertEqual(ctx.exception.close_code, 1009)

    def test_close_payload_carries_custom_codes(self) -> None:
        for code in (CLOSE_AUTH_FAILED, CLOSE_SUPERSEDED):
            self.assertEqual(parse_close_payload(encode_close_payload(code, "why")), Close(code, "why"))
        self.assertEqual(parse_close_payload(b""), Close(1005, ""))

    def test_connection_pair_text_ping_and_close(self) -> None:
        a, b = socket.socketpair()
        client = WebSocketConnection(a, a.makefile("rb").read, client_side=True)
        server = WebSocketConnection(b, b.makefile("rb").read, client_side=False)
        try:
            client.send_text("héllo")
            self.assertEqual(server.recv(), "héllo")
            # A protocol-level ping is answered by the receiver without surfacing to the caller.
            client.send_ping(b"hb")
            server.send_text("after-ping")
            client_frames = client.recv()
            self.assertEqual(client_frames, "after-ping")
            server.send_close(CLOSE_SUPERSEDED, "superseded")
            self.assertEqual(client.recv(), Close(CLOSE_SUPERSEDED, "superseded"))
        finally:
            client.close()
            server.close()

    def test_server_rejects_unmasked_client_frames(self) -> None:
        a, b = socket.socketpair()
        server = WebSocketConnection(b, b.makefile("rb").read, client_side=False)
        try:
            a.sendall(encode_frame(OP_TEXT, b"plain", mask=False))
            with self.assertRaises(WebSocketProtocolError):
                server.recv()
        finally:
            a.close()
            server.close()

    def test_control_frames_must_be_short(self) -> None:
        a, b = socket.socketpair()
        server = WebSocketConnection(b, b.makefile("rb").read, client_side=False)
        try:
            a.sendall(encode_frame(OP_PING, b"x" * 126, mask=True))
            with self.assertRaises(WebSocketProtocolError):
                server.recv()
        finally:
            a.close()
            server.close()


class RelayMessageTests(unittest.TestCase):
    def test_every_frame_type_roundtrips_with_type_and_utc_ts(self) -> None:
        event = ready_event(1, "proj:a")
        messages: list[RelayMessage] = [
            Hello(consumer_id="hibernaut", token="t"),
            Welcome(consumer_id="hibernaut", conn_epoch=17, heartbeat_sec=20, ack_timeout_sec=15),
            Push(conn_epoch=17, event=event),
            Ack(conn_epoch=17, event_id=event.event_id),
            Nack(conn_epoch=17, event_id=event.event_id, error="disk full", retryable=True),
            Ping(conn_epoch=17),
            Pong(conn_epoch=17),
        ]
        expected_types = ["hello", "welcome", "push", "ack", "nack", "ping", "pong"]
        for message, expected in zip(messages, expected_types, strict=True):
            text = encode_message(message)
            data = json.loads(text)
            self.assertEqual(data["type"], expected)
            self.assertTrue(data["ts"].endswith("Z"))
            parse_utc(data["ts"])
            self.assertEqual(decode_message(text), message)

    def test_wire_keys_match_the_protocol(self) -> None:
        event = ready_event(1, "proj:a")
        keys = {
            "hello": {"type", "consumer_id", "token", "agent_version", "ts"},
            "welcome": {"type", "consumer_id", "conn_epoch", "heartbeat_sec", "ack_timeout_sec", "ts"},
            "push": {"type", "conn_epoch", "event", "ts"},
            "ack": {"type", "conn_epoch", "event_id", "ts"},
            "nack": {"type", "conn_epoch", "event_id", "error", "retryable", "ts"},
            "ping": {"type", "conn_epoch", "ts"},
        }
        samples: list[RelayMessage] = [
            Hello(consumer_id="c", token="t"),
            Welcome(consumer_id="c", conn_epoch=1, heartbeat_sec=20, ack_timeout_sec=15),
            Push(conn_epoch=1, event=event),
            Ack(conn_epoch=1, event_id=event.event_id),
            Nack(conn_epoch=1, event_id=event.event_id, error="e", retryable=False),
            Ping(conn_epoch=1),
        ]
        for sample in samples:
            data = json.loads(encode_message(sample))
            self.assertEqual(set(data), keys[data["type"]])
        welcome = json.loads(encode_message(samples[1]))
        self.assertEqual((welcome["heartbeat_sec"], welcome["ack_timeout_sec"]), (20, 15))
        self.assertEqual(json.loads(encode_message(samples[0]))["agent_version"], "1")
        self.assertEqual(json.loads(encode_message(samples[2]))["event"], event.to_dict())

    def test_malformed_frames_are_rejected(self) -> None:
        bad = [
            "not json",
            "[]",
            json.dumps({"ts": "2026-11-22T00:00:00Z"}),
            json.dumps({"type": "bogus", "ts": "2026-11-22T00:00:00Z"}),
            json.dumps({"type": "ping", "conn_epoch": 1}),
            json.dumps({"type": "ping", "conn_epoch": "1", "ts": "2026-11-22T00:00:00Z"}),
            json.dumps({"type": "ack", "conn_epoch": 1, "ts": "2026-11-22T00:00:00Z"}),
            json.dumps({"type": "push", "conn_epoch": 1, "event": {"source": "nope"}, "ts": "2026-11-22T00:00:00Z"}),
            json.dumps({"type": "hello", "consumer_id": "", "token": "t", "agent_version": "1", "ts": "2026-11-22T00:00:00Z"}),
        ]
        for text in bad:
            with self.assertRaises(RelayProtocolError, msg=text):
                decode_message(text)

    def test_token_is_not_in_repr(self) -> None:
        self.assertNotIn("secret-value", repr(Hello(consumer_id="c", token="secret-value")))


class RelayConfigTests(unittest.TestCase):
    def test_defaults(self) -> None:
        self.assertEqual(
            parse_config({}).relay,
            RelayConfig(
                enabled=False,
                path="/ws/relay",
                auth_timeout_sec=5,
                heartbeat_sec=20,
                ack_timeout_sec=15,
                max_in_flight=8,
                transport="in_process",
            ),
        )

    def test_parse_relay_section(self) -> None:
        relay = parse_config(
            {
                "relay": {
                    "enabled": True,
                    "path": "/relay",
                    "auth_timeout_sec": 3,
                    "heartbeat_sec": 10,
                    "ack_timeout_sec": 7.5,
                    "max_in_flight": 4,
                    "transport": "hybrid",
                    "listen_host": "0.0.0.0",
                    "listen_port": 9443,
                }
            }
        ).relay
        self.assertEqual(
            (relay.enabled, relay.path, relay.auth_timeout_sec, relay.heartbeat_sec),
            (True, "/relay", 3.0, 10.0),
        )
        self.assertEqual((relay.ack_timeout_sec, relay.max_in_flight, relay.transport), (7.5, 4, "hybrid"))
        self.assertEqual((relay.listen_host, relay.listen_port), ("0.0.0.0", 9443))

    def test_invalid_relay_sections(self) -> None:
        for section in (
            {"transport": "carrier-pigeon"},
            {"enabled": False, "transport": "relay"},
            {"enabled": True, "path": "ws/relay"},
            {"enabled": True, "max_in_flight": 0},
            {"enabled": True, "heartbeat_sec": 0},
        ):
            with self.assertRaises(ConfigError, msg=str(section)):
                parse_config({"relay": section})


class ResetOfflinePendingTests(unittest.TestCase):
    def test_only_pending_rows_of_that_consumer_become_due(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SwitchboardStore(str(Path(tmp) / "s.db"))
            now = utc_now()
            first, second = ready_event(1, "proj:a"), ready_event(2, "proj:b")
            store.insert_inbound(first, ("hibernaut", "taskboard"), now)
            store.insert_inbound(second, ("hibernaut",), now)
            later = now + timedelta(seconds=30)
            for event_id, consumer, status in (
                (first.event_id, "hibernaut", "pending"),
                (first.event_id, "taskboard", "pending"),
                (second.event_id, "hibernaut", "retry"),
            ):
                store.update_delivery(
                    event_id, consumer, status=status, attempts=0 if status == "pending" else 1,
                    next_retry_at=later, last_error=None, now=now,
                )
            self.assertEqual(store.reset_offline_pending("hibernaut", now), 1)
            due = {(d.row.event_id, d.row.consumer_id) for d in store.due_deliveries(now)}
            self.assertEqual(due, {(first.event_id, "hibernaut")})
            store.close()


class CompositeTransportTests(unittest.TestCase):
    def test_routes_by_mode(self) -> None:
        local = InProcessTransport()
        local.register("switchboard.outbound", lambda e: None)
        local.register("taskboard", lambda e: None)
        remote_calls: list[str] = []

        class Remote:
            def deliver(self, consumer_id: str, event: CanonicalEvent) -> DeliverOutcome:
                remote_calls.append(consumer_id)
                return DeliverOutcome(status="offline")

        event = ready_event(1, "proj:a")
        hybrid = CompositeTransport(local, Remote(), mode="hybrid", local_consumers={"switchboard.outbound"})
        self.assertEqual(hybrid.deliver("taskboard", event).status, "acked")
        self.assertEqual(hybrid.deliver("hibernaut", event).status, "offline")
        relay = CompositeTransport(local, Remote(), mode="relay", local_consumers={"switchboard.outbound"})
        self.assertEqual(relay.deliver("switchboard.outbound", event).status, "acked")
        self.assertEqual(relay.deliver("taskboard", event).status, "offline")
        self.assertEqual(remote_calls, ["hibernaut", "taskboard"])


class AgentBackoffTests(unittest.TestCase):
    def test_exponential_schedule_capped_at_30s_with_20_percent_jitter(self) -> None:
        schedule = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
        middle = [backoff_delay(i, schedule, 0.2, lambda: 0.5) for i in range(8)]
        self.assertEqual(middle, [1, 2, 4, 8, 16, 30, 30, 30])
        self.assertAlmostEqual(backoff_delay(0, schedule, 0.2, lambda: 0.0), 0.8)
        self.assertAlmostEqual(backoff_delay(9, schedule, 0.2, lambda: 1.0), 36.0)

    def test_agent_default_schedule(self) -> None:
        agent = RelayAgent("ws://127.0.0.1:1/ws/relay", "hibernaut", lambda e: None, TokenBroker("t"))
        self.assertEqual(agent.backoff_seconds, (1.0, 2.0, 4.0, 8.0, 16.0, 30.0))
        self.assertEqual(agent.jitter, 0.2)


class RelayEndToEndTests(unittest.TestCase):
    def test_hello_welcome_push_ack_marks_delivery_acked(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30})
        self.addCleanup(h.close)
        seen: list[CanonicalEvent] = []
        agent = h.agent(lambda e: seen.append(e) or None)
        self.assertTrue(agent.connected.wait(5))
        self.assertTrue(h.hub.is_online("hibernaut"))
        self.assertEqual(agent.conn_epoch, h.hub.conn_epoch("hibernaut"))

        event = ready_event(1, "proj:a")
        self.assertEqual(h.board.bus.publish(event).consumers, ("hibernaut",))
        self.assertTrue(wait_until(lambda: h.delivery(event.event_id) == ("acked", 1)))
        self.assertEqual([e.event_id for e in seen], [event.event_id])
        self.assertEqual(seen[0], event)
        connected = h.relay_audits("connected")
        self.assertEqual(connected[0]["consumer_id"], "hibernaut")
        self.assertNotIn(RELAY_TOKEN, json.dumps(h.audit.records))

    def test_invalid_token_and_missing_hello_close_with_4001(self) -> None:
        h = RelayHarness(relay={"auth_timeout_sec": 0.12, "heartbeat_sec": 30})
        self.addCleanup(h.close)

        ws = connect(h.url, timeout=5)
        ws.send_text(encode_message(Hello(consumer_id="hibernaut", token="wrong-token")))
        self.assertEqual(ws.recv(), Close(CLOSE_AUTH_FAILED, "authentication failed"))
        ws.close()
        failed = h.relay_audits("auth_failed")
        self.assertEqual(failed[0]["consumer_id"], "hibernaut")

        ws = connect(h.url, timeout=5)
        started = time.monotonic()
        received = ws.recv()
        elapsed = time.monotonic() - started
        ws.close()
        self.assertEqual(received, Close(CLOSE_AUTH_FAILED, "hello timeout"))
        self.assertGreaterEqual(elapsed, 0.08)
        self.assertLess(elapsed, 3)
        self.assertTrue(wait_until(lambda: len(h.relay_audits("auth_timeout")) == 1))

        ws = connect(h.url, timeout=5)
        ws.send_text(encode_message(Ping(conn_epoch=0)))
        self.assertEqual(ws.recv(), Close(CLOSE_AUTH_FAILED, "first frame must be hello"))
        ws.close()
        self.assertFalse(h.hub.is_online("hibernaut"))
        self.assertNotIn("wrong-token", json.dumps(h.audit.records))

    def test_agent_with_bad_token_keeps_retrying_and_never_connects(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30})
        self.addCleanup(h.close)
        agent = h.agent(lambda e: None, token="wrong-token")
        self.assertTrue(wait_until(lambda: len(h.relay_audits("auth_failed")) >= 2))
        self.assertFalse(agent.connected.is_set())

    def test_new_connection_supersedes_old_epoch_and_stale_frames_are_dropped(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30, "ack_timeout_sec": 5})
        self.addCleanup(h.close)
        old, first = handshake(h.url)
        event = ready_event(1, "proj:a")
        h.board.bus.publish(event)
        pushed = recv_message(old)
        assert isinstance(pushed, Push)
        self.assertEqual((pushed.conn_epoch, pushed.event.event_id), (first.conn_epoch, event.event_id))

        new, second = handshake(h.url)
        self.assertEqual(second.conn_epoch, first.conn_epoch + 1)
        self.assertEqual(old.recv(), Close(CLOSE_SUPERSEDED, "superseded"))
        # The old connection's late ack must not settle anything.
        try:
            old.send_text(encode_message(Ack(conn_epoch=first.conn_epoch, event_id=event.event_id)))
        except OSError:
            pass

        repushed = recv_message(new)
        assert isinstance(repushed, Push)
        self.assertEqual((repushed.conn_epoch, repushed.event.event_id), (second.conn_epoch, event.event_id))
        self.assertEqual(h.delivery(event.event_id), ("pending", 0))

        # A frame on the new connection that carries the old epoch is dropped too.
        new.send_text(encode_message(Ack(conn_epoch=first.conn_epoch, event_id=event.event_id)))
        self.assertTrue(wait_until(lambda: len(h.relay_audits("stale_frame")) >= 1))
        self.assertEqual(h.delivery(event.event_id), ("pending", 0))

        new.send_text(encode_message(Ack(conn_epoch=second.conn_epoch, event_id=event.event_id)))
        self.assertTrue(wait_until(lambda: h.delivery(event.event_id) == ("acked", 1)))
        self.assertEqual(len(h.relay_audits("superseded")), 1)
        self.assertEqual(h.hub.conn_epoch("hibernaut"), second.conn_epoch)
        old.close()
        new.close()

    def test_offline_delivery_never_counts_as_an_attempt(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30}, delivery={"retry_backoff_seconds": [0.01] * 5})
        self.addCleanup(h.close)
        event = ready_event(1, "proj:a")
        h.board.bus.publish(event)
        # ~15 offline rounds; counted as attempts this would dead-letter after 6.
        time.sleep(0.15)
        self.assertEqual(h.delivery(event.event_id), ("pending", 0))

    def test_reconnect_replays_backlog_at_once_in_scope_order(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30})
        self.addCleanup(h.close)
        a1, a2, b1, a3 = (ready_event(1, "proj:a"), ready_event(2, "proj:a"), ready_event(3, "proj:b"), ready_event(4, "proj:a"))
        for event in (a1, a2, b1, a3):
            h.board.bus.publish(event)
            time.sleep(0.002)
        # The worker tried the heads, found nobody online and pushed them back by 5s.
        def heads_deferred() -> bool:
            rows = [h.board.store.get_delivery(e.event_id, "hibernaut") for e in (a1, b1)]
            return all(r is not None and parse_utc(r.next_retry_at) > utc_now() + timedelta(seconds=3) for r in rows)
        self.assertTrue(wait_until(heads_deferred))
        for event in (a1, a2, b1, a3):
            self.assertEqual(h.delivery(event.event_id), ("pending", 0))

        seen: list[tuple[str, str]] = []
        lock = threading.Lock()

        def handler(event: CanonicalEvent) -> None:
            with lock:
                seen.append((event.scope, event.event_id))

        connected_at = time.monotonic()
        h.agent(handler)
        all_acked = wait_until(lambda: all(h.delivery(e.event_id) == ("acked", 1) for e in (a1, a2, b1, a3)), 4)
        self.assertTrue(all_acked)
        # Well inside the 5s offline backoff: the reconnect reset the schedule.
        self.assertLess(time.monotonic() - connected_at, 4)
        self.assertEqual([i for s, i in seen if s == "proj:a"], [a1.event_id, a2.event_id, a3.event_id])
        self.assertEqual([i for s, i in seen if s == "proj:b"], [b1.event_id])

    def test_online_nack_and_ack_timeout_dead_letter_without_blocking_other_scopes(self) -> None:
        h = RelayHarness(
            relay={"heartbeat_sec": 30, "ack_timeout_sec": 0.1},
            delivery={"retry_backoff_seconds": [0.01, 0.01, 0.01, 0.01, 0.01]},
        )
        self.addCleanup(h.close)
        failing, behind, other = ready_event(1, "proj:a"), ready_event(2, "proj:a"), ready_event(3, "proj:b")
        calls: list[str] = []
        lock = threading.Lock()

        def handler(event: CanonicalEvent) -> DeliverStatus | None:
            with lock:
                calls.append(event.event_id)
                count = calls.count(failing.event_id)
            if event.event_id != failing.event_id:
                return None
            # Even calls outlive ack_timeout_sec; the late reply is still a failure, because a
            # late ack for the same event_id would legitimately settle the next attempt.
            if count % 2 == 0:
                time.sleep(0.18)
            return "nack"

        agent = h.agent(handler)
        self.assertTrue(agent.connected.wait(5))
        for event in (failing, behind, other):
            h.board.bus.publish(event)
            time.sleep(0.002)

        self.assertTrue(wait_until(lambda: h.delivery(failing.event_id)[0] == "dead_letter", 15))
        self.assertEqual(h.delivery(failing.event_id), ("dead_letter", 6))
        self.assertEqual(h.delivery(other.event_id), ("acked", 1))
        self.assertEqual(h.delivery(behind.event_id), ("pending", 0))
        self.assertNotIn(behind.event_id, calls)
        outcomes = {r["outcome"] for r in h.audit.records if r["stage"] == "delivery" and r["event_id"] == failing.event_id}
        self.assertTrue({"nack", "timeout", "dead_letter"} <= outcomes, outcomes)
        self.assertGreaterEqual(len(h.relay_audits("ack_timeout")), 1)
        self.assertTrue(
            wait_until(lambda: any(r.message.title == "Switchboard dead letter" for r in h.board.store.list_outbound()))
        )

    def test_heartbeat_loss_evicts_consumer_and_agent_reconnects_to_redeliver(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 0.1, "ack_timeout_sec": 10})
        self.addCleanup(h.close)
        proxy = FreezableProxy(h.board.relay_port)
        self.addCleanup(proxy.close)
        calls: list[str] = []

        def handler(event: CanonicalEvent) -> None:
            calls.append(event.event_id)
            if len(calls) == 1:
                # Half-open from here on: the ack and every heartbeat vanish.
                proxy.freeze()

        agent = h.agent(handler, url=proxy.url(h.board.relay_path), idle_timeout_sec=0.55)
        self.assertTrue(agent.connected.wait(5))
        event = ready_event(1, "proj:a")
        h.board.bus.publish(event)

        self.assertTrue(wait_until(lambda: len(h.relay_audits("heartbeat_timeout")) == 1, 3))
        self.assertFalse(h.hub.is_online("hibernaut"))
        self.assertEqual(h.delivery(event.event_id), ("pending", 0))
        self.assertTrue(wait_until(lambda: h.delivery(event.event_id) == ("acked", 1), 8))
        self.assertEqual(calls, [event.event_id, event.event_id])
        self.assertEqual(h.hub.conn_epoch("hibernaut"), 2)
        self.assertGreaterEqual(agent.sessions, 2)

    def test_standalone_listener_and_upgrade_required(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30, "listen_port": 0})
        self.addCleanup(h.close)
        self.assertNotEqual(h.board.relay_port, h.board.http.port)
        ws, welcome = handshake(h.url)
        self.assertEqual(welcome.conn_epoch, 1)
        ws.close()
        import urllib.error
        import urllib.request

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"http://127.0.0.1:{h.board.relay_port}{h.board.relay_path}", timeout=5)
        self.assertEqual(ctx.exception.code, 426)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"{h.board.http.base_url}{h.board.relay_path}", timeout=5)
        self.assertEqual(ctx.exception.code, 404)

    def test_cli_agent_prints_events_and_acks(self) -> None:
        h = RelayHarness(relay={"heartbeat_sec": 30})
        self.addCleanup(h.close)
        env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src"), "SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN}
        proc = subprocess.Popen(
            [sys.executable, "-m", "switchboard.relay_agent", "--url", h.url, "--consumer-id", "hibernaut"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )
        self.addCleanup(proc.kill)
        self.assertTrue(wait_until(lambda: h.hub.is_online("hibernaut"), 10))
        event = ready_event(1, "proj:a")
        h.board.bus.publish(event)
        assert proc.stdout is not None
        line = proc.stdout.readline()
        self.assertEqual(json.loads(line)["event_id"], event.event_id)
        self.assertTrue(wait_until(lambda: h.delivery(event.event_id) == ("acked", 1)))
        proc.terminate()
        self.assertEqual(proc.wait(10), 0)


class FreezableProxy:
    def __init__(self, upstream_port: int) -> None:
        self._upstream = upstream_port
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._frozen: list[threading.Event] = []
        self._sockets: list[socket.socket] = []
        self._lock = threading.Lock()
        self._closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def url(self, path: str) -> str:
        return f"ws://127.0.0.1:{self._listener.getsockname()[1]}{path}"

    def freeze(self) -> None:
        with self._lock:
            for flag in self._frozen:
                flag.set()

    def close(self) -> None:
        self._closed = True
        self._listener.close()
        with self._lock:
            for sock in self._sockets:
                sock.close()

    def _accept(self) -> None:
        while not self._closed:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            upstream = socket.create_connection(("127.0.0.1", self._upstream))
            frozen = threading.Event()
            with self._lock:
                self._frozen.append(frozen)
                self._sockets.extend((client, upstream))
            threading.Thread(target=self._pump, args=(client, upstream, frozen), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client, frozen), daemon=True).start()

    # A frozen pair swallows bytes and never forwards a FIN, like a dead NAT mapping.
    def _pump(self, src: socket.socket, dst: socket.socket, frozen: threading.Event) -> None:
        while True:
            try:
                data = src.recv(65536)
            except OSError:
                return
            if not data:
                if not frozen.is_set():
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                return
            if frozen.is_set():
                continue
            try:
                dst.sendall(data)
            except OSError:
                return


if __name__ == "__main__":
    unittest.main()
