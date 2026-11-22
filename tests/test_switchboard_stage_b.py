from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from agent_runtime.adapters.channels.switchboard_relay import SwitchboardRelayLink
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config, start_switchboard
from switchboard.app import Switchboard, build_switchboard
from switchboard.envelope import CanonicalEvent
from switchboard.http_client import HttpResponse
from switchboard.mcp_publisher import PublishError, SwitchboardMCPPublisher
from tests.fakes import RecordingChannel
from tests.switchboard_support import MCP_TOKEN, RELAY_TOKEN, FakeHttpClient, StaticCredentials, make_config

KEY = "github:kun-g/hibernaut#42"
SCOPE = "proj:hibernaut"
MCP_URL = "https://switchboard.example.test/mcp"


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def report(**overrides: Any) -> CanonicalEvent:
    fields: dict[str, Any] = {
        "source": "hibernaut",
        "event_type": "execution.reported",
        "external_id": "outbox:m-1",
        "scope": SCOPE,
        "correlation_key": KEY,
        "actor": "hibernaut",
        "subject": "Run finished",
        "body": "done",
        "metadata": {"run_id": "r-1", "session_id": "s-1", "outcome_kind": "emit"},
    }
    fields.update(overrides)
    return CanonicalEvent(**fields)


def rpc_result(structured: dict[str, Any], *, is_error: bool = False) -> HttpResponse:
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "content": [{"type": "text", "text": json.dumps(structured)}],
            "structuredContent": structured,
            "isError": is_error,
        },
    }
    return HttpResponse(200, json.dumps(body).encode("utf-8"))


class MCPPublisherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.http = FakeHttpClient()
        self.credentials = StaticCredentials()
        self.credentials.tokens["mcp"] = MCP_TOKEN
        self.publisher = SwitchboardMCPPublisher(MCP_URL, self.credentials, http_client=self.http)

    def test_posts_report_execution_with_bearer_and_parses_result(self) -> None:
        self.http.responses.append(
            rpc_result({"status": "accepted", "event_id": "evt_X", "consumers": ["taskboard"], "problems": []})
        )
        event = report()
        result = self.publisher.publish(event)
        self.assertEqual((result.status, result.event_id, result.consumers), ("accepted", "evt_X", ("taskboard",)))
        call = self.http.calls[0]
        self.assertEqual((call["method"], call["url"]), ("POST", MCP_URL))
        self.assertEqual(call["headers"]["Authorization"], f"Bearer {MCP_TOKEN}")
        params = call["json"]["params"]
        self.assertEqual(params["name"], "report_execution")
        self.assertEqual(
            params["arguments"],
            {
                "external_id": "outbox:m-1",
                "scope": SCOPE,
                "correlation_key": KEY,
                "subject": "Run finished",
                "body": "done",
                "actor": "hibernaut",
                "occurred_at": event.occurred_at,
                "metadata": {"run_id": "r-1", "session_id": "s-1", "outcome_kind": "emit"},
                "source": "hibernaut",
            },
        )
        self.assertNotIn("event_id", params["arguments"])

    def test_execution_reported_from_taskboard_keeps_its_source(self) -> None:
        self.http.responses.append(rpc_result({"status": "accepted", "event_id": "evt_T", "consumers": []}))
        self.publisher.publish(report(source="taskboard", actor="taskboard"))
        self.assertEqual(self.http.calls[0]["json"]["params"]["arguments"]["source"], "taskboard")

    def test_work_ready_goes_through_submit_work(self) -> None:
        self.http.responses.append(
            rpc_result({"status": "accepted", "event_id": "evt_R", "consumers": ["hibernaut"], "problems": []})
        )
        metadata = {"work_kind": "task", "state": "ready", "task_id": "tb_tsk_1", "dispatch_seq": 1}
        event = CanonicalEvent(
            source="taskboard",
            event_type="work.ready",
            external_id="tb:tb_tsk_1:d1",
            scope=SCOPE,
            correlation_key=KEY,
            actor="taskboard",
            subject="Implement",
            body="details",
            metadata=metadata,
        )
        result = self.publisher.publish(event)
        self.assertEqual((result.status, result.event_id, result.consumers), ("accepted", "evt_R", ("hibernaut",)))
        params = self.http.calls[0]["json"]["params"]
        self.assertEqual(params["name"], "submit_work")
        self.assertEqual(
            params["arguments"],
            {
                "source": "taskboard",
                "event_type": "work.ready",
                "external_id": "tb:tb_tsk_1:d1",
                "scope": SCOPE,
                "correlation_key": KEY,
                "actor": "taskboard",
                "subject": "Implement",
                "body": "details",
                "metadata": metadata,
            },
        )

        self.http.responses.append(rpc_result({"status": "invalid", "problems": ["bad"]}, is_error=True))
        self.assertEqual(self.publisher.publish(event).status, "invalid")
        self.http.responses.append(rpc_result({"error": "source must be one of"}, is_error=True))
        with self.assertRaisesRegex(PublishError, "submit_work failed"):
            self.publisher.publish(event)
        self.http.responses.append(HttpResponse(503, b"down"))
        with self.assertRaisesRegex(PublishError, "HTTP 503"):
            self.publisher.publish(event)

    def test_invalid_report_is_a_result_and_the_channel_adapter_raises(self) -> None:
        self.http.responses.append(
            rpc_result({"status": "invalid", "event_id": None, "problems": ["missing scope"]}, is_error=True)
        )
        result = self.publisher.publish(report())
        self.assertEqual((result.status, result.problems), ("invalid", ("missing scope",)))

        self.http.responses.append(rpc_result({"status": "invalid", "problems": ["missing scope"]}, is_error=True))
        adapter = SwitchboardChannelAdapter(self.publisher.publish)
        with self.assertRaisesRegex(RuntimeError, "missing scope"):
            asyncio.run(adapter.send(SCOPE, "done", {"kind": "emit", "correlation_key": KEY, "session_id": "s-1"}))

    def test_transport_auth_and_rpc_failures_raise_for_outbox_retry(self) -> None:
        failures: list[HttpResponse] = [
            HttpResponse(401, b'{"error": "invalid bearer token"}'),
            HttpResponse(502, b"bad gateway"),
            HttpResponse(200, b"not json"),
            HttpResponse(200, json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "nope"}}).encode()),
            rpc_result({"error": "unknown tool 'report_execution'"}, is_error=True),
        ]
        for response in failures:
            self.http.responses.append(response)
            with self.assertRaises(PublishError):
                self.publisher.publish(report())

        class Unreachable:
            def request(self, method: str, url: str, *, headers: Any, body: bytes | None = None) -> HttpResponse:
                raise ConnectionRefusedError("refused")

        offline = SwitchboardMCPPublisher(MCP_URL, self.credentials, http_client=Unreachable())
        with self.assertRaisesRegex(PublishError, "refused"):
            offline.publish(report())

    def test_401_body_is_not_echoed(self) -> None:
        self.http.responses.append(HttpResponse(401, b'{"error": "secret-looking-detail"}'))
        with self.assertRaises(PublishError) as caught:
            self.publisher.publish(report())
        self.assertNotIn("secret-looking-detail", str(caught.exception))

    def test_missing_token_or_other_event_types_never_reach_the_network(self) -> None:
        self.credentials.tokens.pop("mcp")
        with self.assertRaisesRegex(PublishError, "no 'mcp' token"):
            self.publisher.publish(report())
        self.credentials.tokens["mcp"] = MCP_TOKEN
        with self.assertRaisesRegex(PublishError, "only execution.reported and work.ready"):
            self.publisher.publish(report(event_type="conversation.message", metadata={"channel": "github"}))
        self.assertEqual(self.http.calls, [])


class RelayConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _parse(self, **section: Any) -> Any:
        raw = {"runtime": {"memory_root": str(self.root / "memory")}, "switchboard": section}
        return parse_config(raw, base_dir=self.root).switchboard

    def test_relay_mode_reads_urls_and_resolves_ca_file(self) -> None:
        link = self._parse(
            enabled=True,
            mode="relay",
            relay_url="wss://sb.example.test/ws/relay",
            mcp_url="https://sb.example.test/mcp",
            ca_file="ca.pem",
        )
        self.assertEqual(link.mode, "relay")
        self.assertEqual(link.relay_url, "wss://sb.example.test/ws/relay")
        self.assertEqual(link.mcp_url, "https://sb.example.test/mcp")
        self.assertEqual(link.ca_file, str(self.root / "ca.pem"))
        self.assertIsNone(link.config_path)

    def test_relay_mode_needs_both_urls_with_matching_schemes(self) -> None:
        bad_sections: list[dict[str, Any]] = [
            {"enabled": True, "mode": "relay", "mcp_url": "https://h/mcp"},
            {"enabled": True, "mode": "relay", "relay_url": "wss://h/ws/relay"},
            {"enabled": True, "mode": "relay", "relay_url": "https://h/ws/relay", "mcp_url": "https://h/mcp"},
            {"enabled": True, "mode": "relay", "relay_url": "wss://h/ws/relay", "mcp_url": "wss://h/mcp"},
            {"enabled": False, "mode": "relay", "relay_url": "wss:///ws/relay"},
        ]
        for section in bad_sections:
            with self.subTest(section=section), self.assertRaises(ValueError):
                self._parse(**section)
        self.assertFalse(self._parse(enabled=False, mode="relay").enabled)

    def test_relay_start_needs_both_tokens_and_still_takes_them_out_of_the_environment(self) -> None:
        link = self._parse(enabled=True, mode="relay", relay_url="ws://127.0.0.1:1/ws/relay", mcp_url="http://127.0.0.1:1/mcp")
        env = {"SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN, "PATH": "/bin"}
        with self.assertRaisesRegex(ValueError, "SWITCHBOARD_MCP_TOKEN"):
            start_switchboard(link, environ=env)
        self.assertEqual(env, {"PATH": "/bin"})

    def test_relay_link_connects_only_after_subscribe(self) -> None:
        link = self._parse(enabled=True, mode="relay", relay_url="ws://127.0.0.1:1/ws/relay", mcp_url="http://127.0.0.1:1/mcp")
        env = {"SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN, "SWITCHBOARD_MCP_TOKEN": MCP_TOKEN}
        handle = start_switchboard(link, environ=env)
        assert isinstance(handle, SwitchboardRelayLink)
        try:
            self.assertEqual(env, {})
            self.assertIsNone(handle.agent._thread)  # pyright: ignore[reportPrivateUsage]
            with self.assertRaises(ValueError):
                handle.subscribe("someone-else", lambda event: "acked")
            self.assertIsNone(handle.agent._thread)  # pyright: ignore[reportPrivateUsage]
        finally:
            handle.stop()


class RelayEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.hub_http = FakeHttpClient()
        config = make_config(
            self.tmp.name,
            relay={"enabled": True, "transport": "hybrid", "heartbeat_sec": 30},
            delivery={"retry_backoff_seconds": [0.05] * 5},
        )
        config = replace(config, http=replace(config.http, port=0))
        self.board: Switchboard = build_switchboard(config, credentials=StaticCredentials(), http_client=self.hub_http)
        self.board.start()
        hibernaut_cfg = parse_config(
            {
                "runtime": {"memory_root": str(self.root / "memory")},
                "switchboard": {
                    "enabled": True,
                    "mode": "relay",
                    "relay_url": self.board.relay_url,
                    "mcp_url": f"{self.board.http.base_url}/mcp",
                    "consumer_id": "hibernaut",
                },
            },
            base_dir=self.root,
        )
        self.cfg = hibernaut_cfg
        handle = start_switchboard(
            hibernaut_cfg.switchboard,
            environ={"SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN, "SWITCHBOARD_MCP_TOKEN": MCP_TOKEN},
        )
        assert isinstance(handle, SwitchboardRelayLink)
        self.link = handle
        self.db = SQLiteDatabase(self.root / "h.db")
        self.daemons: list[AlwaysOnDaemon] = []

    def tearDown(self) -> None:
        self.link.stop()
        for daemon in self.daemons:
            daemon.close()
        self.board.stop()
        self.tmp.cleanup()

    def _daemon(self) -> AlwaysOnDaemon:
        daemon = build_daemon_from_config(self.cfg, database=self.db, channel=RecordingChannel(), switchboard=self.link)
        self.daemons.append(daemon)
        return daemon

    def _push(self, external_id: str, body: str) -> None:
        result = self.board.bus.publish(
            CanonicalEvent(
                source="github",
                event_type="conversation.message",
                external_id=external_id,
                scope=SCOPE,
                correlation_key=KEY,
                actor="kun",
                subject="Switchboard phase 1",
                body=body,
                metadata={"channel": "github", "reply_target": "kun-g/hibernaut#42"},
            )
        )
        self.assertEqual(result.status, "accepted")

    def test_downlink_over_relay_and_uplink_over_mcp(self) -> None:
        daemon = self._daemon()
        self.assertIsInstance(daemon.channel, SwitchboardChannelAdapter)
        self.assertTrue(self.link.agent.connected.wait(5))

        self._push("comment:1", "please fix")
        self.assertTrue(wait_until(lambda: len(daemon.inbox.fetch_pending(SCOPE)) == 1))
        event = daemon.inbox.fetch_pending(SCOPE)[0]
        self.assertEqual(event.source, "sb:github")
        self.assertEqual(event.correlation_key, KEY)
        self.assertEqual(event.payload["body"], "please fix")

        metadata = {"kind": "emit", "correlation_key": KEY, "session_id": "s-1", "run_id": "r-1", "message_id": "m-1"}
        asyncio.run(daemon.channel.send(SCOPE, "fixed in abc123", dict(metadata)))
        stored = self.board.store.find_inbound("hibernaut", "outbox:m-1")
        assert stored is not None
        self.assertEqual((stored.event_type, stored.scope, stored.body), ("execution.reported", SCOPE, "fixed in abc123"))
        self.assertEqual(stored.metadata["outcome_kind"], "emit")
        self.assertTrue(
            wait_until(lambda: any("github" in c["url"] and c["method"] == "POST" for c in self.hub_http.calls))
        )
        comment = next(c for c in self.hub_http.calls if "github" in c["url"])
        self.assertIn("fixed in abc123", comment["json"]["body"])

        # An outbox retry after a lost response is a hub duplicate, not a second comment.
        asyncio.run(daemon.channel.send(SCOPE, "fixed in abc123", dict(metadata)))
        self.assertEqual(self.board.store.count_inbound(), 2)

    def test_reload_swaps_the_handler_without_reconnecting(self) -> None:
        first = self._daemon()
        self.assertTrue(self.link.agent.connected.wait(5))
        second = self._daemon()
        self.assertEqual(self.link.agent.sessions, 1)
        self._push("comment:2", "after reload")
        self.assertTrue(wait_until(lambda: len(second.inbox.fetch_pending(SCOPE)) == 1))
        self.assertIsNot(first.switchboard_bridge, second.switchboard_bridge)


if __name__ == "__main__":
    unittest.main()
