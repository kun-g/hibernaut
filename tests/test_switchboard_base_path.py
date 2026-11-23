from __future__ import annotations

import json
import tempfile
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from typing import Any

from switchboard.app import build_switchboard
from switchboard.config import ConfigError, parse_config
from switchboard.ports import RecordingAuditSink
from switchboard.relay_protocol import Welcome
from tests.switchboard_support import (
    FEISHU_TOKEN,
    MCP_TOKEN,
    FakeHttpClient,
    Harness,
    StaticCredentials,
    github_headers,
    issue_payload,
    make_config,
    make_harness,
    post,
)
from tests.test_switchboard_relay import RelayHarness, handshake


def get(url: str) -> tuple[int, dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


class BasePathConfigTests(unittest.TestCase):
    def test_default_is_no_prefix(self) -> None:
        self.assertEqual(parse_config({}).http.base_path, "")

    def test_base_path_is_normalised(self) -> None:
        for raw, expected in (
            ("/switchboard", "/switchboard"),
            ("switchboard/", "/switchboard"),
            ("/hub/switchboard/", "/hub/switchboard"),
            (" /switchboard ", "/switchboard"),
            ("/", ""),
            ("", ""),
        ):
            with self.subTest(raw):
                self.assertEqual(parse_config({"http": {"base_path": raw}}).http.base_path, expected)

    def test_bad_base_paths_are_rejected(self) -> None:
        for raw in ("/switch board", "/sb?x=1", "/sb#frag", "//sb", "/a//b", 5):
            with self.subTest(raw), self.assertRaises(ConfigError):
                parse_config({"http": {"base_path": raw}})


class BasePathServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness(http={"base_path": "/switchboard"})

    def tearDown(self) -> None:
        self.h.close()

    def prefixes(self) -> tuple[str, str]:
        return self.h.url, f"{self.h.url}/switchboard"

    def test_healthz_answers_with_and_without_the_prefix(self) -> None:
        for base in self.prefixes():
            with self.subTest(base):
                self.assertEqual(get(f"{base}/healthz"), (200, {"status": "ok"}))
        self.assertEqual(get(f"{self.h.url}/other/healthz")[0], 404)

    def test_webhooks_and_mcp_answer_with_and_without_the_prefix(self) -> None:
        for index, base in enumerate(self.prefixes()):
            with self.subTest(base):
                raw = json.dumps(issue_payload(number=50 + index)).encode("utf-8")
                status, body = post(f"{base}/webhooks/github", raw, github_headers(raw, "issues", f"d-{index}"))
                self.assertEqual((status, body["status"]), (202, "accepted"))

                status, body = post(
                    f"{base}/webhooks/feishu", {"type": "url_verification", "challenge": "abc", "token": FEISHU_TOKEN}
                )
                self.assertEqual((status, body), (200, {"challenge": "abc"}))

                headers = {"Authorization": f"Bearer {MCP_TOKEN}", "Content-Type": "application/json"}
                status, listed = post(f"{base}/mcp", {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers)
                self.assertEqual(status, 200)
                self.assertTrue(listed["result"]["tools"])
        self.assertEqual(self.h.board.store.count_inbound(), 2)

    def test_unknown_prefixed_routes_are_404(self) -> None:
        self.assertEqual(post(f"{self.h.url}/switchboard/webhooks/gitlab", {})[0], 404)
        self.assertEqual(post(f"{self.h.url}/elsewhere/mcp", {})[0], 404)


class _PrefixedRelayHarness(RelayHarness):
    def __init__(self, relay: dict[str, Any]) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        config = make_config(
            self.tmp.name,
            http={"base_path": "/switchboard"},
            relay={"enabled": True, "transport": "hybrid", **relay},
        )
        config = replace(config, http=replace(config.http, port=0))
        self.audit = RecordingAuditSink()
        self.board = build_switchboard(
            config, credentials=StaticCredentials(), audit=self.audit, http_client=FakeHttpClient()
        )
        self.board.start()
        hub = self.board.relay
        assert hub is not None
        self.hub = hub
        self.agents = []


class BasePathRelayTests(unittest.TestCase):
    def test_relay_handshake_works_with_and_without_the_prefix(self) -> None:
        # Shared [http] port, then a separate relay listener.
        for relay in ({}, {"listen_host": "127.0.0.1", "listen_port": 0}):
            with self.subTest(relay):
                harness = _PrefixedRelayHarness(relay)
                try:
                    base = harness.url.removesuffix("/ws/relay")
                    self.assertTrue(base.startswith("ws://"))
                    for url in (f"{base}/ws/relay", f"{base}/switchboard/ws/relay"):
                        ws, welcome = handshake(url)
                        self.assertIsInstance(welcome, Welcome)
                        ws.close()
                    self.assertEqual(get(base.replace("ws://", "http://", 1) + "/switchboard/ws/relay")[0], 426)
                finally:
                    harness.close()


if __name__ == "__main__":
    unittest.main()
