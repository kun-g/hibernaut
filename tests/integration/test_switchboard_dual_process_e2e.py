from __future__ import annotations

import json
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

from tests.support.switchboard_support import MCP_TOKEN, RELAY_TOKEN, mcp_call, tool_result
from tests.support.switchboard_dual_process_e2e import (
    Child,
    HIBERNAUT_ENTRY,
    child_env,
    free_port,
    port_open,
    query,
    wait_until,
)


SCOPE = "proj:hibernaut"
KEY = "github:kun-g/hibernaut#42"
CONSUMER = "hibernaut"
STARTUP_TIMEOUT = 45.0


class DualProcessEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="sb_dual_")
        self.root = Path(self.tmp.name)
        self.sb_port = free_port()
        self.hb_port = free_port()
        self.sb_db = self.root / "switchboard.db"
        self.hb_db = self.root / "hibernaut.db"
        self.gate = self.root / "gate.open"
        self.outbox_clock = self.root / "outbox-clock"

        sb_config = self.root / "switchboard.toml"
        sb_config.write_text(
            f"""
[http]
host = "127.0.0.1"
port = {self.sb_port}

[sqlite]
path = "{self.sb_db}"

[routing]
default_scope = "{SCOPE}"

[delivery]
retry_backoff_seconds = [0.05, 0.05, 0.05, 0.05, 0.05]

[outbound]
# A closed loopback port: replies are recorded in outbound_messages but never reach GitHub.
github_api_base = "http://127.0.0.1:9"
reply_to_origin = ["github"]

[relay]
enabled = true
transport = "hybrid"
heartbeat_sec = 2
ack_timeout_sec = 5
""",
            encoding="utf-8",
        )
        hb_config = self.root / "hibernaut.toml"
        hb_config.write_text(
            f"""
[runtime]
db_path = "{self.hb_db}"
memory_root = "{self.root / 'memory'}"
max_concurrency = 2
default_executor = "demo"

[channels]
host = "127.0.0.1"
port = {self.hb_port}

[switchboard]
enabled = true
mode = "relay"
relay_url = "ws://127.0.0.1:{self.sb_port}/ws/relay"
mcp_url = "http://127.0.0.1:{self.sb_port}/mcp"
consumer_id = "{CONSUMER}"
""",
            encoding="utf-8",
        )
        self.switchboard = Child(
            "switchboard",
            [sys.executable, "-m", "switchboard.app", "--config", str(sb_config)],
            child_env(
                SWITCHBOARD_MCP_WEBHOOK_SECRET=MCP_TOKEN,
                SWITCHBOARD_RELAY_WEBHOOK_SECRET=RELAY_TOKEN,
            ),
            self.root,
        )
        self.hibernaut = Child(
            "hibernaut",
            [
                sys.executable, str(HIBERNAUT_ENTRY), "--config", str(hb_config), "--demo",
                "--outbox-interval", "0.1", "--config-poll-interval", "0.5",
            ],
            child_env(
                SWITCHBOARD_RELAY_TOKEN=RELAY_TOKEN,
                SWITCHBOARD_MCP_TOKEN=MCP_TOKEN,
                HB_E2E_GATE=str(self.gate),
                HB_E2E_OUTBOX_CLOCK=str(self.outbox_clock),
            ),
            self.root,
        )
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.switchboard.stop)
        self.addCleanup(self.hibernaut.stop)
        self.start_switchboard()
        self.start_hibernaut()

    def _diagnostics(self) -> str:
        return f"{self.switchboard.logs()}\n{self.hibernaut.logs()}"

    def start_switchboard(self) -> None:
        self.switchboard.start()
        wait_until(lambda: port_open(self.sb_port) or self._exited(self.switchboard), STARTUP_TIMEOUT, "switchboard port")
        self.switchboard.assert_alive()

    def start_hibernaut(self) -> None:
        self.hibernaut.start()
        wait_until(lambda: port_open(self.hb_port) or self._exited(self.hibernaut), STARTUP_TIMEOUT, "hibernaut port")
        self.hibernaut.assert_alive()

    @staticmethod
    def _exited(child: Child) -> bool:
        return child.proc is not None and child.proc.poll() is not None

    def wait(self, predicate: Callable[[], bool], message: str, timeout: float = STARTUP_TIMEOUT) -> None:
        try:
            wait_until(predicate, timeout, message)
        except AssertionError as exc:
            raise AssertionError(f"{exc}\n{self._diagnostics()}") from None

    def post_message(self, external_id: str, body: str) -> str:
        status, response = mcp_call(
            f"http://127.0.0.1:{self.sb_port}",
            "post_message",
            {"correlation_key": KEY, "body": body, "scope": SCOPE, "channel": "github", "external_id": external_id},
        )
        self.assertEqual(status, 200, response)
        result = tool_result(response)
        self.assertEqual(result["status"], "accepted", result)
        return str(result["event_id"])

    def delivery(self, event_id: str) -> tuple[str, int] | None:
        rows = query(
            self.sb_db,
            "SELECT status, attempts FROM delivery_log WHERE event_id = ? AND consumer_id = ?",
            (event_id, CONSUMER),
        )
        return (str(rows[0][0]), int(rows[0][1])) if rows else None

    def reports(self) -> list[tuple[str, str]]:
        rows = query(
            self.sb_db,
            "SELECT external_id, payload_json FROM inbound_events WHERE event_type = 'execution.reported' "
            "ORDER BY received_at",
        )
        return [(str(r[0]), str(json.loads(r[1]).get("body", ""))) for r in rows]

    def outbound_bodies(self) -> list[str]:
        rows = query(
            self.sb_db,
            "SELECT payload_json FROM outbound_messages WHERE channel = 'github' AND correlation_key = ?",
            (KEY,),
        )
        return [str(json.loads(r[0]).get("body_md", "")) for r in rows]

    def hb_outbox(self) -> list[tuple[str, str, int, str | None]]:
        rows = query(self.hb_db, "SELECT message_id, status, attempts, next_retry_at FROM outbox ORDER BY seq")
        return [(str(r[0]), str(r[1]), int(r[2]), r[3]) for r in rows]

    def test_normal_round_trip_downlink_relay_and_uplink_mcp(self) -> None:
        event_id = self.post_message("rt-1", "hello over relay")

        self.wait(lambda: self.delivery(event_id) == ("acked", 1), "delivery_log acked by hibernaut")
        self.wait(lambda: any(body == "echo: hello over relay" for _, body in self.reports()), "execution.reported")
        self.wait(lambda: "echo: hello over relay" in self.outbound_bodies(), "outbound_messages row")
        # Switchboard records the upload before Hibernaut reads the answer and marks the row.
        self.wait(lambda: self.hb_outbox()[0][1] == "sent", "outbox sent")
        (_, status, attempts, next_retry_at), = self.hb_outbox()
        self.assertEqual((status, attempts, next_retry_at), ("sent", 0, None))
        self.assertEqual([body for _, body in self.reports()], ["echo: hello over relay"])

    def test_hibernaut_offline_backlog_is_replayed_in_order_after_restart(self) -> None:
        warmup = self.post_message("warm-1", "warmup")
        self.wait(lambda: self.delivery(warmup) == ("acked", 1), "warmup acked")

        self.assertEqual(self.hibernaut.stop(), 0, self.hibernaut.logs())
        first = self.post_message("off-1", "first while offline")
        second = self.post_message("off-2", "second while offline")
        # Observe the offline head of the queue being deferred on successive delivery rounds.
        def next_retry():
            [(due,)] = query(self.sb_db, "SELECT next_retry_at FROM delivery_log WHERE event_id = ? AND consumer_id = ?",
                             (first, CONSUMER))
            return due

        for _ in range(3):
            due = next_retry()
            self.wait(lambda: next_retry() > due, "offline delivery deferred again")
        self.assertEqual(self.delivery(first), ("pending", 0))
        self.assertEqual(self.delivery(second), ("pending", 0))

        self.start_hibernaut()
        self.wait(
            lambda: self.delivery(first) == ("acked", 1) and self.delivery(second) == ("acked", 1),
            "backlog acked after restart",
        )
        bodies = [
            json.loads(r[0])["body"]
            for r in query(
                self.hb_db,
                "SELECT payload_json FROM event_inbox WHERE source = 'sb:mcp' AND external_id IN (?, ?) ORDER BY seq",
                (first, second),
            )
        ]
        self.assertEqual(bodies, ["first while offline", "second while offline"])
        self.wait(lambda: any(b == "echo: second while offline" for _, b in self.reports()), "backlog reported")

    def test_switchboard_restart_is_ridden_out_by_outbox_backoff(self) -> None:
        tick = self.outbox_clock.with_suffix(".tmp")
        tick.write_text("2030-01-01T00:00:00.000000+00:00")
        tick.replace(self.outbox_clock)
        event_id = self.post_message("gate-1", "gate: report after the switchboard restarts")
        self.wait(lambda: self.delivery(event_id) == ("acked", 1), "gated event acked")

        self.assertEqual(self.switchboard.stop(), 0, self.switchboard.logs())
        self.gate.touch()
        self.wait(lambda: any(attempts >= 1 for _, _, attempts, _ in self.hb_outbox()), "first failed upload")
        (_, status, attempts, next_retry_at), = self.hb_outbox()
        self.assertEqual((status, attempts), ("pending", 1))
        self.assertEqual(next_retry_at, "2030-01-01T00:00:05.000000+00:00")

        self.start_switchboard()
        self.assertEqual(self.hb_outbox()[0][1:], ("pending", 1, next_retry_at))
        assert next_retry_at is not None
        tick.write_text(next_retry_at)
        tick.replace(self.outbox_clock)
        self.wait(lambda: self.hb_outbox()[0][1] == "sent", "outbox sent after backoff", timeout=20.0)
        (_, status, attempts, next_retry_at), = self.hb_outbox()
        self.assertEqual((status, attempts, next_retry_at), ("sent", 1, None))
        self.wait(
            lambda: any(b == "echo: gate: report after the switchboard restarts" for _, b in self.reports()),
            "execution.reported after restart",
        )


if __name__ == "__main__":
    unittest.main()
