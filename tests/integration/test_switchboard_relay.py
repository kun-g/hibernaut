from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from tests.support.switchboard_support import RELAY_TOKEN
from tests.support.switchboard_relay import RelayHarness, ready_event, wait_until


REPO_ROOT = Path(__file__).resolve().parents[2]


class RelayEndToEndTests(unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()
