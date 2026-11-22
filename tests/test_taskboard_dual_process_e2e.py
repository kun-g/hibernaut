from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tests.switchboard_support import GITHUB_SECRET, MCP_TOKEN, RELAY_TOKEN, issue_payload, post, send_github
from tests.test_switchboard_dual_process_e2e import Child, child_env, free_port, port_open, query, wait_until

# Real `python -m switchboard.app` (hybrid relay) and `python -m taskboard.app` on loopback ports.
SCOPE = "proj:hibernaut"
TASKBOARD_TOKEN = "test-taskboard-mcp-token"
STARTUP_TIMEOUT = 20.0
FLOW_TIMEOUT = 20.0


class TaskboardDualProcessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="tb_dual_")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.sb_port = free_port()
        self.tb_port = free_port()
        self.sb_db = self.root / "switchboard.db"
        self.tb_db = self.root / "taskboard.db"

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

[routing.github_repos]
"kun-g/hibernaut" = "{SCOPE}"

[delivery]
retry_backoff_seconds = [0.2, 0.2, 0.2, 0.2, 0.2]

[feishu]
api_base = "http://127.0.0.1:9"

[outbound]
# A closed loopback port: nothing leaves the machine.
github_api_base = "http://127.0.0.1:9"

[relay]
enabled = true
transport = "hybrid"
heartbeat_sec = 2
ack_timeout_sec = 5
""",
            encoding="utf-8",
        )
        tb_config = self.root / "taskboard.toml"
        tb_config.write_text(
            f"""
[http]
host = "127.0.0.1"
port = {self.tb_port}
base_path = "/taskboard/"

[sqlite]
path = "{self.tb_db}"

[switchboard]
relay_url = "ws://127.0.0.1:{self.sb_port}/ws/relay"
mcp_url = "http://127.0.0.1:{self.sb_port}/mcp"
consumer_id = "taskboard"

[intake]
auto_ready_sources = ["github"]
""",
            encoding="utf-8",
        )
        self.switchboard = Child(
            "switchboard",
            [sys.executable, "-m", "switchboard.app", "--config", str(sb_config)],
            child_env(
                SWITCHBOARD_MCP_WEBHOOK_SECRET=MCP_TOKEN,
                SWITCHBOARD_RELAY_WEBHOOK_SECRET=RELAY_TOKEN,
                SWITCHBOARD_GITHUB_WEBHOOK_SECRET=GITHUB_SECRET,
            ),
            self.root,
        )
        tb_env = {k: v for k, v in child_env().items() if not k.startswith("TASKBOARD_")}
        tb_env.update(
            SWITCHBOARD_RELAY_TOKEN=RELAY_TOKEN,
            SWITCHBOARD_MCP_TOKEN=MCP_TOKEN,
            TASKBOARD_MCP_TOKEN=TASKBOARD_TOKEN,
        )
        self.taskboard = Child(
            "taskboard",
            [sys.executable, "-m", "taskboard.app", "--config", str(tb_config)],
            tb_env,
            self.root,
        )
        self.addCleanup(self.switchboard.stop)
        self.addCleanup(self.taskboard.stop)

        self.switchboard.start()
        wait_until(lambda: port_open(self.sb_port), STARTUP_TIMEOUT, "switchboard to listen")
        self.taskboard.start()
        wait_until(lambda: port_open(self.tb_port), STARTUP_TIMEOUT, "taskboard to listen")

    def _fail(self, message: str) -> None:
        self.fail(f"{message}\n{self.switchboard.logs()}\n{self.taskboard.logs()}")

    def _wait(self, predicate: Any, message: str) -> None:
        try:
            wait_until(predicate, FLOW_TIMEOUT, message)
        except AssertionError:
            self._fail(f"timed out waiting for {message}")

    def _sb_events(self, source: str, event_type: str) -> list[dict[str, Any]]:
        rows = query(
            self.sb_db,
            "SELECT payload_json FROM inbound_events WHERE source = ? AND event_type = ? ORDER BY received_at",
            (source, event_type),
        )
        return [json.loads(row[0]) for row in rows]

    def _delivery(self, event_id: str, consumer: str) -> str | None:
        rows = query(
            self.sb_db,
            "SELECT status FROM delivery_log WHERE event_id = ? AND consumer_id = ?",
            (event_id, consumer),
        )
        return str(rows[0][0]) if rows else None

    def _ready_for(self, task_id: str) -> dict[str, Any] | None:
        return next(
            (e for e in self._sb_events("taskboard", "work.ready") if e["metadata"]["task_id"] == task_id),
            None,
        )

    def _task_by_ref(self, source_ref: str) -> tuple[str, str, int] | None:
        rows = query(
            self.tb_db, "SELECT task_id, status, dispatch_seq FROM tasks WHERE source_ref = ?", (source_ref,)
        )
        return (str(rows[0][0]), str(rows[0][1]), int(rows[0][2])) if rows else None

    def _tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        status, response = post(
            f"http://127.0.0.1:{self.tb_port}/taskboard/mcp",
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
            {"Authorization": f"Bearer {TASKBOARD_TOKEN}", "Content-Type": "application/json"},
        )
        self.assertEqual(status, 200, response)
        result = response["result"]
        if result["isError"]:
            self._fail(f"{name} failed: {result['structuredContent']}")
        return result["structuredContent"]

    def test_github_issue_becomes_work_ready_for_hibernaut(self) -> None:
        status, _ = send_github(f"http://127.0.0.1:{self.sb_port}", issue_payload("opened", number=42))
        self.assertEqual(status, 202)
        [opened] = self._sb_events("github", "work.opened")

        self._wait(lambda: self._task_by_ref("github:kun-g/hibernaut#42") is not None, "taskboard to store the issue")
        task = self._task_by_ref("github:kun-g/hibernaut#42")
        assert task is not None
        task_id, task_status, dispatch_seq = task
        self.assertEqual((task_status, dispatch_seq), ("ready", 1))

        self._wait(lambda: self._ready_for(task_id) is not None, "work.ready to reach switchboard")
        ready = self._ready_for(task_id)
        assert ready is not None
        self.assertEqual(ready["external_id"], f"tb:{task_id}:d1")
        self.assertEqual((ready["scope"], ready["correlation_key"], ready["actor"]), (SCOPE, "github:kun-g/hibernaut#42", "taskboard"))
        self.assertEqual(ready["metadata"]["trace_id"], opened["event_id"])
        self.assertEqual(ready["metadata"]["hop_count"], 1)
        # Queued for Hibernaut (not connected here, so it waits as pending).
        self.assertEqual(self._delivery(ready["event_id"], "hibernaut"), "pending")
        self._wait(lambda: self._delivery(opened["event_id"], "taskboard") == "acked", "work.opened ack")

        # An old issue that only now gets the trigger label arrives as work.updated and is created once.
        labeled = issue_payload("labeled", number=43, updated_at="2026-11-22T06:00:00Z")
        send_github(f"http://127.0.0.1:{self.sb_port}", labeled, delivery="d-43")
        self._wait(lambda: self._task_by_ref("github:kun-g/hibernaut#43") is not None, "labeled issue to become a task")
        old = self._task_by_ref("github:kun-g/hibernaut#43")
        assert old is not None
        self._wait(lambda: self._ready_for(old[0]) is not None, "work.ready for the labeled issue")

        self.taskboard.assert_alive()
        self.assertEqual(self.taskboard.stop(), 0, self.taskboard.logs())

    def test_mcp_dependency_unlock_publishes_downstream_work_ready(self) -> None:
        a = self._tool("tb_create_task", {"scope": SCOPE, "title": "A", "assignee": "agent:sde"})["task"]
        b = self._tool("tb_create_task", {"scope": SCOPE, "title": "B", "assignee": "agent:sde"})["task"]
        self._tool("tb_manage_dependency", {"op": "add", "upstream_task_id": a["task_id"], "downstream_task_id": b["task_id"]})

        self._tool("tb_transition_task", {"task_id": a["task_id"], "target_status": "ready"})
        self._wait(lambda: self._ready_for(a["task_id"]) is not None, "work.ready for A")
        self.assertIsNone(self._ready_for(b["task_id"]))

        self._tool("tb_transition_task", {"task_id": a["task_id"], "target_status": "in_progress"})
        done = self._tool("tb_transition_task", {"task_id": a["task_id"], "target_status": "done"})
        self.assertEqual(done["task"]["status"], "done")

        self._wait(lambda: self._ready_for(b["task_id"]) is not None, "work.ready for B")
        ready_b = self._ready_for(b["task_id"])
        assert ready_b is not None
        self.assertEqual(ready_b["external_id"], f"tb:{b['task_id']}:d1")
        self.assertEqual(self._delivery(ready_b["event_id"], "hibernaut"), "pending")
        listed = self._tool("tb_list_tasks", {"status": "ready"})
        self.assertEqual([t["task_id"] for t in listed["tasks"]], [b["task_id"]])

        # A human task's notification keeps source="taskboard" through report_execution; Taskboard acks its own echo.
        human = self._tool(
            "tb_create_task", {"scope": SCOPE, "title": "Approve", "assignee": "human:kun", "initial_status": "ready"}
        )["task"]

        def notification() -> dict[str, Any] | None:
            return next(
                (e for e in self._sb_events("taskboard", "execution.reported") if e["metadata"]["task_id"] == human["task_id"]),
                None,
            )

        self._wait(lambda: notification() is not None, "human_todo notification")
        event = notification()
        assert event is not None
        self.assertEqual(event["metadata"]["notify_kind"], "human_todo")
        self._wait(lambda: self._delivery(event["event_id"], "taskboard") == "acked", "taskboard to ack its own echo")
        self.assertEqual(self._sb_events("hibernaut", "execution.reported"), [])
