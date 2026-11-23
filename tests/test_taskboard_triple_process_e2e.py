from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tests.process_cleanup import kill_processes_under, processes_under
from tests.switchboard_support import GITHUB_SECRET, MCP_TOKEN, RELAY_TOKEN, issue_payload, post, send_github
from tests.test_switchboard_dual_process_e2e import (
    HIBERNAUT_ENTRY,
    Child,
    child_env,
    free_port,
    port_open,
    query,
    wait_until,
)

# Real `python -m switchboard.app`, `python -m taskboard.app` and Hibernaut main() (deterministic --demo executor),
# all reaching Switchboard through its /switchboard sub-path the way they do behind one public domain.
SCOPE = "proj:hibernaut"
REPO = "kun-g/hibernaut"
TRIGGER_LABEL = "hibernaut"
TASKBOARD_TOKEN = "test-taskboard-mcp-token"
OPS_CHAT = "oc_ops_test"
STARTUP_TIMEOUT = 20.0
FLOW_TIMEOUT = 30.0


class TripleProcessEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="tb_triple_")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        # Last resort for a failed case: nothing started under this root may outlive it.
        self.addCleanup(kill_processes_under, self.root)
        self.sb_port, self.tb_port, self.hb_port = free_port(), free_port(), free_port()
        self.sb_db = self.root / "switchboard.db"
        self.tb_db = self.root / "taskboard.db"
        self.hb_db = self.root / "hibernaut.db"
        sb_url = f"127.0.0.1:{self.sb_port}/switchboard"

        sb_config = self.root / "switchboard.toml"
        sb_config.write_text(
            f"""
[http]
host = "127.0.0.1"
port = {self.sb_port}
base_path = "/switchboard"

[sqlite]
path = "{self.sb_db}"

[routing]
default_scope = "{SCOPE}"

[routing.github_repos]
"{REPO}" = "{SCOPE}"

[github]
require_label = "{TRIGGER_LABEL}"

[delivery]
retry_backoff_seconds = [0.2, 0.2, 0.2, 0.2, 0.2]

[feishu]
# Closed loopback ports: outbound rows are recorded but nothing leaves the machine.
api_base = "http://127.0.0.1:9"

[outbound]
github_api_base = "http://127.0.0.1:9"
approval_channel = "feishu"
approval_target = "{OPS_CHAT}"

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
base_path = "/taskboard"

[sqlite]
path = "{self.tb_db}"

[switchboard]
relay_url = "ws://{sb_url}/ws/relay"
mcp_url = "http://{sb_url}/mcp"
consumer_id = "taskboard"

[intake]
auto_ready_sources = ["github"]
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
relay_url = "ws://{sb_url}/ws/relay"
mcp_url = "http://{sb_url}/mcp"
consumer_id = "hibernaut"
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
            SWITCHBOARD_RELAY_TOKEN=RELAY_TOKEN, SWITCHBOARD_MCP_TOKEN=MCP_TOKEN, TASKBOARD_MCP_TOKEN=TASKBOARD_TOKEN
        )
        self.taskboard = Child(
            "taskboard", [sys.executable, "-m", "taskboard.app", "--config", str(tb_config)], tb_env, self.root
        )
        self.hibernaut = Child(
            "hibernaut",
            [
                sys.executable, str(HIBERNAUT_ENTRY), "--config", str(hb_config), "--demo",
                "--outbox-interval", "0.1", "--config-poll-interval", "0.5",
            ],
            child_env(SWITCHBOARD_RELAY_TOKEN=RELAY_TOKEN, SWITCHBOARD_MCP_TOKEN=MCP_TOKEN),
            self.root,
        )
        self.children = (self.hibernaut, self.taskboard, self.switchboard)
        for child in self.children:
            self.addCleanup(child.stop)

        for child, port in ((self.switchboard, self.sb_port), (self.taskboard, self.tb_port), (self.hibernaut, self.hb_port)):
            child.start()
            self._wait(lambda port=port, child=child: port_open(port) or not self._alive(child), f"{child.name} to listen")
            child.assert_alive()
        # Both relay consumers are connected before any event is sent.
        self._wait(lambda: self._relay_connected("taskboard") and self._relay_connected("hibernaut"), "relay logins")

    # --- helpers ---

    @staticmethod
    def _alive(child: Child) -> bool:
        return child.proc is not None and child.proc.poll() is None

    def _logs(self) -> str:
        return "\n".join(child.logs() for child in self.children)

    def _wait(self, predicate: Any, message: str, timeout: float = FLOW_TIMEOUT) -> None:
        try:
            wait_until(predicate, timeout, message)
        except AssertionError as exc:
            raise AssertionError(f"{exc}\n{self._logs()}") from None

    def _relay_connected(self, consumer: str) -> bool:
        return f"relay consumer {consumer} connected" in self.switchboard.logs()

    def _tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        status, response = post(
            f"http://127.0.0.1:{self.tb_port}/taskboard/mcp",
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
            {"Authorization": f"Bearer {TASKBOARD_TOKEN}", "Content-Type": "application/json"},
        )
        self.assertEqual(status, 200, response)
        result = response["result"]
        if result["isError"]:
            self.fail(f"{name} failed: {result['structuredContent']}\n{self._logs()}")
        return result["structuredContent"]

    def _tasks(self, **filters: Any) -> dict[str, dict[str, Any]]:
        listed = self._tool("tb_list_tasks", {"scope": SCOPE, **filters})["tasks"]
        return {task["task_id"]: task for task in listed}

    def _status(self, task_id: str) -> str | None:
        task = self._tasks().get(task_id)
        return None if task is None else str(task["status"])

    def _reports(self, source: str) -> list[dict[str, Any]]:
        rows = query(
            self.sb_db,
            "SELECT payload_json FROM inbound_events WHERE source = ? AND event_type = 'execution.reported' "
            "ORDER BY received_at",
            (source,),
        )
        return [json.loads(row[0]) for row in rows]

    def _task_actions(self, task_id: str) -> list[str]:
        return [
            str(event["metadata"].get("task_action"))
            for event in self._reports("hibernaut")
            if event["metadata"].get("task_id") == task_id and event["metadata"].get("task_action")
        ]

    def _ready_events(self) -> list[dict[str, Any]]:
        rows = query(
            self.sb_db,
            "SELECT payload_json FROM inbound_events WHERE source = 'taskboard' AND event_type = 'work.ready' "
            "ORDER BY received_at",
        )
        return [json.loads(row[0]) for row in rows]

    def _outbound(self) -> list[dict[str, Any]]:
        rows = query(self.sb_db, "SELECT channel, target_json, payload_json FROM outbound_messages ORDER BY rowid")
        return [{"channel": r[0], "target": json.loads(r[1]), **json.loads(r[2])} for r in rows]

    def _delivery(self, event_id: str, consumer: str) -> str | None:
        rows = query(
            self.sb_db, "SELECT status FROM delivery_log WHERE event_id = ? AND consumer_id = ?", (event_id, consumer)
        )
        return str(rows[0][0]) if rows else None

    def _shutdown(self) -> None:
        for child in self.children:
            child.assert_alive()
        for child in self.children:
            self.assertEqual(child.stop(), 0, f"{child.name} did not exit cleanly\n{child.logs()}")
        self.assertEqual(processes_under(self.root), [])

    # --- scenarios ---

    def test_github_issue_is_claimed_and_completed_by_hibernaut(self) -> None:
        payload = issue_payload("opened", number=42, repo=REPO)
        payload["issue"]["labels"] = [{"name": TRIGGER_LABEL}]
        status, _ = send_github(f"http://127.0.0.1:{self.sb_port}/switchboard", payload)
        self.assertEqual(status, 202)

        def task_id() -> str | None:
            rows = query(self.tb_db, "SELECT task_id FROM tasks WHERE source_ref = ?", (f"github:{REPO}#42",))
            return str(rows[0][0]) if rows else None

        self._wait(lambda: task_id() is not None, "Taskboard to create the issue's task")
        tid = task_id()
        assert tid is not None

        def ready_events() -> list[dict[str, Any]]:
            return [e for e in self._ready_events() if e["metadata"]["task_id"] == tid]

        # Taskboard stores the task before it publishes it.
        self._wait(lambda: len(ready_events()) > 0, "work.ready for the task")
        [ready] = ready_events()
        self._wait(lambda: self._delivery(ready["event_id"], "hibernaut") == "acked", "work.ready acked by Hibernaut")
        self._wait(lambda: self._status(tid) == "done", "the task to be done")
        self.assertEqual(self._task_actions(tid), ["claim", "complete"])

        # The claim is internal task sync; the GitHub origin takes no automatic reply: nothing goes out.
        self.assertEqual(self._outbound(), [])
        self._shutdown()

    def test_blocked_task_is_dispatched_when_its_upstream_completes(self) -> None:
        b = self._tool("tb_create_task", {"scope": SCOPE, "title": "B", "description": "second", "assignee": "agent:sde"})["task"]
        a = self._tool("tb_create_task", {"scope": SCOPE, "title": "A", "description": "first", "assignee": "agent:sde"})["task"]
        self.assertEqual((a["status"], b["status"]), ("backlog", "backlog"))
        self._tool(
            "tb_manage_dependency",
            {"op": "add", "upstream_task_id": a["task_id"], "downstream_task_id": b["task_id"], "kind": "blocks"},
        )
        self._tool("tb_transition_task", {"task_id": a["task_id"], "target_status": "ready"})

        self._wait(lambda: self._status(a["task_id"]) == "done", "A to be done")
        self._wait(lambda: self._status(b["task_id"]) == "done", "B to be unlocked, dispatched and done")
        self.assertEqual(self._task_actions(a["task_id"]), ["claim", "complete"])
        self.assertEqual(self._task_actions(b["task_id"]), ["claim", "complete"])
        dispatched = [e["metadata"]["task_id"] for e in self._ready_events()]
        self.assertEqual(dispatched, [a["task_id"], b["task_id"]])
        done = self._tasks(status="done")
        self.assertEqual(set(done), {a["task_id"], b["task_id"]})
        self._shutdown()

    def test_human_todo_notification_reaches_the_ops_chat_once(self) -> None:
        human = self._tool(
            "tb_create_task",
            {"scope": SCOPE, "title": "Approve the rollout", "assignee": "human:kun", "initial_status": "ready"},
        )["task"]

        def notices() -> list[dict[str, Any]]:
            return [e for e in self._reports("taskboard") if e["metadata"].get("task_id") == human["task_id"]]

        self._wait(lambda: len(notices()) == 1, "the human_todo notification")
        [notice] = notices()
        self.assertEqual(notice["metadata"]["notify_kind"], "human_todo")
        self._wait(lambda: len(self._outbound()) == 1, "the outbound notification")
        [message] = self._outbound()
        self.assertEqual(message["channel"], "feishu")
        self.assertIn(OPS_CHAT, json.dumps(message["target"]))
        self.assertIn("Approve the rollout", json.dumps(message))
        # Taskboard receives its own execution.reported back and must not turn it into a task.
        self._wait(lambda: self._delivery(notice["event_id"], "taskboard") == "acked", "Taskboard to ack its echo")
        self.assertEqual(list(self._tasks()), [human["task_id"]])
        self.assertEqual(self._ready_events(), [])
        self._shutdown()


if __name__ == "__main__":
    unittest.main()
