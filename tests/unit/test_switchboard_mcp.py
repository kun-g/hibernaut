from __future__ import annotations

import unittest

from tests.support.switchboard_support import MCP_TOKEN, Harness, make_harness, mcp_call, post, tool_result


class MCPToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness(delivery={"retry_backoff_seconds": [0]})

    def tearDown(self) -> None:
        self.h.close()

    def test_initialize_and_list_tools(self) -> None:
        headers = {"Authorization": f"Bearer {MCP_TOKEN}"}
        status, init = post(f"{self.h.url}/mcp", {"jsonrpc": "2.0", "id": 1, "method": "initialize"}, headers)
        self.assertEqual(status, 200)
        self.assertEqual(init["result"]["serverInfo"]["name"], "switchboard")
        _, listed = post(f"{self.h.url}/mcp", {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, headers)
        names = [tool["name"] for tool in listed["result"]["tools"]]
        self.assertEqual(names, ["submit_work", "post_message", "decide_approval", "report_execution", "replay_delivery"])

    def test_bad_token_is_401(self) -> None:
        status, _ = mcp_call(self.h.url, "submit_work", {"external_id": "x", "subject": "s"}, token="nope")
        self.assertEqual(status, 401)
        self.assertEqual(self.h.board.store.count_inbound(), 0)
        self.assertTrue(any(r["outcome"] == "rejected_401" for r in self.h.audit.records))

    def test_submit_work_from_external_cron(self) -> None:
        args = {
            "source": "timer",
            "external_id": "cron:daily-report:2026-11-22",
            "subject": "Daily report",
            "scope": "proj:hibernaut",
        }
        _, response = mcp_call(self.h.url, "submit_work", args)
        result = tool_result(response)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["consumers"], ["taskboard"])
        event = self.h.board.store.get_inbound(result["event_id"])
        assert event is not None
        self.assertEqual((event.source, event.event_type), ("timer", "work.opened"))
        self.assertEqual(event.correlation_key, "timer:cron:daily-report:2026-11-22")

        _, again = mcp_call(self.h.url, "submit_work", args)
        self.assertEqual(tool_result(again)["status"], "duplicate")

        _, ready = mcp_call(
            self.h.url,
            "submit_work",
            {"external_id": "task-7-ready", "subject": "go", "event_type": "work.ready", "task_id": "7"},
        )
        ready_result = tool_result(ready)
        self.assertEqual(ready_result["consumers"], ["hibernaut"])
        _, missing = mcp_call(self.h.url, "submit_work", {"external_id": "r2", "subject": "go", "event_type": "work.ready"})
        self.assertTrue(missing["result"]["isError"])
        self.assertIn("work.ready requires metadata.task_id", tool_result(missing)["problems"])

    def test_report_execution_fans_out_to_taskboard_and_outbound(self) -> None:
        args = {
            "external_id": "outbox:m-1",
            "scope": "proj:hibernaut",
            "correlation_key": "github:kun-g/hibernaut#42",
            "subject": "Run finished",
            "body": "done",
            "occurred_at": "2026-11-22T09:00:00+08:00",
            "metadata": {"run_id": "run-1", "session_id": "s-1", "outcome_kind": "emit"},
        }
        _, response = mcp_call(self.h.url, "report_execution", args)
        self.assertFalse(response["result"]["isError"])
        result = tool_result(response)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["consumers"], ["taskboard", "switchboard.outbound"])
        event = self.h.board.store.get_inbound(result["event_id"])
        assert event is not None
        self.assertEqual((event.source, event.event_type, event.actor), ("hibernaut", "execution.reported", "hibernaut"))
        self.assertEqual(event.scope, "proj:hibernaut")
        self.assertEqual(event.occurred_at, "2026-11-22T01:00:00.000000Z")
        self.assertEqual(event.metadata["session_id"], "s-1")

        _, again = mcp_call(self.h.url, "report_execution", {**args, "body": "done again"})
        self.assertEqual(tool_result(again)["status"], "duplicate")
        self.assertEqual(tool_result(again)["event_id"], result["event_id"])

    def test_report_execution_source_defaults_to_hibernaut_and_keeps_taskboard(self) -> None:
        tools = {tool["name"]: tool for tool in post(
            f"{self.h.url}/mcp",
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            {"Authorization": f"Bearer {MCP_TOKEN}"},
        )[1]["result"]["tools"]}
        self.assertEqual(
            tools["report_execution"]["inputSchema"]["properties"]["source"],
            {"type": "string", "enum": ["hibernaut", "taskboard"], "default": "hibernaut"},
        )
        base = {
            "scope": "proj:hibernaut",
            "correlation_key": "tb_tsk_1",
            "subject": "[human_todo] Review",
            "metadata": {"run_id": "tb_run_1", "session_id": "tb_sess_1", "outcome_kind": "emit"},
        }
        _, default = mcp_call(self.h.url, "report_execution", {**base, "external_id": "tb:notify:1"})
        event = self.h.board.store.get_inbound(tool_result(default)["event_id"])
        assert event is not None
        self.assertEqual((event.source, event.actor), ("hibernaut", "hibernaut"))

        _, from_taskboard = mcp_call(
            self.h.url, "report_execution", {**base, "external_id": "tb:notify:1", "source": "taskboard"}
        )
        result = tool_result(from_taskboard)
        # (source, external_id) is the dedup key, so the same id from Taskboard is a new event.
        self.assertEqual(result["status"], "accepted")
        event = self.h.board.store.get_inbound(result["event_id"])
        assert event is not None
        self.assertEqual((event.source, event.actor), ("taskboard", "taskboard"))

        _, bad = mcp_call(self.h.url, "report_execution", {**base, "external_id": "x", "source": "github"})
        self.assertTrue(bad["result"]["isError"])
        self.assertIn("source must be one of", tool_result(bad)["error"])

    def test_report_execution_rejects_incomplete_reports(self) -> None:
        _, missing = mcp_call(
            self.h.url,
            "report_execution",
            {"external_id": "outbox:m-2", "correlation_key": "k", "subject": "s", "metadata": {"outcome_kind": "maybe"}},
        )
        self.assertTrue(missing["result"]["isError"])
        problems = tool_result(missing)["problems"]
        self.assertIn("missing scope", problems)
        self.assertIn("execution.reported requires metadata.run_id", problems)
        self.assertIn("execution.reported requires metadata.session_id", problems)
        self.assertTrue(any("outcome_kind must be one of" in p for p in problems))
        self.assertEqual(self.h.board.store.count_inbound(), 0)

    def test_originless_report_requires_a_bound_suspend(self) -> None:
        args = {"external_id": "outbox:local", "scope": "proj:local", "subject": "Run"}
        meta = {"run_id": "run", "outcome_kind": "suspend", "session_id": "s", "approval_request_id": "a"}
        for override in ({"outcome_kind": "emit"}, {"approval_request_id": ""}, {"session_id": " "},
                         {"approval_request_id": 12}, {"session_id": True}):
            with self.subTest(override=override):
                _, result = mcp_call(self.h.url, "report_execution", {**args, "metadata": {**meta, **override}})
                self.assertEqual(tool_result(result)["status"], "invalid")
        self.assertEqual(self.h.board.store.count_inbound(), 0)

    def test_post_message_and_decide_approval(self) -> None:
        _, posted = mcp_call(self.h.url, "post_message", {"correlation_key": "github:kun-g/hibernaut#42", "body": "hi"})
        self.assertEqual(tool_result(posted)["consumers"], ["hibernaut"])
        approval = {
            "approval_request_id": "apr_9",
            "decision": "approve",
            "correlation_key": "github:kun-g/hibernaut#42",
            "actor": "kun",
        }
        _, first = mcp_call(self.h.url, "decide_approval", approval)
        self.assertEqual(tool_result(first)["status"], "accepted")
        _, second = mcp_call(self.h.url, "decide_approval", {**approval, "decision": "reject"})
        self.assertEqual(tool_result(second)["status"], "duplicate")

    def test_replay_delivery(self) -> None:
        board = self.h.board
        board.bus.subscribe("hibernaut", lambda e: "nack")
        _, posted = mcp_call(self.h.url, "post_message", {"correlation_key": "k", "body": "hi"})
        event_id = tool_result(posted)["event_id"]
        board.worker.run_once()
        row = board.store.get_delivery(event_id, "hibernaut")
        assert row is not None
        self.assertEqual(row.status, "dead_letter")

        board.bus.subscribe("hibernaut", lambda e: None)
        _, replayed = mcp_call(self.h.url, "replay_delivery", {"event_id": event_id, "consumer_id": "hibernaut"})
        self.assertEqual(tool_result(replayed)["delivery_status"], "pending")
        board.worker.run_once()
        row = board.store.get_delivery(event_id, "hibernaut")
        assert row is not None
        self.assertEqual(row.status, "acked")

        _, rejected = mcp_call(self.h.url, "replay_delivery", {"event_id": event_id, "consumer_id": "hibernaut"})
        self.assertTrue(rejected["result"]["isError"])


if __name__ == "__main__":
    unittest.main()
