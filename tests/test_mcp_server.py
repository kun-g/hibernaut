from __future__ import annotations

import asyncio
import io
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import create_webhook_server
from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.app.main import build_daemon
from agent_runtime.adapters.channels.mcp_server import MCPServerAdapter
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.core.models import (
    INTERNAL_SOURCES,
    RETURN_CANCELLED_SOURCE,
    Emit,
    ForwardSpec,
    HeldReturn,
    PermissionDecision,
    SessionRef,
    Silent,
    Suspend,
)
from agent_runtime.engine import EventHandler
from tests.fakes import InMemoryPermission, RecordingChannel
from tests.test_inbox_forwarding import MAIN, SDE, ForwardCase


class TestMCPServerAdapter(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="mcp_test_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_stdio_handshake_tools_list_and_workspace_reclaim(self) -> None:
        wt_dir = self.root / "hb-issue-11"
        wt_dir.mkdir()
        daemon = build_daemon(
            db_path=self.root / "stdio.db",
            memory_root=self.root / "mem_stdio",
            mcp_bearer_token="mcp-secret",
        )
        assert daemon.mcp_server is not None
        daemon.workspaces.reserve_creating(
            work_key="github:kun-g/hibernaut#11",
            repo_checkout=str(self.root / "repo"),
            workdir=str(wt_dir),
            branch="hb/issue-11",
        )
        daemon.workspaces.mark_ready(
            "github:kun-g/hibernaut#11",
            workdir=str(wt_dir),
            branch="hb/issue-11",
        )
        # Swap worktree manager with clean mock runner for deterministic reclaim test
        daemon.mcp_server._worktree_manager = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            backend="git",
            runner=lambda cmd, _cwd: CommandResult(0, "[]" if "pr" in cmd else "", ""),
        )

        frames = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "hibernaut_list_workspaces", "arguments": {}},
            },
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "hibernaut_reclaim_workspace",
                    "arguments": {"work_key": "github:kun-g/hibernaut#11"},
                },
            },
        ]
        stdin = io.StringIO("\n".join(json.dumps(f) for f in frames) + "\n")
        stdout = io.StringIO()
        try:
            daemon.mcp_server.serve_stdio(stdin, stdout)
        finally:
            daemon.close()

        replies = [json.loads(line) for line in stdout.getvalue().strip().splitlines()]
        # Notification emits no reply -> 5 responses for ids 1..5.
        self.assertEqual([r["id"] for r in replies], [1, 2, 3, 4, 5])
        self.assertEqual(replies[0]["result"]["serverInfo"]["name"], "hibernaut-mcp")
        self.assertEqual(replies[1]["result"], {})

        tool_names = [t["name"] for t in replies[2]["result"]["tools"]]
        self.assertEqual(
            tool_names,
            [
                "hibernaut_ingest_event",
                "hibernaut_list_sessions",
                "hibernaut_list_events",
                "hibernaut_list_workspaces",
                "hibernaut_get_held_return",
                "hibernaut_resolve_approval",
                "hibernaut_reclaim_workspace",
                "hibernaut_close_session",
                "hibernaut_list_runs",
                "hibernaut_get_run_log",
                "hibernaut_list_audits",
                "hibernaut_executor_stats",
                "hibernaut_replay_event",
            ],
        )

        ws_list = replies[3]["result"]["structuredContent"]["workspaces"]
        self.assertEqual(len(ws_list), 1)
        self.assertEqual(ws_list[0]["work_key"], "github:kun-g/hibernaut#11")

        reclaim_res = replies[4]["result"]["structuredContent"]
        self.assertTrue(reclaim_res["reclaimed"])

    def held_return(self) -> tuple[Any, Any]:
        daemon = build_daemon(db_path=self.root / "held.db", memory_root=self.root / "mem_held")
        self.addCleanup(daemon.close)
        daemon.sessions.save(SessionRef(session_id="parent", inbox_id="proj:x", executor_id="steward",
                                        forward=ForwardSpec(target_inbox="proj:x", pending_returns=("forward:e:0",))))
        owing = ForwardSpec(target_inbox="proj:x:sde", return_to="proj:x", hop_count=1, parent_session_id="parent",
                             delegation_id="forward:e:0")
        daemon.sessions.save(SessionRef(session_id="child", inbox_id="proj:x:sde", executor_id="sde",
                                        parent_session_id="parent", forward=owing))
        held = HeldReturn(agent_id="sde", external_id="return:e:0", reason="ask first",
                          payload={"text": "the result", "status": "done"})
        daemon.sessions.save(SessionRef(session_id="held:return:e:0", inbox_id="proj:x:sde", executor_id="sde",
                                        status="suspended", pending_request_id="req_1", parent_session_id="child",
                                        forward=replace(owing, held=held)))
        mcp = daemon.mcp_server

        def call(name: str, **arguments: Any) -> Any:
            assert mcp is not None
            return mcp.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                       "params": {"name": name, "arguments": arguments}})

        return daemon, call

    def test_a_person_reads_a_held_result_before_deciding(self) -> None:
        _daemon, call = self.held_return()
        for arguments in ({"approval_request_id": "req_1"}, {"session_id": "held:return:e:0"}):
            self.assertEqual(call("hibernaut_get_held_return", **arguments)["result"]["structuredContent"], {
                "session_id": "held:return:e:0", "inbox_id": "proj:x:sde", "agent_id": "sde", "return_to": "proj:x",
                "status": "suspended", "approval_request_id": "req_1", "reason": "ask first",
                "result": {"text": "the result", "status": "done"},
            })
        for arguments in ({"session_id": "child"}, {"session_id": "none"}, {"approval_request_id": "req_2"}, {}):
            self.assertIn("error", call("hibernaut_get_held_return", **arguments))
        # Nothing else shows it.
        for name in ("hibernaut_list_sessions", "hibernaut_list_events"):
            self.assertNotIn("the result", json.dumps(call(name)))

    async def test_http_mcp_auth_guard_disabled_without_token_and_suspend_resume_e2e(self) -> None:
        # 1. Without bearer token, /mcp endpoint must be disabled (404).
        disabled_daemon = build_daemon(
            db_path=self.root / "disabled.db",
            memory_root=self.root / "mem_disabled",
            mcp_bearer_token=None,
        )
        srv_disabled = create_webhook_server(
            "127.0.0.1",
            0,
            disabled_daemon.handler,
            mcp_adapter=disabled_daemon.mcp_server,
        )
        port_disabled = srv_disabled.server_address[1]
        t_disabled = threading.Thread(target=srv_disabled.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        t_disabled.start()

        def _post(
            port: int,
            path: str,
            payload: dict[str, object],
            token: str | None = None,
        ) -> tuple[int, dict[str, object]]:
            headers = {"Content-Type": "application/json"}
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            req = urllib.request.Request(
                url=f"http://127.0.0.1:{port}{path}",
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers=headers,
            )
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    return int(resp.status), json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as err:
                return int(err.code), json.loads(err.read().decode("utf-8"))

        try:
            code_disabled, _ = await asyncio.to_thread(
                _post,
                port_disabled,
                "/mcp",
                {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
            self.assertEqual(code_disabled, 404)
        finally:
            srv_disabled.shutdown()
            srv_disabled.server_close()
            t_disabled.join(timeout=2.0)
            disabled_daemon.close()

        # 2. With bearer token enabled: unauthorized -> 401; authorized -> full E2E dispatch + Suspend resume.
        channel = RecordingChannel()
        token = "super-secret-mcp-token"
        daemon = build_daemon(
            db_path=self.root / "mcp_http.db",
            memory_root=self.root / "mem_http",
            max_concurrency=2,
            channel=channel,
            mcp_bearer_token=token,
        )
        server = create_webhook_server(
            "127.0.0.1",
            0,
            daemon.handler,
            mcp_adapter=daemon.mcp_server,
        )
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        server_thread.start()

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )

        try:
            unauth_code, _ = await asyncio.to_thread(
                _post,
                port,
                "/mcp",
                {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                "wrong-token",
            )
            self.assertEqual(unauth_code, 401)

            # Ingest an event that triggers Suspend via hibernaut_ingest_event
            ingest_code, ingest_body = await asyncio.to_thread(
                _post,
                port,
                "/mcp",
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_ingest_event",
                        "arguments": {
                            "inbox_id": "proj:hibernaut",
                            "source": "mcp:claude_code",
                            "external_id": "mcp-evt-1",
                            "correlation_key": "github:kun-g/hibernaut#88",
                            "payload": {
                                "action": "send_email",
                                "to": "team@example.com",
                                "subject": "MCP Ship",
                            },
                        },
                    },
                },
                token,
            )
            self.assertEqual(ingest_code, 200)
            self.assertFalse(ingest_body["result"]["structuredContent"]["duplicate"])  # type: ignore[index]

            await asyncio.sleep(0.05)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            # Query suspended sessions via hibernaut_list_sessions
            list_code, list_body = await asyncio.to_thread(
                _post,
                port,
                "/mcp",
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_list_sessions",
                        "arguments": {"inbox_id": "proj:hibernaut", "status": "suspended"},
                    },
                },
                token,
            )
            self.assertEqual(list_code, 200)
            sessions = list_body["result"]["structuredContent"]["sessions"]  # type: ignore[index]
            self.assertEqual(len(sessions), 1)
            req_id = sessions[0]["pending_approval_id"]
            self.assertTrue(req_id)

            # Resolve approval via hibernaut_resolve_approval -> resumes Suspend to Emit
            resolve_code, _ = await asyncio.to_thread(
                _post,
                port,
                "/mcp",
                {
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_resolve_approval",
                        "arguments": {
                            "inbox_id": "proj:hibernaut",
                            "approval_request_id": req_id,
                            "approved": True,
                            "comment": "LGTM from MCP",
                        },
                    },
                },
                token,
            )
            self.assertEqual(resolve_code, 200)

            await asyncio.sleep(0.05)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()

            suspend_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Suspend)]
            emit_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Emit)]
            self.assertEqual(len(suspend_reports), 1)
            self.assertEqual(len(emit_reports), 1)
            assert isinstance(emit_reports[0].outcome, Emit)
            self.assertIn("team@example.com", emit_reports[0].outcome.messages[0])
        finally:
            stop.set()
            await daemon_task
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2.0)
            daemon.close()



class IngestBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.daemon = build_daemon(db_path=root / "x.db", memory_root=root / "mem")
        self.addCleanup(self.daemon.close)

    def test_mcp_refuses_internal_sources(self) -> None:
        for source in INTERNAL_SOURCES:
            with self.subTest(source):
                assert self.daemon.mcp_server is not None
                reply = self.daemon.mcp_server.handle_rpc({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "hibernaut_ingest_event", "arguments": {
                        "inbox_id": "proj:x", "source": source, "payload": {}}},
                })
                assert reply is not None
                self.assertEqual(reply["error"]["code"], -32602)
                self.assertIn("reserved", reply["error"]["message"])
        self.assertEqual(self.daemon.inbox.fetch_pending("proj:x"), [])

    def test_http_refuses_internal_sources(self) -> None:
        server = create_webhook_server("127.0.0.1", 0, self.daemon.handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        def post(source: str) -> tuple[int, Any]:
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_address[1]}/events", method="POST",
                data=json.dumps({"inbox_id": "proj:x", "source": source, "external_id": source}).encode(),
            )
            try:
                with urllib.request.urlopen(request, timeout=5.0) as response:
                    return response.status, json.load(response)
            except urllib.error.HTTPError as exc:
                return exc.code, json.load(exc)

        for source in INTERNAL_SOURCES:
            code, body = post(source)
            self.assertEqual(code, 400)
            self.assertIn("reserved", body["error"])
        self.assertEqual(self.daemon.inbox.fetch_pending("proj:x"), [])
        self.assertEqual(post("webhook")[0], 202)


class CloseSessionTests(ForwardCase):
    # Closing over MCP, on sessions as the worker really leaves them.
    def setUp(self) -> None:
        super().setUp()
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        self.mcp = MCPServerAdapter(
            handler=EventHandler(inbox=self.inbox, on_wake=self.woken.append), sessions=self.sessions,
            workspaces=SQLiteWorkspaceStore(db), inbox=self.inbox, lock=self.lock,  # type: ignore[arg-type]
        )

    def close(self, session_id: str) -> Any:
        reply = self.mcp.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "hibernaut_close_session", "arguments": {"session_id": session_id}}})
        assert reply is not None
        return reply["result"]["structuredContent"]

    def resolve(self, request_id: str) -> None:
        self.mcp.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "hibernaut_resolve_approval",
            "arguments": {"inbox_id": SDE, "approval_request_id": request_id, "approved": True}}})

    async def held(self) -> tuple[str, str, Any]:
        # (the steward's session, the child's session, the card about its held result)
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision("block", "not today")})
        self.answer("sde", "result")
        await self.run_inbox(SDE)
        [card] = self.sent("approval_request")
        steward, child = self.agents["steward"].runs[0][1], self.agents["sde"].runs[0][1]
        assert steward is not None and child is not None
        return steward.session_id, child.session_id, card

    def cancels(self) -> list[Any]:
        return [e for e in self.inbox.all_events() if e.source == RETURN_CANCELLED_SOURCE and e.status != "dropped"]

    def status(self, session_id: str) -> str:
        session = self.sessions.get(session_id)
        assert session is not None
        return session.status

    async def test_closing_a_child_withdraws_the_approval_of_its_held_result(self) -> None:
        for name in ("child", "holder"):
            with self.subTest(name):
                self.setUp()
                steward, child, card = await self.held()
                holder, request_id = card.metadata["session_id"], card.metadata["approval_request_id"]
                self.assertEqual(self.close(child if name == "child" else holder)["status"], "closed")
                self.close(child if name == "child" else holder)
                self.assertEqual(self.status(holder), "closed")
                # Nobody can decide on it any more, whichever way the answer comes.
                self.permissions = InMemoryPermission()
                self.resolve(request_id)
                self.ingest(SDE, key=None, approval_request_id=request_id, approved=True, event_type="approval.decided")
                await self.run_inbox(SDE)
                self.assertEqual((self.returned(), len(self.agents["sde"].runs)), ([], 1))
                [cancel] = self.cancels()
                self.assertEqual((cancel.inbox_id, cancel.session_id), (MAIN, steward))
                self.assertIn(MAIN, self.woken)
                [report] = await self.run_inbox()
                self.assertFalse(report.pending_elsewhere)
                self.assertEqual(len(self.sent("return_withheld")), 1)

    async def test_closing_waits_for_the_run_that_holds_the_inbox(self) -> None:
        for name in ("child", "holder"):
            with self.subTest(name):
                self.setUp()
                _steward, child, card = await self.held()
                holder = card.metadata["session_id"]
                target = child if name == "child" else holder
                self.assertTrue(self.lock.acquire(SDE, "a worker", 60))
                self.assertEqual(self.close(target)["status"], "closing")
                self.assertEqual((self.status(child), self.status(holder), self.cancels()),
                                 ("active", "suspended", []))
                self.lock.release(SDE, "a worker")
                await self.run_inbox(SDE)
                self.assertEqual((self.status(holder), len(self.cancels())), ("closed", 1))
                self.assertEqual(self.status(child), "closed" if name == "child" else "active")

    async def test_closing_releases_the_inbox_and_wakes_it(self) -> None:
        _steward, child, _card = await self.held()
        self.woken.clear()
        self.close(child)
        self.assertIn(SDE, self.woken)
        self.assertTrue(self.lock.acquire(SDE, "a worker", 60))

    async def test_closing_a_parent_closes_its_children_elsewhere(self) -> None:
        steward, child, card = await self.held()
        self.close(steward)
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox(SDE)
        self.assertEqual((self.status(child), self.status(card.metadata["session_id"])), ("closed", "closed"))

    async def test_a_message_cannot_close_a_held_result(self) -> None:
        _steward, _child, card = await self.held()
        for source in ("session:close", "webhook"):
            self.ingest(SDE, key=None, source=source, session_id=card.metadata["session_id"], action="close_session")
        await self.run_inbox(SDE)
        self.assertEqual(self.status(card.metadata["session_id"]), "suspended")


if __name__ == "__main__":
    unittest.main()
