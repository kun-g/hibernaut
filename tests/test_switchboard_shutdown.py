from __future__ import annotations

import contextlib
import io
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Mapping, Sequence
from concurrent.futures import thread as futures_thread
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from switchboard.app import Switchboard, build_switchboard
from switchboard.envelope import CanonicalEvent, utc_now
from switchboard.http_client import HttpResponse
from switchboard.ports import DeliverStatus, OutboundMessage
from switchboard.relay_protocol import Ack, Close, Push, connect, encode_message
from tests.switchboard_support import (
    RELAY_TOKEN,
    FakeHttpClient,
    StaticCredentials,
    github_headers,
    issue_payload,
    make_config,
    mcp_call,
    send_github,
)
from tests.test_switchboard_relay import handshake, ready_event, recv_message, wait_until

REPO = Path(__file__).resolve().parent.parent


class Gate:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.opened = threading.Event()

    def pass_through(self) -> None:
        self.entered.set()
        assert self.opened.wait(30)


class GatedCredentials(StaticCredentials):
    def __init__(self, gate: Gate, channel: str = "github") -> None:
        super().__init__()
        self.gate: Gate | None = gate
        self.channel = channel

    def get_webhook_secret(self, channel: str) -> str | None:
        if channel == self.channel and self.gate is not None:
            self.gate.pass_through()
        return super().get_webhook_secret(channel)


# Records every use of the connection, so a test can tell that none came after a point.
class RecordingConnection:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self.used: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.used.append(name)
        return getattr(self._conn, name)


class GatedHttpClient(FakeHttpClient):
    def __init__(self, gate: Gate) -> None:
        super().__init__()
        self.gate = gate

    def request(
        self, method: str, url: str, *, headers: Mapping[str, str], body: bytes | None = None
    ) -> HttpResponse:
        self.gate.pass_through()
        return super().request(method, url, headers=headers, body=body)


def text_message(number: int) -> OutboundMessage:
    return OutboundMessage(
        msg_id=f"msg_{number}",
        idempotency_key=f"key_{number}",
        channel="feishu",
        kind="text",
        target={"chat_id": f"oc_{number}"},
        correlation_key=None,
        title="",
        body_md="hi",
    )


class ShutdownHarness(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="sb_stop_", ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.db = Path(tmp.name) / "switchboard.db"

    def board(self, *, timeout: float = 10.0, **options: Any) -> Switchboard:
        sections = options.pop("sections", {})
        config = make_config(self.tmp, **sections)
        config = replace(config, http=replace(config.http, port=0), shutdown_timeout_seconds=timeout)
        options.setdefault("credentials", StaticCredentials())
        options.setdefault("http_client", FakeHttpClient())
        board = build_switchboard(config, **options)
        board.start()
        self.addCleanup(board.stop, 0.1)
        return board

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        with contextlib.closing(sqlite3.connect(self.db)) as conn:
            return [tuple(row) for row in conn.execute(sql).fetchall()]

    # The last connection to leave a WAL database checkpoints it and removes its side files.
    def assert_store_closed(self, board: Switchboard) -> None:
        with self.assertRaises(sqlite3.ProgrammingError):
            board.store.count_inbound()
        self.assertEqual([p.name for p in Path(self.tmp).iterdir() if p.name.startswith("switchboard.db")], ["switchboard.db"])

    def stop_in_thread(self, board: Switchboard) -> tuple[threading.Thread, list[bool]]:
        result: list[bool] = []
        thread = threading.Thread(target=lambda: result.append(board.stop()), daemon=True)
        thread.start()
        return thread, result

    def stop_waiting_on(self, board: Switchboard, component: Any, method: str) -> tuple[threading.Thread, list[bool]]:
        entered = threading.Event()
        operation = getattr(component, method)

        def observe(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            return operation(*args, **kwargs)

        with mock.patch.object(component, method, side_effect=observe):
            thread, result = self.stop_in_thread(board)
            self.assertTrue(entered.wait(5))
        return thread, result


def refused(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return False
    except OSError:
        return True


class DrainTests(ShutdownHarness):
    def test_request_in_flight_completes_while_new_connections_are_refused(self) -> None:
        gate = Gate()
        board = self.board(credentials=GatedCredentials(gate))
        port = board.http.port
        answers: list[tuple[int, dict[str, Any]]] = []
        client = threading.Thread(
            target=lambda: answers.append(send_github(board.http.base_url, issue_payload())), daemon=True
        )
        client.start()
        self.assertTrue(gate.entered.wait(5))
        stopper, result = self.stop_in_thread(board)
        self.assertTrue(wait_until(lambda: refused(port)))
        self.assertTrue(stopper.is_alive())
        gate.opened.set()
        client.join(5)
        stopper.join(5)
        self.assertEqual(answers[0][0], 202, answers)
        self.assertEqual(result, [True])
        self.assert_store_closed(board)
        self.assertEqual(self.rows("SELECT event_id FROM inbound_events"), [(answers[0][1]["event_id"],)])

    def test_delivery_in_flight_is_recorded_and_the_next_one_is_not_claimed(self) -> None:
        for max_parallel in (1, 2):
            with self.subTest(max_parallel=max_parallel):
                self.db.unlink(missing_ok=True)
                gate = Gate()
                seen: list[str] = []
                board = self.board(sections={"delivery": {"max_parallel": max_parallel}})

                def handler(event: CanonicalEvent, seen: list[str] = seen, gate: Gate = gate) -> DeliverStatus:
                    seen.append(event.event_id)
                    gate.pass_through()
                    return "acked"

                events = [ready_event(n, f"proj:{n}") for n in range(4)]
                board.worker.request_stop()
                self.assertTrue(board.worker.join(5))
                for event in events:
                    board.bus.publish(event)
                board.bus.subscribe("hibernaut", handler)
                board.worker.start()
                self.assertTrue(wait_until(lambda seen=seen, n=max_parallel: len(seen) == n))
                stopper, result = self.stop_waiting_on(board, board.worker, "join")
                self.assertTrue(stopper.is_alive())
                gate.opened.set()
                stopper.join(5)
                self.assertEqual(result, [True])
                self.assertEqual(len(seen), max_parallel)
                self.assert_store_closed(board)
                rows = self.rows("SELECT status, attempts FROM delivery_log ORDER BY status")
                self.assertEqual(rows, [("acked", 1)] * max_parallel + [("pending", 0)] * (4 - max_parallel))

    def test_outbound_send_in_flight_is_recorded_and_the_next_one_is_not_claimed(self) -> None:
        gate = Gate()
        http = GatedHttpClient(gate)
        board = self.board(http_client=http)
        for number in (1, 2):
            board.store.insert_outbound(text_message(number), board.pipeline._clock())  # pyright: ignore[reportPrivateUsage]
        board.gateway.wake()
        self.assertTrue(gate.entered.wait(5))
        stopper, result = self.stop_waiting_on(board, board.gateway, "join")
        self.assertTrue(stopper.is_alive())
        gate.opened.set()
        stopper.join(5)
        self.assertEqual(result, [True])
        self.assertEqual(len(http.calls), 1)
        self.assert_store_closed(board)
        self.assertEqual(
            self.rows("SELECT msg_id, status, attempts FROM outbound_messages ORDER BY msg_id"),
            [("msg_1", "sent", 1), ("msg_2", "pending", 0)],
        )

    def test_write_in_flight_finishes_before_the_database_closes(self) -> None:
        board = self.board()
        # Leave only the open write transaction to drain.
        self.assertTrue(board.worker.stop())
        self.assertTrue(board.gateway.stop())
        entered, commit = threading.Event(), threading.Event()
        store = board.store

        # Holds the store lock with an open write transaction, as a slow INSERT would.
        def slow_write() -> None:
            with store._lock:  # pyright: ignore[reportPrivateUsage]
                conn = store._conn  # pyright: ignore[reportPrivateUsage]
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO inbound_events VALUES ('evt_slow', 1, 's', 'x', 't', 'p', NULL, 'o', 'r', '{}')"
                )
                entered.set()
                commit.wait(10)
                conn.execute("COMMIT")

        writer = threading.Thread(target=slow_write, daemon=True)
        writer.start()
        self.assertTrue(entered.wait(5))
        stopper, result = self.stop_waiting_on(board, store, "close")
        self.assertTrue(stopper.is_alive())
        commit.set()
        stopper.join(5)
        self.assertEqual(result, [True])
        self.assert_store_closed(board)
        self.assertEqual(self.rows("SELECT event_id FROM inbound_events"), [("evt_slow",)])

    def test_relay_push_in_flight_is_acked_then_the_peer_gets_a_close_frame(self) -> None:
        board = self.board(sections={"relay": {"enabled": True, "transport": "hybrid"}})
        ws, welcome = handshake(board.relay_url)
        self.addCleanup(ws.close)
        event = ready_event(1, "proj:a")
        board.bus.publish(event)
        push = recv_message(ws)
        assert isinstance(push, Push)
        stopper, result = self.stop_waiting_on(board, board.worker, "join")
        self.assertTrue(wait_until(lambda: refused(board.relay_port)))
        self.assertTrue(stopper.is_alive())
        ws.send_text(encode_message(Ack(conn_epoch=welcome.conn_epoch, event_id=event.event_id)))
        closed = recv_message(ws)
        assert isinstance(closed, Close)
        self.assertEqual(closed.code, 1001)
        ws.send_close(1001)
        stopper.join(5)
        self.assertEqual(result, [True])
        self.assert_store_closed(board)
        self.assertEqual(self.rows("SELECT status, attempts FROM delivery_log"), [("acked", 1)])

    def test_unauthenticated_relay_connection_gets_a_close_frame(self) -> None:
        board = self.board(sections={"relay": {"enabled": True, "transport": "hybrid"}})
        ws = connect(board.relay_url, timeout=5)
        self.addCleanup(ws.close)
        hub = board.relay
        assert hub is not None
        self.assertTrue(wait_until(lambda: bool(hub._sockets)))  # pyright: ignore[reportPrivateUsage]
        _, result = self.stop_in_thread(board)
        closed = ws.recv()
        assert isinstance(closed, Close)
        self.assertEqual(closed.code, 1001)
        ws.send_close(1001)
        self.assertTrue(wait_until(lambda: result == [True]))

    def test_push_waiting_for_the_window_is_deferred_not_sent(self) -> None:
        board = self.board(sections={"relay": {"enabled": True, "transport": "hybrid", "max_in_flight": 1}})
        ws, welcome = handshake(board.relay_url)
        self.addCleanup(ws.close)
        events = [ready_event(n, f"proj:{n}") for n in (1, 2)]
        for event in events:
            board.bus.publish(event)
        first = recv_message(ws)
        assert isinstance(first, Push)
        stopper, result = self.stop_in_thread(board)
        self.assertTrue(wait_until(lambda: refused(board.relay_port)))
        ws.send_text(encode_message(Ack(conn_epoch=welcome.conn_epoch, event_id=first.event.event_id)))
        self.assertIsInstance(recv_message(ws), Close)
        ws.send_close(1001)
        stopper.join(5)
        self.assertEqual(result, [True])
        self.assertEqual(
            sorted(self.rows("SELECT status, attempts FROM delivery_log")), [("acked", 1), ("pending", 0)]
        )

    def test_idle_connection_does_not_hold_the_shutdown(self) -> None:
        board = self.board()
        with socket.create_connection(("127.0.0.1", board.http.port), timeout=5) as idle:
            # A request line that never ends is not a request yet.
            idle.sendall(b"POST /webh")
            self.assertTrue(wait_until(lambda: bool(board.http._server._waiting)))
            started = time.monotonic()
            self.assertTrue(board.stop())
            self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(idle.recv(1024), b"")

    def test_request_arriving_on_an_accepted_connection_is_still_served(self) -> None:
        board = self.board()
        raw = b'{"action": "noop"}'
        headers = github_headers(raw, "ping", "d-9")
        head = "".join(f"{k}: {v}\r\n" for k, v in headers.items())
        with socket.create_connection(("127.0.0.1", board.http.port), timeout=5) as sock:
            self.assertTrue(wait_until(lambda: bool(board.http._server._waiting)))
            stopper, result = self.stop_in_thread(board)
            self.assertTrue(wait_until(lambda: refused(board.http.port)))
            sock.sendall(f"POST /webhooks/github HTTP/1.1\r\nHost: x\r\nContent-Length: {len(raw)}\r\n{head}\r\n".encode() + raw)
            self.assertRegex(sock.recv(4096), rb"^HTTP/1\.0 2\d\d ")
            stopper.join(5)
        self.assertEqual(result, [True])

    def test_stop_is_idempotent(self) -> None:
        board = self.board()
        self.assertTrue(board.stop())
        self.assertTrue(board.stop())


class TimeoutTests(ShutdownHarness):
    def assert_forced(self, board: Switchboard, component: str) -> None:
        started = time.monotonic()
        with self.assertLogs("switchboard", level="WARNING") as logs:
            self.assertFalse(board.stop())
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, board.config.shutdown_timeout_seconds)
        self.assertLess(elapsed, 2.5)
        self.assertEqual({r.levelname for r in logs.records}, {"WARNING"})
        self.assertIn(component, "\n".join(logs.output))
        self.assertIn(f"timed out after {board.config.shutdown_timeout_seconds:g}s", "\n".join(logs.output))
        with self.assertRaises(sqlite3.ProgrammingError):
            board.store.count_inbound()

    def stuck_delivery(self, **sections: Mapping[str, Any]) -> tuple[Switchboard, Gate]:
        gate = Gate()
        board = self.board(timeout=0.2, sections=sections)
        self.addCleanup(gate.opened.set)

        def handler(event: CanonicalEvent) -> DeliverStatus:
            gate.pass_through()
            return "acked"

        board.bus.subscribe("hibernaut", handler)
        for number in range(3):
            board.bus.publish(ready_event(number, f"proj:{number}"))
        self.assertTrue(gate.entered.wait(5))
        return board, gate

    def test_stuck_delivery(self) -> None:
        board, _ = self.stuck_delivery()
        self.assert_forced(board, "deliveries")
        self.assertEqual(self.rows("SELECT DISTINCT status, attempts FROM delivery_log"), [("pending", 0)])

    def test_stuck_parallel_deliveries_leave_no_thread_that_blocks_interpreter_exit(self) -> None:
        board, _ = self.stuck_delivery(delivery={"max_parallel": 4})
        self.assert_forced(board, "deliveries")
        blocking = [t.name for t in threading.enumerate() if t.name.startswith("switchboard") and not t.daemon]
        self.assertEqual(blocking, [])
        # Pool threads are daemons too, but concurrent.futures joins the registered ones at interpreter exit.
        pooled = [t.name for t in futures_thread._threads_queues if t.name.startswith("switchboard")]  # type: ignore[attr-defined]
        self.assertEqual(pooled, [])

    def test_stuck_outbound_send(self) -> None:
        gate = Gate()
        board = self.board(timeout=0.2, http_client=GatedHttpClient(gate))
        self.addCleanup(gate.opened.set)
        board.gateway.deliver(text_message(1))
        self.assertTrue(gate.entered.wait(5))
        self.assert_forced(board, "outbound")
        self.assertEqual(self.rows("SELECT status, attempts FROM outbound_messages"), [("pending", 0)])

    def test_stuck_http_request(self) -> None:
        gate = Gate()
        board = self.board(timeout=0.2, credentials=GatedCredentials(gate))
        before = set(threading.enumerate())

        def client() -> None:
            with contextlib.suppress(OSError):
                send_github(board.http.base_url, issue_payload())

        sender = threading.Thread(target=client, daemon=True)
        sender.start()
        self.assertTrue(gate.entered.wait(5))
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assert_forced(board, "HTTP")
            # Released after the cut: it must leave without touching the closed database.
            gate.opened.set()
            self.assertTrue(wait_until(lambda: set(threading.enumerate()) <= before))
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertNotIn("ProgrammingError", stderr.getvalue())

    def test_request_whose_body_never_arrives_is_cut(self) -> None:
        board = self.board(timeout=0.2)
        before = set(threading.enumerate())
        with socket.create_connection(("127.0.0.1", board.http.port), timeout=5) as sock:
            sock.sendall(b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n{")
            self.assertTrue(wait_until(lambda: bool(board.http._server._active)))
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                self.assert_forced(board, "HTTP")
                handlers = set(threading.enumerate()) - before
                self.assertEqual(handlers, set())
            try:
                self.assertEqual(sock.recv(1024), b"")
            except ConnectionResetError:
                pass
            # The rest of the body arrives too late to be handled.
            with contextlib.suppress(OSError):
                sock.sendall(b" " * 99)
        self.assertNotIn("Traceback", stderr.getvalue())

    # Each tool writes inside MCPInboundAdapter.handle(), so nothing after handle() can hold it back.
    MCP_WRITES: tuple[tuple[str, dict[str, str]], ...] = (
        ("submit_work", {"external_id": "late", "subject": "late"}),
        ("replay_delivery", {"event_id": "", "consumer_id": "hibernaut"}),
    )

    # A board with one dead letter and an MCP call held in its token check.
    def stuck_mcp_call(self, tool: str, arguments: dict[str, str]) -> tuple[Switchboard, Gate, set[threading.Thread]]:
        gate = Gate()
        board = self.board(timeout=0.05, credentials=GatedCredentials(gate, "mcp"))
        self.addCleanup(gate.opened.set)
        dead = ready_event(1, "proj:a")
        # Not due, so the worker leaves it alone.
        later = utc_now() + timedelta(hours=1)
        board.store.insert_inbound(dead, ["hibernaut"], later)
        board.store.update_delivery(
            dead.event_id, "hibernaut", status="dead_letter", attempts=6, next_retry_at=later, last_error="x", now=later
        )
        if "event_id" in arguments:
            arguments = {**arguments, "event_id": dead.event_id}
        before = set(threading.enumerate())

        def client() -> None:
            with contextlib.suppress(OSError):
                mcp_call(board.http.base_url, tool, arguments)

        threading.Thread(target=client, daemon=True).start()
        self.assertTrue(gate.entered.wait(5))
        return board, gate, before

    def assert_no_late_write(self) -> None:
        self.assertEqual(self.rows("SELECT external_id FROM inbound_events WHERE external_id = 'late'"), [])
        self.assertEqual(self.rows("SELECT status, attempts FROM delivery_log"), [("dead_letter", 6)])

    def test_mcp_call_released_after_the_stop_does_not_reach_the_closed_database(self) -> None:
        for tool, arguments in self.MCP_WRITES:
            with self.subTest(tool):
                self.setUp()
                board, gate, before = self.stuck_mcp_call(tool, arguments)
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    self.assert_forced(board, "HTTP")
                    conn = RecordingConnection(board.store._conn)  # pyright: ignore[reportPrivateUsage]
                    board.store._conn = conn  # type: ignore[assignment]  # pyright: ignore
                    gate.opened.set()
                    self.assertTrue(wait_until(lambda: set(threading.enumerate()) <= before))
                self.assertEqual(conn.used, [])
                self.assertNotIn("Traceback", stderr.getvalue())
                self.assertNotIn("ProgrammingError", stderr.getvalue())
                self.assert_no_late_write()

    def test_mcp_call_released_after_a_stop_that_could_not_close_the_database_writes_nothing(self) -> None:
        for tool, arguments in self.MCP_WRITES:
            with self.subTest(tool):
                self.setUp()
                board, gate, before = self.stuck_mcp_call(tool, arguments)
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    with mock.patch.object(board.store, "close", return_value=False) as close, self.assertLogs("switchboard", level="WARNING") as logs:
                        self.assertFalse(board.stop())
                        close.assert_called_once()
                    self.assertIn("SQLite write", "\n".join(logs.output))
                    conn = RecordingConnection(board.store._conn)  # pyright: ignore[reportPrivateUsage]
                    board.store._conn = conn  # type: ignore[assignment]  # pyright: ignore
                    gate.opened.set()
                    self.assertTrue(wait_until(lambda: set(threading.enumerate()) <= before))
                self.assertEqual(conn.used, [])
                self.assertNotIn("Traceback", stderr.getvalue())
                self.assert_no_late_write()

    def test_mcp_call_released_between_the_cut_and_the_close_writes_nothing(self) -> None:
        for tool, arguments in self.MCP_WRITES:
            with self.subTest(tool):
                self.setUp()
                board, gate, before = self.stuck_mcp_call(tool, arguments)
                close = board.store.close

                def release_then_close(timeout: float = -1) -> bool:
                    gate.opened.set()
                    self.assertTrue(wait_until(lambda: set(threading.enumerate()) <= before))
                    return close(timeout)

                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr), mock.patch.object(board.store, "close", release_then_close):
                    self.assert_forced(board, "HTTP")
                self.assertNotIn("Traceback", stderr.getvalue())
                self.assert_no_late_write()

    def test_silent_relay_peers_are_cut_at_the_same_deadline(self) -> None:
        board = self.board(timeout=0.3, sections={"relay": {"enabled": True, "transport": "hybrid"}})
        ws, _ = handshake(board.relay_url)
        self.addCleanup(ws.close)
        started = time.monotonic()
        with self.assertLogs("switchboard", level="WARNING") as logs:
            self.assertFalse(board.stop())
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.3)
        self.assertLess(elapsed, 0.9)
        self.assertIn("relay connections", "\n".join(logs.output))
        self.assertIsInstance(recv_message(ws), Close)
        self.assert_store_closed(board)

    def test_cut_pushes_do_not_add_a_second_waiting_period(self) -> None:
        board = self.board(
            timeout=0.3, sections={"relay": {"enabled": True, "transport": "hybrid", "ack_timeout_sec": 30}}
        )
        ws, _ = handshake(board.relay_url)
        self.addCleanup(ws.close)
        board.bus.publish(ready_event(1, "proj:a"))
        self.assertIsInstance(recv_message(ws), Push)
        started = time.monotonic()
        with self.assertLogs("switchboard", level="WARNING"):
            self.assertFalse(board.stop())
        self.assertLess(time.monotonic() - started, 0.9)
        self.assertIsInstance(recv_message(ws), Close)
        self.assertEqual(self.rows("SELECT status, attempts FROM delivery_log"), [("pending", 0)])

    def test_unacked_relay_push_goes_back_to_pending_without_an_attempt(self) -> None:
        board = self.board(
            timeout=0.2, sections={"relay": {"enabled": True, "transport": "hybrid", "ack_timeout_sec": 30}}
        )
        ws, _ = handshake(board.relay_url)
        self.addCleanup(ws.close)
        board.bus.publish(ready_event(1, "proj:a"))
        self.assertIsInstance(recv_message(ws), Push)
        self.assert_forced(board, "deliveries")
        self.assertIsInstance(recv_message(ws), Close)
        self.assertEqual(self.rows("SELECT status, attempts FROM delivery_log"), [("pending", 0)])

    def test_stuck_write_leaves_the_connection_to_process_exit(self) -> None:
        board = self.board(timeout=0.2)
        lock = board.store._lock  # pyright: ignore[reportPrivateUsage]
        lock.acquire()
        try:
            started = time.monotonic()
            with self.assertLogs("switchboard", level="WARNING"):
                self.assertFalse(board.stop())
            self.assertLess(time.monotonic() - started, 2.5)
        finally:
            lock.release()


# Linux lets a thread that is just creating another one take a signal sent to the process, and Python runs handlers on
# the main thread only. Each byte on stdin is a signal that this helper thread takes.
SIGNALLED_OFF_MAIN = """
import os, signal, sys, threading
import switchboard.app

def take() -> None:
    while data := os.read(0, 1):
        signal.pthread_kill(threading.get_ident(), data[0])

threading.Thread(target=take, daemon=True).start()
sys.exit(switchboard.app.main(sys.argv[1:]))
"""


class SignalTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="sb_signal_")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.db = self.root / "switchboard.db"
        self.log = self.root / "stderr.log"

    def start(
        self, timeout: float, launcher: Sequence[str] = ("-m", "switchboard.app")
    ) -> tuple[subprocess.Popen[bytes], int]:
        config = self.root / "switchboard.toml"
        config.write_text(
            f'shutdown_timeout_seconds = {timeout}\n[http]\nport = 0\n[sqlite]\npath = "{self.db}"\n'
            '[relay]\nenabled = true\ntransport = "hybrid"\nack_timeout_sec = 60\nheartbeat_sec = 60\n',
            encoding="utf-8",
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD_")}
        env.update(
            PYTHONPATH=str(REPO / "src"), PYTHONUNBUFFERED="1",
            SWITCHBOARD_RELAY_WEBHOOK_SECRET=RELAY_TOKEN, SWITCHBOARD_MCP_WEBHOOK_SECRET="test-mcp-bearer",
        )
        with self.log.open("wb") as log:
            proc = subprocess.Popen(
                [sys.executable, *launcher, "--config", str(config)],
                cwd=self.root, env=env, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT,
            )
        assert proc.stdin is not None
        self.addCleanup(proc.stdin.close)
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        self.assertTrue(wait_until(lambda: "relay endpoint" in self.output(), 20), self.output())
        port = int(self.output().split("listening on http://127.0.0.1:")[1].split()[0])
        return proc, port

    def output(self) -> str:
        return self.log.read_text(errors="replace")

    # An accepted request whose body never arrives keeps its handler thread busy.
    def hang_request(self, port: int) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.addCleanup(sock.close)
        sock.sendall(b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n{")
        time.sleep(0.3)
        return sock

    def test_first_signal_drains_and_exits_zero(self) -> None:
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum.name):
                proc, _ = self.start(10)
                proc.send_signal(signum)
                self.assertEqual(proc.wait(8), 0, self.output())
                self.assertNotIn("WARNING", self.output())
                self.assertNotIn("Traceback", self.output())
                self.assertEqual([p.name for p in self.root.glob("switchboard.db*")], ["switchboard.db"])

    def test_stuck_work_is_cut_at_the_timeout_with_exit_zero(self) -> None:
        proc, port = self.start(0.2)
        # Both events wait for the consumer, so its connection makes them due in one parallel round.
        for number in (1, 2):
            status, body = mcp_call(
                f"http://127.0.0.1:{port}",
                "post_message",
                {"correlation_key": f"github:a/b#{number}", "body": "hi", "scope": f"proj:{number}", "external_id": f"m{number}"},
            )
            self.assertEqual(status, 200, body)
        time.sleep(0.2)
        ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
        self.addCleanup(ws.close)
        for _ in range(2):
            self.assertIsInstance(recv_message(ws), Push)
        self.hang_request(port)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(8), 0, self.output())
        self.assertLess(time.monotonic() - started, 4)
        self.assertRegex(self.output(), r"WARNING .*HTTP requests, deliveries")
        self.assertNotIn("Traceback", self.output())
        with contextlib.closing(sqlite3.connect(self.db)) as conn:
            rows = conn.execute("SELECT status, attempts FROM delivery_log").fetchall()
        self.assertEqual(rows, [("pending", 0)] * 2)

    def test_signals_taken_by_another_thread_are_not_lost(self) -> None:
        for first, second in ((signal.SIGTERM, None), (signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM)):
            with self.subTest(first=first.name, second=second and second.name):
                proc, port = self.start(60, ("-c", SIGNALLED_OFF_MAIN))
                assert proc.stdin is not None
                if second is not None:
                    ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
                    self.addCleanup(ws.close)
                os.write(proc.stdin.fileno(), bytes([first]))
                if second is None:
                    self.assertEqual(proc.wait(8), 0, self.output())
                    continue
                self.assertIsInstance(recv_message(ws), Close)
                self.assertTrue(refused(port))
                os.write(proc.stdin.fileno(), bytes([second]))
                self.assertEqual(proc.wait(8), 128 + second, self.output())

    def test_second_signal_exits_at_once(self) -> None:
        for first, second in ((signal.SIGTERM, signal.SIGTERM), (signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM)):
            with self.subTest(first=first.name, second=second.name):
                proc, port = self.start(60)
                ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
                self.addCleanup(ws.close)
                proc.send_signal(first)
                # The unacknowledged close frame proves the first signal reached the drain.
                self.assertIsInstance(recv_message(ws), Close)
                self.assertTrue(refused(port))
                self.assertIsNone(proc.poll(), self.output())
                started = time.monotonic()
                proc.send_signal(second)
                self.assertEqual(proc.wait(8), 128 + second)
                self.assertLess(time.monotonic() - started, 2)


if __name__ == "__main__":
    unittest.main()
