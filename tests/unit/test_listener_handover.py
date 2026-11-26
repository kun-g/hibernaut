import asyncio
import contextlib
import http.client
import io
import json
import os
import select
import socket
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from agent_runtime.adapters.channels.channels import (
    HANDOVER_FD_ENV,
    LISTEN_FD_ENV,
    ListenerHandover,
    WebhookHTTPServer,
)
from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from scripts import run_updating


class NoContent(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def listening_fd() -> tuple[int, int]:
    # Only a bare fd stays open, as after inheriting it from another process.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        return os.dup(sock.fileno()), sock.getsockname()[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(port: int, timeout: float = 5.0) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", "/")
        return conn.getresponse().status
    finally:
        conn.close()


def fd_open(fd: int) -> bool:
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


class ListenerHandoverTests(unittest.TestCase):
    def serve(self, server: WebhookHTTPServer) -> None:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()

        def close() -> None:
            server.shutdown()
            server.server_close()
            thread.join()

        self.addCleanup(close)

    def test_without_an_inherited_socket_the_daemon_binds_its_own(self):
        environ: dict[str, str] = {}
        port = free_port()
        server = ListenerHandover.from_environ(environ).server(("127.0.0.1", port), NoContent)
        self.serve(server)
        self.assertEqual(server.server_address, ("127.0.0.1", port))
        self.assertEqual(get(port), 204)

    def test_an_inherited_socket_for_the_configured_address_is_served_instead_of_binding(self):
        fd, port = listening_fd()
        os.set_inheritable(fd, True)
        environ = {LISTEN_FD_ENV: str(fd), "OTHER": "kept"}
        handover = ListenerHandover.from_environ(environ)
        # Executor subprocesses inherit os.environ; the fd number must not reach them.
        self.assertEqual(environ, {"OTHER": "kept"})
        server = handover.server(("localhost", port), NoContent)
        self.serve(server)
        self.assertEqual(server.socket.fileno(), fd)
        self.assertFalse(os.get_inheritable(fd))
        self.assertEqual(get(port), 204)

    def test_an_inherited_socket_for_another_address_is_closed_and_the_configured_one_bound(self):
        fd, inherited_port = listening_fd()
        port = free_port()
        server = ListenerHandover.from_environ({LISTEN_FD_ENV: str(fd)}).server(("127.0.0.1", port), NoContent)
        self.serve(server)
        self.assertFalse(fd_open(fd))
        self.assertEqual(get(port), 204)
        with self.assertRaises(ConnectionRefusedError):
            get(inherited_port)

    def test_a_released_listener_queues_connections_until_the_next_generation_serves_them(self):
        supervisor_end, daemon_end = socket.socketpair()
        self.addCleanup(supervisor_end.close)
        channel_fd = os.dup(daemon_end.fileno())
        daemon_end.close()
        handover = ListenerHandover.from_environ({HANDOVER_FD_ENV: str(channel_fd)})
        server = handover.server(("127.0.0.1", 0), NoContent)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        self.assertEqual(get(port), 204)

        def stop_daemon() -> None:
            server.shutdown()
            handover.release(server)
            server.server_close()

        stopping = threading.Thread(target=stop_daemon)
        stopping.start()
        fd = run_updating.reclaim_listener(supervisor_end)
        stopping.join(10)
        thread.join()
        assert fd is not None
        self.addCleanup(lambda: fd_open(fd) and os.close(fd))
        self.assertFalse(fd_open(channel_fd))

        # Only the supervisor holds the listener now: a request waits in the backlog instead of being refused.
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        self.addCleanup(conn.close)
        conn.request("GET", "/")
        self.assertEqual(select.select([fd], [], [], 10)[0], [fd])
        assert conn.sock is not None
        self.assertEqual(select.select([conn.sock], [], [], 0)[0], [])

        next_server = ListenerHandover.from_environ({LISTEN_FD_ENV: str(fd)}).server(("127.0.0.1", port), NoContent)
        self.serve(next_server)
        with conn.getresponse() as response:
            self.assertEqual(response.status, 204)

    def test_reclaim_finds_nothing_when_the_daemon_sent_no_listener(self):
        supervisor_end, daemon_end = socket.socketpair()
        daemon_end.close()
        with supervisor_end:
            self.assertIsNone(run_updating.reclaim_listener(supervisor_end))

    def test_a_daemon_that_gets_no_ack_gives_up_and_closes_its_copy(self):
        supervisor_end, daemon_end = socket.socketpair()
        self.addCleanup(supervisor_end.close)
        handover = ListenerHandover(channel=daemon_end)
        server = handover.server(("127.0.0.1", 0), NoContent)
        started = time.monotonic()
        handover.release(server, ack_timeout=0.2)
        self.assertLess(time.monotonic() - started, 5)
        server.server_close()
        # The daemon is gone by the time the supervisor reads: the listener it finds is not kept.
        self.assertIsNone(run_updating.reclaim_listener(supervisor_end))

    def test_without_a_handover_channel_release_does_nothing(self):
        server = ListenerHandover.from_environ({}).server(("127.0.0.1", 0), NoContent)
        ListenerHandover.from_environ({}).release(server)
        server.server_close()

    def test_the_supervisor_and_the_daemon_name_the_same_variables(self):
        self.assertEqual((run_updating.LISTEN_FD_ENV, run_updating.HANDOVER_FD_ENV), (LISTEN_FD_ENV, HANDOVER_FD_ENV))


class ConfiguredDaemonHandoverTests(unittest.IsolatedAsyncioTestCase):
    def post(self, port: int) -> int | str | None:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            conn.request("POST", "/events", json.dumps({"external_id": str(time.monotonic_ns()), "payload": {}}),
                         {"Content-Type": "application/json"})
            return conn.getresponse().status
        except ConnectionRefusedError:
            return None
        except (ConnectionResetError, BrokenPipeError, http.client.RemoteDisconnected) as exc:
            # Queued on the old listener just as the reload closed it.
            return repr(exc)
        finally:
            conn.close()

    async def wait_for(self, check, serving: asyncio.Task[None]) -> None:
        deadline = time.monotonic() + 45
        while not await asyncio.to_thread(check):
            self.assertFalse(serving.done(), serving.done() and serving.exception())
            self.assertLess(time.monotonic(), deadline, self.stderr.getvalue())
            await asyncio.sleep(0.02)

    async def test_a_port_change_closes_the_inherited_port_and_the_stop_hands_back_the_new_listener(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.stderr = io.StringIO()
        self.enterContext(contextlib.redirect_stderr(self.stderr))
        path = root / "hibernaut.toml"

        def write_config(port: int) -> None:
            path.write_text(f'[runtime]\ndb_path = ":memory:"\nmemory_root = "{root / "mem"}"\n'
                            f'[channels]\nhost = "127.0.0.1"\nport = {port}\n')

        fd, inherited_port = listening_fd()
        new_port = free_port()
        supervisor_end, daemon_end = socket.socketpair()
        self.addCleanup(supervisor_end.close)
        write_config(inherited_port)
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        serving = asyncio.create_task(serve_configured(
            path, config, daemon, stop, reload_interval=0.02, outbox_interval_seconds=0.02,
            listeners=ListenerHandover(socket.socket(fileno=fd), daemon_end),
        ))
        await self.wait_for(lambda: self.post(inherited_port) == 202, serving)
        self.assertIn("listener inherited", self.stderr.getvalue())

        write_config(new_port)
        await self.wait_for(lambda: self.post(new_port) == 202, serving)
        # As without a supervisor: the daemon held the only copy, so the old port now refuses.
        await self.wait_for(lambda: self.post(inherited_port) is None, serving)

        stop.set()
        handed_back = await asyncio.to_thread(run_updating.reclaim_listener, supervisor_end)
        await asyncio.wait_for(serving, 15)
        assert handed_back is not None
        with socket.socket(fileno=handed_back) as listener:
            self.assertEqual(listener.getsockname(), ("127.0.0.1", new_port))


if __name__ == "__main__":
    unittest.main()
