from __future__ import annotations

import argparse
import importlib
import random
import signal
import ssl
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Final, cast

from switchboard.credentials import EnvCredentialBroker
from switchboard.envelope import CanonicalEvent
from switchboard.ports import (
    AuditSinkPort,
    ConsumerHandler,
    CredentialBrokerPort,
    DeliverStatus,
    NoopAuditSink,
    safe_audit,
)
from switchboard.relay_protocol import (
    AGENT_VERSION,
    CLOSE_AUTH_FAILED,
    CLOSE_NORMAL,
    CLOSE_SUPERSEDED,
    Ack,
    Close,
    Hello,
    Nack,
    Ping,
    Pong,
    Push,
    RelayMessage,
    RelayProtocolError,
    WebSocketConnection,
    WebSocketError,
    Welcome,
    connect,
    decode_message,
    encode_message,
)

DEFAULT_BACKOFF_SECONDS: Final = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
DEFAULT_JITTER: Final = 0.2
RELAY_CREDENTIAL_CHANNEL: Final = "relay"
_UNSENDABLE_CLOSE_CODES: Final = frozenset({1005, 1006, 1015})


def backoff_delay(
    attempt: int,
    schedule: Sequence[float],
    jitter: float,
    random_fn: Callable[[], float],
) -> float:
    base = schedule[min(attempt, len(schedule) - 1)]
    return base * (1 + jitter * (2 * random_fn() - 1))


class RelayAgent:
    def __init__(
        self,
        url: str,
        consumer_id: str,
        handler: ConsumerHandler,
        credentials: CredentialBrokerPort,
        *,
        audit: AuditSinkPort | None = None,
        ssl_context: ssl.SSLContext | None = None,
        backoff_seconds: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
        jitter: float = DEFAULT_JITTER,
        random_fn: Callable[[], float] = random.random,
        connect_timeout: float = 10.0,
        idle_timeout_sec: float | None = None,
        max_workers: int = 8,
        credential_channel: str = RELAY_CREDENTIAL_CHANNEL,
        agent_version: str = AGENT_VERSION,
    ) -> None:
        self.url = url
        self.consumer_id = consumer_id
        self.backoff_seconds = tuple(float(s) for s in backoff_seconds)
        self.jitter = jitter
        self._handler = handler
        self._credentials = credentials
        self._audit: AuditSinkPort = audit or NoopAuditSink()
        self._ssl_context = ssl_context
        self._random = random_fn
        self._connect_timeout = connect_timeout
        self._idle_timeout = idle_timeout_sec
        self._channel = credential_channel
        self._agent_version = agent_version
        self._pool = ThreadPoolExecutor(max_workers, thread_name_prefix="relay-agent-handler")
        self._lock = threading.Lock()
        self._ws: WebSocketConnection | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.connected = threading.Event()
        self.conn_epoch: int | None = None
        self.sessions = 0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self.run, name="switchboard-relay-agent", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        with self._lock:
            ws = self._ws
        if ws is not None:
            ws.send_close(CLOSE_NORMAL, "agent stopping")
            ws.abort()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None
        self._pool.shutdown(wait=False, cancel_futures=True)

    # Reconnects with jittered exponential backoff until stop(), or until another agent
    # with the same consumer_id takes over (close 4009).
    def run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            welcomed, superseded = self._session()
            if superseded:
                self._log("superseded by a newer connection for this consumer_id; not reconnecting")
                return
            if self._stop.is_set():
                return
            if welcomed:
                attempt = 0
            delay = backoff_delay(attempt, self.backoff_seconds, self.jitter, self._random)
            attempt += 1
            self._stop.wait(delay)

    def _session(self) -> tuple[bool, bool]:
        token = self._credentials.issue_ephemeral(self._channel)
        if not token:
            self._log(f"credential broker has no {self._channel!r} token")
            return False, False
        try:
            ws = connect(self.url, timeout=self._connect_timeout, ssl_context=self._ssl_context)
        except (OSError, WebSocketError) as exc:
            self._log(f"connect to {self.url} failed: {exc!r}")
            return False, False
        with self._lock:
            self._ws = ws
        welcomed = False
        done = threading.Event()
        welcome_timer = threading.Timer(self._connect_timeout, ws.abort)
        welcome_timer.daemon = True
        try:
            if self._stop.is_set():
                return False, False
            ws.send_text(encode_message(Hello(consumer_id=self.consumer_id, token=token, agent_version=self._agent_version)))
            welcome_timer.start()
            first = ws.recv()
            welcome_timer.cancel()
            if isinstance(first, Close):
                return False, self._closed(ws, first)
            welcome = decode_message(first)
            if not isinstance(welcome, Welcome):
                raise RelayProtocolError(f"expected welcome, got {type(welcome).__name__}")
            welcomed = True
            epoch = welcome.conn_epoch
            self.conn_epoch = epoch
            self.sessions += 1
            self.connected.set()
            safe_audit(self._audit, "relay_agent", "connected", consumer_id=self.consumer_id, detail={"conn_epoch": epoch})
            idle = self._idle_timeout or 3 * welcome.heartbeat_sec
            threading.Thread(target=self._watchdog, args=(ws, idle, done), daemon=True).start()
            while True:
                received = ws.recv()
                if isinstance(received, Close):
                    return True, self._closed(ws, received)
                try:
                    message = decode_message(received)
                except RelayProtocolError as exc:
                    self._log(f"dropping malformed frame: {exc}")
                    continue
                self._dispatch(ws, epoch, message)
        except (OSError, WebSocketError, RelayProtocolError) as exc:
            if not self._stop.is_set():
                self._log(f"relay connection lost: {exc!r}")
            return welcomed, False
        finally:
            welcome_timer.cancel()
            done.set()
            self.connected.clear()
            with self._lock:
                self._ws = None
            ws.close()
            if welcomed:
                safe_audit(self._audit, "relay_agent", "disconnected", consumer_id=self.consumer_id)

    def _dispatch(self, ws: WebSocketConnection, epoch: int, message: RelayMessage) -> None:
        match message:
            case Push():
                if message.conn_epoch == epoch:
                    self._pool.submit(self._handle_push, ws, epoch, message.event)
            case Ping():
                ws.send_text(encode_message(Pong(conn_epoch=epoch)))
            case _:
                pass

    # The handler returns only once the event is durably stored; only then is it acked.
    def _handle_push(self, ws: WebSocketConnection, epoch: int, event: CanonicalEvent) -> None:
        reply: Ack | Nack
        try:
            status: DeliverStatus | None = self._handler(event)
            if status is None or status == "acked":
                reply = Ack(conn_epoch=epoch, event_id=event.event_id)
            else:
                reply = Nack(conn_epoch=epoch, event_id=event.event_id, error=status, retryable=True)
        except Exception as exc:  # noqa: BLE001
            reply = Nack(conn_epoch=epoch, event_id=event.event_id, error=repr(exc), retryable=True)
        try:
            ws.send_text(encode_message(reply))
        except (OSError, WebSocketError):
            self._log(f"reply for {event.event_id} lost; the hub will push it again")

    def _closed(self, ws: WebSocketConnection, close: Close) -> bool:
        ws.send_close(CLOSE_NORMAL if close.code in _UNSENDABLE_CLOSE_CODES else close.code)
        if close.code == CLOSE_AUTH_FAILED:
            safe_audit(self._audit, "relay_agent", "auth_rejected", consumer_id=self.consumer_id, detail={"reason": close.reason})
            self._log(f"relay rejected authentication: {close.reason}")
        return close.code == CLOSE_SUPERSEDED

    # A half-open connection delivers nothing; without traffic for idle seconds, drop it.
    def _watchdog(self, ws: WebSocketConnection, idle: float, done: threading.Event) -> None:
        while not done.wait(min(idle / 4, 1.0)):
            if time.monotonic() - ws.last_received > idle:
                self._log(f"no traffic for {idle:.1f}s; reconnecting")
                ws.abort()
                return

    def _log(self, message: str) -> None:
        print(f"[relay-agent {self.consumer_id}] {message}", file=sys.stderr)


def _print_event(event: CanonicalEvent) -> None:
    print(event.to_json(), flush=True)


def load_handler(spec: str) -> ConsumerHandler:
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise ValueError("--handler must look like package.module:function")
    handler = getattr(importlib.import_module(module_name), attribute)
    if not callable(handler):
        raise ValueError(f"{spec} is not callable")
    return cast(ConsumerHandler, handler)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m switchboard.relay_agent",
        description="Connect to a Switchboard RelayHub and consume pushed events. "
        "The token comes from SWITCHBOARD_RELAY_TOKEN.",
    )
    parser.add_argument("--url", required=True, help="ws:// or wss:// URL of the hub's /ws/relay endpoint")
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument(
        "--handler",
        help="package.module:function called with each CanonicalEvent; default prints JSON lines to stdout",
    )
    parser.add_argument("--ca-file", help="CA bundle for wss:// with a private CA")
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    handler: ConsumerHandler = load_handler(args.handler) if args.handler else _print_event
    context = ssl.create_default_context(cafile=args.ca_file) if args.ca_file else None
    agent = RelayAgent(
        args.url,
        args.consumer_id,
        handler,
        EnvCredentialBroker(),
        ssl_context=context,
        connect_timeout=args.connect_timeout,
    )
    stopped = threading.Event()

    def _stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    agent.start()
    stopped.wait()
    agent.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
