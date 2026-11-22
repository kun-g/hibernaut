from __future__ import annotations

import hmac
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from switchboard.config import RelayConfig
from switchboard.envelope import CanonicalEvent
from switchboard.ports import (
    AuditSinkPort,
    CredentialBrokerPort,
    DeliverOutcome,
    NoopAuditSink,
    safe_audit,
)
from switchboard.relay_protocol import (
    CLOSE_AUTH_FAILED,
    CLOSE_GOING_AWAY,
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
    WebSocketProtocolError,
    Welcome,
    decode_message,
    encode_message,
)

RELAY_CREDENTIAL_CHANNEL: Final = "relay"
# How long a closed connection may keep sending (its frames are dropped) before we cut it.
CLOSE_GRACE_SEC: Final = 2.0
_UNSENDABLE_CLOSE_CODES: Final = frozenset({1005, 1006, 1015})


@dataclass(slots=True, eq=False)
class _InFlight:
    event_id: str
    scope: str
    done: threading.Event = field(default_factory=threading.Event)
    outcome: DeliverOutcome | None = None


@dataclass(slots=True, eq=False)
class _RelayConnection:
    ws: WebSocketConnection
    consumer_id: str
    epoch: int
    peer: str
    in_flight: dict[str, _InFlight] = field(default_factory=lambda: {})
    busy_scopes: set[str] = field(default_factory=lambda: set())
    # False until welcome is on the wire: nothing else may be sent before it.
    ready: bool = False
    closed: bool = False
    missed_pings: int = 0
    last_ping_sent: float = 0.0


class RelayHub:
    def __init__(
        self,
        config: RelayConfig,
        credentials: CredentialBrokerPort,
        *,
        audit: AuditSinkPort | None = None,
        credential_channel: str = RELAY_CREDENTIAL_CHANNEL,
    ) -> None:
        self._config = config
        self._credentials = credentials
        self._audit: AuditSinkPort = audit or NoopAuditSink()
        self._channel = credential_channel
        # Guards routes, epochs and every connection's in-flight table.
        self._cond = threading.Condition()
        self._routes: dict[str, _RelayConnection] = {}
        self._epochs: dict[str, int] = {}
        self._sockets: set[WebSocketConnection] = set()
        self._listeners: list[Callable[[str], object]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.transport = WebSocketRelayTransport(self)

    @property
    def config(self) -> RelayConfig:
        return self._config

    def on_consumer_connected(self, listener: Callable[[str], object]) -> None:
        self._listeners.append(listener)

    def is_online(self, consumer_id: str) -> bool:
        with self._cond:
            return consumer_id in self._routes

    def conn_epoch(self, consumer_id: str) -> int | None:
        with self._cond:
            return self._epochs.get(consumer_id)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop, name="switchboard-relay-heartbeat", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None
        with self._cond:
            connections = list(self._routes.values())
            self._routes.clear()
            for conn in connections:
                self._fail_in_flight(conn)
            sockets = list(self._sockets)
            self._cond.notify_all()
        for conn in connections:
            conn.ws.send_close(CLOSE_GOING_AWAY, "relay hub stopping")
        for ws in sockets:
            ws.abort()

    # Runs on the HTTP handler thread for the whole life of one WebSocket connection.
    def serve(self, ws: WebSocketConnection, peer: str) -> None:
        with self._cond:
            if self._stop.is_set():
                ws.send_close(CLOSE_GOING_AWAY, "relay hub stopping")
                return
            self._sockets.add(ws)
        try:
            hello = self._authenticate(ws, peer)
            if hello is None:
                return
            conn = self._install(ws, hello.consumer_id, peer)
            if conn is None:
                return
            try:
                self._read_loop(conn)
            finally:
                self._evict(conn, "disconnected", close_code=None)
        finally:
            with self._cond:
                self._sockets.discard(ws)
            ws.abort()

    def push(self, consumer_id: str, event: CanonicalEvent) -> DeliverOutcome:
        ack_timeout = self._config.ack_timeout_sec
        deadline = time.monotonic() + ack_timeout
        with self._cond:
            while True:
                conn = self._routes.get(consumer_id)
                if conn is None or conn.closed:
                    return DeliverOutcome(status="offline")
                # in_flight = 1 per (scope, consumer_id) keeps each queue head-of-line ordered.
                if (
                    conn.ready
                    and event.scope not in conn.busy_scopes
                    and event.event_id not in conn.in_flight
                    and len(conn.in_flight) < self._config.max_in_flight
                ):
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Not an attempt: the push never left; the worker defers it like offline.
                    return DeliverOutcome(status="offline", error="relay window busy")
                self._cond.wait(remaining)
            slot = _InFlight(event.event_id, event.scope)
            conn.in_flight[event.event_id] = slot
            conn.busy_scopes.add(event.scope)
        try:
            try:
                conn.ws.send_text(encode_message(Push(conn_epoch=conn.epoch, event=event)))
            except (OSError, WebSocketError) as exc:
                self._evict(conn, "send_failed", close_code=CLOSE_GOING_AWAY, detail={"error": repr(exc)})
                return DeliverOutcome(status="offline", error=repr(exc))
            if slot.done.wait(ack_timeout):
                return slot.outcome or DeliverOutcome(status="offline")
            self._emit(
                "ack_timeout",
                consumer_id,
                event_id=event.event_id,
                detail={"conn_epoch": conn.epoch, "ack_timeout_sec": ack_timeout},
            )
            self._probe(conn)
            return DeliverOutcome(status="timeout", error=f"no ack within {ack_timeout}s")
        finally:
            with self._cond:
                conn.in_flight.pop(event.event_id, None)
                conn.busy_scopes.discard(event.scope)
                self._cond.notify_all()

    def _authenticate(self, ws: WebSocketConnection, peer: str) -> Hello | None:
        guard = threading.Lock()
        state = {"timed_out": False, "answered": False}

        def expire() -> None:
            with guard:
                if state["answered"]:
                    return
                state["timed_out"] = True
            self._reject(ws, peer, "auth_timeout", "hello timeout", None)
            _abort_later(ws)

        timer = threading.Timer(self._config.auth_timeout_sec, expire)
        timer.daemon = True
        timer.start()
        received: str | Close | None
        try:
            received = ws.recv()
        except (OSError, WebSocketError):
            received = None
        finally:
            timer.cancel()
        with guard:
            if state["timed_out"]:
                return None
            state["answered"] = True

        if received is None or isinstance(received, Close):
            self._emit("auth_failed", None, detail={"peer": peer, "reason": "closed before hello"})
            return None
        try:
            message = decode_message(received)
        except RelayProtocolError:
            message = None
        if not isinstance(message, Hello):
            self._reject(ws, peer, "auth_failed", "first frame must be hello", None)
            _drain(ws)
            return None
        expected = self._credentials.get_webhook_secret(self._channel)
        # Fail closed when no secret is configured; compare in constant time.
        if not expected or not hmac.compare_digest(message.token.encode("utf-8"), expected.encode("utf-8")):
            self._reject(ws, peer, "auth_failed", "authentication failed", message.consumer_id)
            _drain(ws)
            return None
        return message

    def _reject(
        self,
        ws: WebSocketConnection,
        peer: str,
        outcome: str,
        reason: str,
        consumer_id: str | None,
    ) -> None:
        self._emit(outcome, consumer_id, detail={"peer": peer, "reason": reason})
        ws.send_close(CLOSE_AUTH_FAILED, reason)

    def _install(self, ws: WebSocketConnection, consumer_id: str, peer: str) -> _RelayConnection | None:
        # Epoch bump and route swap are one step, so the newest handshake always owns the route.
        with self._cond:
            if self._stop.is_set():
                ws.send_close(CLOSE_GOING_AWAY, "relay hub stopping")
                return None
            epoch = self._epochs.get(consumer_id, 0) + 1
            self._epochs[consumer_id] = epoch
            conn = _RelayConnection(ws=ws, consumer_id=consumer_id, epoch=epoch, peer=peer)
            old = self._routes.get(consumer_id)
            self._routes[consumer_id] = conn
            if old is not None:
                self._fail_in_flight(old)
            self._cond.notify_all()
        welcome = Welcome(
            consumer_id=consumer_id,
            conn_epoch=epoch,
            heartbeat_sec=self._config.heartbeat_sec,
            ack_timeout_sec=self._config.ack_timeout_sec,
        )
        try:
            ws.send_text(encode_message(welcome))
        except (OSError, WebSocketError) as exc:
            self._evict(conn, "send_failed", close_code=CLOSE_GOING_AWAY, detail={"error": repr(exc)})
            return None
        with self._cond:
            conn.ready = True
            self._cond.notify_all()
        if old is not None:
            old.ws.send_close(CLOSE_SUPERSEDED, "superseded")
            _abort_later(old.ws)
            self._emit(
                "superseded",
                consumer_id,
                detail={"old_conn_epoch": old.epoch, "conn_epoch": epoch, "peer": old.peer},
            )
        self._emit("connected", consumer_id, detail={"conn_epoch": epoch, "peer": peer})
        print(f"[switchboard] relay consumer {consumer_id} connected (epoch {epoch}, {peer})", file=sys.stderr)
        for listener in self._listeners:
            try:
                listener(consumer_id)
            except Exception as exc:  # noqa: BLE001
                print(f"[switchboard] relay connect listener failed: {exc!r}", file=sys.stderr)
        return conn

    def _read_loop(self, conn: _RelayConnection) -> None:
        while True:
            try:
                received = conn.ws.recv()
            except WebSocketProtocolError as exc:
                conn.ws.send_close(exc.close_code, str(exc)[:100])
                self._emit("protocol_error", conn.consumer_id, detail={"error": str(exc)})
                return
            except (OSError, WebSocketError):
                return
            if isinstance(received, Close):
                code = CLOSE_NORMAL if received.code in _UNSENDABLE_CLOSE_CODES else received.code
                conn.ws.send_close(code)
                return
            try:
                message = decode_message(received)
            except RelayProtocolError as exc:
                self._emit("protocol_error", conn.consumer_id, detail={"error": str(exc)})
                continue
            self._handle(conn, message)

    def _handle(self, conn: _RelayConnection, message: RelayMessage) -> None:
        match message:
            case Ack() | Nack():
                with self._cond:
                    current = not conn.closed and self._routes.get(conn.consumer_id) is conn
                if not current or message.conn_epoch != conn.epoch:
                    self._emit(
                        "stale_frame",
                        conn.consumer_id,
                        event_id=message.event_id,
                        detail={
                            "type": "ack" if isinstance(message, Ack) else "nack",
                            "frame_epoch": message.conn_epoch,
                            "conn_epoch": conn.epoch,
                        },
                    )
                    return
                if isinstance(message, Ack):
                    outcome = DeliverOutcome(status="acked")
                else:
                    error = message.error or "nack"
                    outcome = DeliverOutcome(status="nack", error=f"{error} (retryable={message.retryable})")
                with self._cond:
                    slot = conn.in_flight.get(message.event_id)
                    if slot is not None and not slot.done.is_set():
                        slot.outcome = outcome
                        slot.done.set()
                        return
                self._emit("unmatched_frame", conn.consumer_id, event_id=message.event_id)
            case Ping():
                try:
                    conn.ws.send_text(encode_message(Pong(conn_epoch=conn.epoch)))
                except (OSError, WebSocketError):
                    pass
            case Pong():
                pass
            case _:
                self._emit("protocol_error", conn.consumer_id, detail={"error": f"unexpected {type(message).__name__}"})

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self._config.heartbeat_sec):
            with self._cond:
                connections = [conn for conn in self._routes.values() if conn.ready]
            for conn in connections:
                # Any frame since the last ping counts as alive; two silent intervals evict.
                if conn.last_ping_sent and conn.ws.last_received < conn.last_ping_sent:
                    conn.missed_pings += 1
                else:
                    conn.missed_pings = 0
                if conn.missed_pings >= 2:
                    self._evict(
                        conn,
                        "heartbeat_timeout",
                        close_code=CLOSE_GOING_AWAY,
                        detail={"missed_pings": conn.missed_pings},
                    )
                    continue
                conn.last_ping_sent = time.monotonic()
                self._probe(conn)

    def _probe(self, conn: _RelayConnection) -> None:
        try:
            conn.ws.send_text(encode_message(Ping(conn_epoch=conn.epoch)))
        except (OSError, WebSocketError) as exc:
            self._evict(conn, "send_failed", close_code=CLOSE_GOING_AWAY, detail={"error": repr(exc)})

    # Unacked pushes on an evicted connection go back to the worker as offline, not failed.
    def _evict(
        self,
        conn: _RelayConnection,
        reason: str,
        *,
        close_code: int | None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        with self._cond:
            current = self._routes.get(conn.consumer_id) is conn
            if current:
                del self._routes[conn.consumer_id]
            self._fail_in_flight(conn)
            self._cond.notify_all()
        if close_code is not None:
            conn.ws.send_close(close_code, reason)
            conn.ws.abort()
        if current:
            self._emit(reason, conn.consumer_id, detail={"conn_epoch": conn.epoch, **(detail or {})})
            print(
                f"[switchboard] relay consumer {conn.consumer_id} removed ({reason}, epoch {conn.epoch})",
                file=sys.stderr,
            )

    def _fail_in_flight(self, conn: _RelayConnection) -> None:
        conn.closed = True
        for slot in conn.in_flight.values():
            if not slot.done.is_set():
                slot.outcome = DeliverOutcome(status="offline", error="relay connection lost")
                slot.done.set()

    def _emit(
        self,
        outcome: str,
        consumer_id: str | None,
        *,
        event_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        safe_audit(self._audit, "relay", outcome, consumer_id=consumer_id, event_id=event_id, detail=detail)


class WebSocketRelayTransport:
    def __init__(self, hub: RelayHub) -> None:
        self._hub = hub

    def deliver(self, consumer_id: str, event: CanonicalEvent) -> DeliverOutcome:
        return self._hub.push(consumer_id, event)


def _abort_later(ws: WebSocketConnection, delay: float = CLOSE_GRACE_SEC) -> None:
    timer = threading.Timer(delay, ws.abort)
    timer.daemon = True
    timer.start()


# After sending a close, wait for the peer's close (or the grace period) so it reads ours.
def _drain(ws: WebSocketConnection) -> None:
    _abort_later(ws)
    while True:
        try:
            if isinstance(ws.recv(), Close):
                return
        except (OSError, WebSocketError):
            return
