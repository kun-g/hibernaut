from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import threading
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final, cast

from switchboard.envelope import CanonicalEvent, format_utc, parse_utc, utc_now

ReadFn = Callable[[int], bytes]

OP_CONTINUATION: Final = 0x0
OP_TEXT: Final = 0x1
OP_BINARY: Final = 0x2
OP_CLOSE: Final = 0x8
OP_PING: Final = 0x9
OP_PONG: Final = 0xA
_CONTROL_OPCODES: Final = frozenset({OP_CLOSE, OP_PING, OP_PONG})
_DATA_OPCODES: Final = frozenset({OP_CONTINUATION, OP_TEXT, OP_BINARY})

CLOSE_NORMAL: Final = 1000
CLOSE_GOING_AWAY: Final = 1001
CLOSE_PROTOCOL_ERROR: Final = 1002
CLOSE_UNSUPPORTED_DATA: Final = 1003
CLOSE_NO_STATUS: Final = 1005
CLOSE_INVALID_PAYLOAD: Final = 1007
CLOSE_TOO_BIG: Final = 1009
CLOSE_AUTH_FAILED: Final = 4001
CLOSE_SUPERSEDED: Final = 4009

_WS_GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
DEFAULT_MAX_MESSAGE_BYTES: Final = 1_048_576
AGENT_VERSION: Final = "1"


class WebSocketError(Exception):
    pass


class HandshakeError(WebSocketError):
    pass


class ConnectionClosed(WebSocketError):
    pass


class WebSocketProtocolError(WebSocketError):
    def __init__(self, message: str, close_code: int = CLOSE_PROTOCOL_ERROR) -> None:
        super().__init__(message)
        self.close_code = close_code


@dataclass(frozen=True, slots=True)
class Frame:
    fin: bool
    opcode: int
    payload: bytes
    masked: bool


@dataclass(frozen=True, slots=True)
class Close:
    code: int
    reason: str = ""


def accept_key(key: str) -> str:
    digest = hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def encode_frame(opcode: int, payload: bytes, *, mask: bool, fin: bool = True) -> bytes:
    head = bytearray([(0x80 if fin else 0) | opcode])
    mask_bit = 0x80 if mask else 0
    length = len(payload)
    if length < 126:
        head.append(mask_bit | length)
    elif length < 1 << 16:
        head.append(mask_bit | 126)
        head += struct.pack("!H", length)
    else:
        head.append(mask_bit | 127)
        head += struct.pack("!Q", length)
    if not mask:
        return bytes(head) + payload
    key = os.urandom(4)
    return bytes(head) + key + _apply_mask(payload, key)


def read_frame(read: ReadFn, *, max_payload: int) -> Frame:
    first, second = _read_exact(read, 2)
    fin = bool(first & 0x80)
    if first & 0x70:
        raise WebSocketProtocolError("reserved bits set without a negotiated extension")
    opcode = first & 0x0F
    if opcode not in _CONTROL_OPCODES and opcode not in _DATA_OPCODES:
        raise WebSocketProtocolError(f"unknown opcode {opcode:#x}")
    masked = bool(second & 0x80)
    length = second & 0x7F
    if length == 126:
        length = int(struct.unpack("!H", _read_exact(read, 2))[0])
    elif length == 127:
        length = int(struct.unpack("!Q", _read_exact(read, 8))[0])
    if opcode in _CONTROL_OPCODES and (length > 125 or not fin):
        raise WebSocketProtocolError("control frames must be unfragmented and at most 125 bytes")
    # Checked before reading so a hostile length cannot make us buffer it.
    if length > max_payload:
        raise WebSocketProtocolError(f"frame of {length} bytes exceeds limit", CLOSE_TOO_BIG)
    key = _read_exact(read, 4) if masked else b""
    payload = _read_exact(read, length)
    if masked:
        payload = _apply_mask(payload, key)
    return Frame(fin=fin, opcode=opcode, payload=payload, masked=masked)


def encode_close_payload(code: int, reason: str = "") -> bytes:
    return struct.pack("!H", code) + reason.encode("utf-8")[:123]


def parse_close_payload(payload: bytes) -> Close:
    if not payload:
        return Close(CLOSE_NO_STATUS)
    if len(payload) == 1:
        raise WebSocketProtocolError("close payload of one byte")
    code = int(struct.unpack("!H", payload[:2])[0])
    try:
        reason = payload[2:].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WebSocketProtocolError("close reason is not UTF-8", CLOSE_INVALID_PAYLOAD) from exc
    return Close(code, reason)


def _apply_mask(payload: bytes, key: bytes) -> bytes:
    if not payload:
        return payload
    repeated = (key * (len(payload) // 4 + 1))[: len(payload)]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(repeated, "big")).to_bytes(len(payload), "big")


def _read_exact(read: ReadFn, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = read(remaining)
        if not chunk:
            raise ConnectionClosed("connection closed mid-frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class WebSocketConnection:
    def __init__(
        self,
        sock: socket.socket,
        read: ReadFn,
        *,
        client_side: bool,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ) -> None:
        self._sock = sock
        self._read = read
        self._client_side = client_side
        self._max_message_bytes = max_message_bytes
        # Pushes, heartbeats and replies come from different threads; frames must not interleave.
        self._send_lock = threading.Lock()
        self._close_sent = False
        self.last_received = time.monotonic()

    @property
    def close_sent(self) -> bool:
        return self._close_sent

    def send_text(self, text: str) -> None:
        self._send(OP_TEXT, text.encode("utf-8"))

    def send_ping(self, payload: bytes = b"") -> None:
        self._send(OP_PING, payload)

    # Returns False when a close was already sent or the socket is gone.
    def send_close(self, code: int = CLOSE_NORMAL, reason: str = "") -> bool:
        with self._send_lock:
            if self._close_sent:
                return False
            self._close_sent = True
            try:
                self._sock.sendall(
                    encode_frame(OP_CLOSE, encode_close_payload(code, reason), mask=self._client_side)
                )
            except OSError:
                return False
        return True

    def recv(self) -> str | Close:
        parts: list[bytes] = []
        size = 0
        message_opcode: int | None = None
        while True:
            frame = read_frame(self._read, max_payload=self._max_message_bytes)
            self.last_received = time.monotonic()
            if frame.masked == self._client_side:
                raise WebSocketProtocolError(
                    "server frames must be unmasked" if self._client_side else "client frames must be masked"
                )
            if frame.opcode == OP_CLOSE:
                return parse_close_payload(frame.payload)
            if frame.opcode == OP_PING:
                try:
                    self._send(OP_PONG, frame.payload)
                except (OSError, ConnectionClosed):
                    pass
                continue
            if frame.opcode == OP_PONG:
                continue
            if frame.opcode == OP_CONTINUATION:
                if message_opcode is None:
                    raise WebSocketProtocolError("continuation without a started message")
            else:
                if message_opcode is not None:
                    raise WebSocketProtocolError("new message before the previous one finished")
                message_opcode = frame.opcode
            size += len(frame.payload)
            if size > self._max_message_bytes:
                raise WebSocketProtocolError("message exceeds limit", CLOSE_TOO_BIG)
            parts.append(frame.payload)
            if not frame.fin:
                continue
            if message_opcode != OP_TEXT:
                raise WebSocketProtocolError("only text messages are accepted", CLOSE_UNSUPPORTED_DATA)
            try:
                return b"".join(parts).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise WebSocketProtocolError("text message is not UTF-8", CLOSE_INVALID_PAYLOAD) from exc

    # Unblocks a reader in another thread without releasing the descriptor under it.
    def abort(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def close(self) -> None:
        self.abort()
        try:
            self._sock.close()
        except OSError:
            pass

    def _send(self, opcode: int, payload: bytes) -> None:
        data = encode_frame(opcode, payload, mask=self._client_side)
        with self._send_lock:
            if self._close_sent:
                raise ConnectionClosed("close already sent")
            self._sock.sendall(data)


def server_handshake_headers(headers: Mapping[str, str]) -> list[tuple[str, str]]:
    lowered = {key.lower(): value for key, value in headers.items()}
    if lowered.get("upgrade", "").strip().lower() != "websocket":
        raise HandshakeError("missing Upgrade: websocket")
    connection_tokens = {token.strip().lower() for token in lowered.get("connection", "").split(",")}
    if "upgrade" not in connection_tokens:
        raise HandshakeError("missing Connection: Upgrade")
    if lowered.get("sec-websocket-version", "").strip() != "13":
        raise HandshakeError("only Sec-WebSocket-Version 13 is supported")
    key = lowered.get("sec-websocket-key", "").strip()
    try:
        if len(base64.b64decode(key, validate=True)) != 16:
            raise HandshakeError("Sec-WebSocket-Key must encode 16 bytes")
    except ValueError as exc:
        raise HandshakeError("Sec-WebSocket-Key is not base64") from exc
    return [
        ("Upgrade", "websocket"),
        ("Connection", "Upgrade"),
        ("Sec-WebSocket-Accept", accept_key(key)),
    ]


def connect(
    url: str,
    *,
    timeout: float = 10.0,
    ssl_context: ssl.SSLContext | None = None,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
) -> WebSocketConnection:
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("ws", "wss") or not parts.hostname:
        raise HandshakeError(f"relay URL must be ws:// or wss://, got {url!r}")
    secure = parts.scheme == "wss"
    port = parts.port or (443 if secure else 80)
    path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    sock = socket.create_connection((parts.hostname, port), timeout=timeout)
    try:
        if secure:
            context = ssl_context or ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=parts.hostname)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        host = parts.hostname if parts.port is None else f"{parts.hostname}:{parts.port}"
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        sock.sendall(request.encode("ascii"))
        reader = sock.makefile("rb")
        status_line = reader.readline(65537).decode("iso-8859-1").strip()
        response_headers: dict[str, str] = {}
        while True:
            line = reader.readline(65537).decode("iso-8859-1")
            if line in ("\r\n", "\n", ""):
                break
            name, _, value = line.partition(":")
            response_headers[name.strip().lower()] = value.strip()
        pieces = status_line.split(" ", 2)
        if len(pieces) < 2 or pieces[1] != "101":
            raise HandshakeError(f"relay refused the upgrade: {status_line!r}")
        if response_headers.get("sec-websocket-accept") != accept_key(key):
            raise HandshakeError("relay answered with a wrong Sec-WebSocket-Accept")
        sock.settimeout(None)
        return WebSocketConnection(sock, reader.read, client_side=True, max_message_bytes=max_message_bytes)
    except BaseException:
        sock.close()
        raise


class RelayProtocolError(ValueError):
    pass


def _now_ts() -> str:
    return format_utc(utc_now())


@dataclass(frozen=True, slots=True)
class Hello:
    consumer_id: str
    token: str = field(repr=False)
    agent_version: str = AGENT_VERSION
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Welcome:
    consumer_id: str
    conn_epoch: int
    heartbeat_sec: float
    ack_timeout_sec: float
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Push:
    conn_epoch: int
    event: CanonicalEvent
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Ack:
    conn_epoch: int
    event_id: str
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Nack:
    conn_epoch: int
    event_id: str
    error: str
    retryable: bool = True
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Ping:
    conn_epoch: int
    ts: str = field(default_factory=_now_ts)


@dataclass(frozen=True, slots=True)
class Pong:
    conn_epoch: int
    ts: str = field(default_factory=_now_ts)


RelayMessage = Hello | Welcome | Push | Ack | Nack | Ping | Pong


def _seconds(value: float) -> int | float:
    return int(value) if float(value).is_integer() else float(value)


def encode_message(message: RelayMessage) -> str:
    data: dict[str, Any]
    match message:
        case Hello():
            data = {
                "type": "hello",
                "consumer_id": message.consumer_id,
                "token": message.token,
                "agent_version": message.agent_version,
            }
        case Welcome():
            data = {
                "type": "welcome",
                "consumer_id": message.consumer_id,
                "conn_epoch": message.conn_epoch,
                "heartbeat_sec": _seconds(message.heartbeat_sec),
                "ack_timeout_sec": _seconds(message.ack_timeout_sec),
            }
        case Push():
            data = {"type": "push", "conn_epoch": message.conn_epoch, "event": message.event.to_dict()}
        case Ack():
            data = {"type": "ack", "conn_epoch": message.conn_epoch, "event_id": message.event_id}
        case Nack():
            data = {
                "type": "nack",
                "conn_epoch": message.conn_epoch,
                "event_id": message.event_id,
                "error": message.error,
                "retryable": message.retryable,
            }
        case Ping():
            data = {"type": "ping", "conn_epoch": message.conn_epoch}
        case Pong():
            data = {"type": "pong", "conn_epoch": message.conn_epoch}
    data["ts"] = message.ts
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def decode_message(text: str) -> RelayMessage:
    try:
        raw: Any = json.loads(text)
    except ValueError as exc:
        raise RelayProtocolError(f"frame is not JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RelayProtocolError("frame must be a JSON object")
    data = cast(dict[str, Any], raw)
    kind = data.get("type")
    ts = _str(data, "ts")
    try:
        parse_utc(ts)
    except ValueError as exc:
        raise RelayProtocolError(f"ts is not ISO-8601: {ts!r}") from exc
    match kind:
        case "hello":
            return Hello(
                consumer_id=_str(data, "consumer_id"),
                token=_str(data, "token"),
                agent_version=_str(data, "agent_version"),
                ts=ts,
            )
        case "welcome":
            return Welcome(
                consumer_id=_str(data, "consumer_id"),
                conn_epoch=_int(data, "conn_epoch"),
                heartbeat_sec=_number(data, "heartbeat_sec"),
                ack_timeout_sec=_number(data, "ack_timeout_sec"),
                ts=ts,
            )
        case "push":
            event_data = data.get("event")
            if not isinstance(event_data, dict):
                raise RelayProtocolError("push.event must be an object")
            try:
                event = CanonicalEvent.from_dict(cast(dict[str, Any], event_data))
            except (KeyError, TypeError, ValueError) as exc:
                raise RelayProtocolError(f"push.event is not a CanonicalEvent: {exc!r}") from exc
            return Push(conn_epoch=_int(data, "conn_epoch"), event=event, ts=ts)
        case "ack":
            return Ack(conn_epoch=_int(data, "conn_epoch"), event_id=_str(data, "event_id"), ts=ts)
        case "nack":
            retryable = data.get("retryable", True)
            if not isinstance(retryable, bool):
                raise RelayProtocolError("nack.retryable must be a boolean")
            error = data.get("error")
            return Nack(
                conn_epoch=_int(data, "conn_epoch"),
                event_id=_str(data, "event_id"),
                error="" if error is None else str(error),
                retryable=retryable,
                ts=ts,
            )
        case "ping":
            return Ping(conn_epoch=_int(data, "conn_epoch"), ts=ts)
        case "pong":
            return Pong(conn_epoch=_int(data, "conn_epoch"), ts=ts)
        case _:
            raise RelayProtocolError(f"unknown frame type {kind!r}")


def _str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise RelayProtocolError(f"{key} must be a non-empty string")
    return value


def _int(data: Mapping[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RelayProtocolError(f"{key} must be an integer")
    return value


def _number(data: Mapping[str, Any], key: str) -> float:
    value = data.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise RelayProtocolError(f"{key} must be a positive number")
    return float(value)
