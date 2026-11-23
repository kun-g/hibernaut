from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import struct
from dataclasses import dataclass
from typing import Any, Final, Mapping, cast

OP_CONTINUATION: Final = 0x0
OP_TEXT: Final = 0x1
OP_BINARY: Final = 0x2
OP_CLOSE: Final = 0x8
OP_PING: Final = 0x9
OP_PONG: Final = 0xA

CLOSE_NORMAL: Final = 1000
CLOSE_GOING_AWAY: Final = 1001
CLOSE_PROTOCOL_ERROR: Final = 1002
CLOSE_TOO_BIG: Final = 1009
CLOSE_AUTH_FAILED: Final = 4001
CLOSE_SUPERSEDED: Final = 4009

_WS_GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
DEFAULT_MAX_MESSAGE_BYTES: Final = 1_048_576


class WSConnectionClosed(Exception):
    def __init__(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        super().__init__(f"ws closed ({code}): {reason}")
        self.code = code
        self.reason = reason


class WSProtocolError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class WSMessage:
    opcode: int
    payload: bytes


def compute_accept_key(sec_key: str) -> str:
    digest = hashlib.sha1((sec_key + _WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def encode_frame(opcode: int, payload: bytes, *, mask: bool) -> bytes:
    header = bytearray()
    header.append(0x80 | (opcode & 0x0F))
    length = len(payload)
    mask_bit = 0x80 if mask else 0x00
    if length < 126:
        header.append(mask_bit | length)
    elif length < 65536:
        header.append(mask_bit | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(mask_bit | 127)
        header.extend(struct.pack("!Q", length))
    if mask:
        mask_key = os.urandom(4)
        header.extend(mask_key)
        masked = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
        return bytes(header) + masked
    return bytes(header) + payload


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    expect_masked: bool,
    max_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
) -> WSMessage:
    try:
        head = await reader.readexactly(2)
    except asyncio.IncompleteReadError as exc:
        raise WSConnectionClosed(CLOSE_GOING_AWAY, "eof") from exc
    b0, b1 = head[0], head[1]
    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    if expect_masked and not masked:
        raise WSProtocolError("client frame must be masked")
    length = b1 & 0x7F
    if length == 126:
        ext = await reader.readexactly(2)
        length = int(struct.unpack("!H", ext)[0])
    elif length == 127:
        ext = await reader.readexactly(8)
        length = int(struct.unpack("!Q", ext)[0])
    if length > max_bytes:
        raise WSConnectionClosed(CLOSE_TOO_BIG, "message too large")
    mask_key = await reader.readexactly(4) if masked else b""
    try:
        raw_payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise WSConnectionClosed(CLOSE_GOING_AWAY, "truncated payload") from exc
    if masked:
        payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(raw_payload))
    else:
        payload = raw_payload
    if opcode == OP_CLOSE:
        code = int(struct.unpack("!H", payload[:2])[0]) if len(payload) >= 2 else CLOSE_NORMAL
        reason = payload[2:].decode("utf-8", errors="replace") if len(payload) > 2 else ""
        raise WSConnectionClosed(code, reason)
    return WSMessage(opcode=opcode, payload=payload)


async def send_json(writer: asyncio.StreamWriter, obj: Mapping[str, Any], *, mask: bool) -> None:
    data = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    writer.write(encode_frame(OP_TEXT, data, mask=mask))
    await writer.drain()


async def recv_json(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    expect_masked: bool,
    send_masked: bool,
    max_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
) -> dict[str, Any]:
    while True:
        msg = await read_frame(reader, expect_masked=expect_masked, max_bytes=max_bytes)
        if msg.opcode == OP_PING:
            writer.write(encode_frame(OP_PONG, msg.payload[:125], mask=send_masked))
            await writer.drain()
            continue
        if msg.opcode == OP_PONG:
            continue
        if msg.opcode != OP_TEXT:
            raise WSProtocolError(f"unexpected opcode {msg.opcode}")
        parsed = json.loads(msg.payload.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise WSProtocolError("expected JSON object frame")
        return cast(dict[str, Any], parsed)


async def send_close(
    writer: asyncio.StreamWriter,
    code: int = CLOSE_NORMAL,
    reason: str = "",
    *,
    mask: bool,
) -> None:
    payload = struct.pack("!H", code) + reason.encode("utf-8")[:120]
    try:
        writer.write(encode_frame(OP_CLOSE, payload, mask=mask))
        await writer.drain()
    except OSError:
        pass
    try:
        writer.close()
        await writer.wait_closed()
    except OSError:
        pass


async def server_handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, expected_path: str) -> None:
    raw = await reader.readuntil(b"\r\n\r\n")
    lines = raw.decode("latin-1").split("\r\n")
    req_line = lines[0].split(" ")
    if len(req_line) < 2 or req_line[0] != "GET" or req_line[1] != expected_path:
        writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        raise WSProtocolError(f"invalid path: {lines[0]}")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    sec_key = headers.get("sec-websocket-key")
    if headers.get("upgrade", "").lower() != "websocket" or not sec_key:
        writer.write(b"HTTP/1.1 426 Upgrade Required\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        raise WSProtocolError("missing websocket upgrade headers")
    accept = compute_accept_key(sec_key)
    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    writer.write(resp.encode("ascii"))
    await writer.drain()


async def client_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    host: str,
    path: str,
) -> None:
    sec_key = base64.b64encode(os.urandom(16)).decode("ascii")
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {sec_key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    writer.write(req.encode("ascii"))
    await writer.drain()
    raw = await reader.readuntil(b"\r\n\r\n")
    lines = raw.decode("latin-1").split("\r\n")
    if "101" not in lines[0]:
        raise WSProtocolError(f"handshake rejected: {lines[0]}")
    expected = compute_accept_key(sec_key)
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    if headers.get("sec-websocket-accept") != expected:
        raise WSProtocolError("invalid Sec-WebSocket-Accept")
