from __future__ import annotations

import socket
from collections.abc import Iterable
from dataclasses import dataclass

from ccpeer.errors import PeerRefused, SendTimeout, TargetNotFound, UnsupportedProtocol
from ccpeer.protocol import (
    DEFAULT_MODE,
    SUPPORTED_PEER_PROTOCOLS,
    VERIFIED_CLAUDE_CODE_VERSIONS,
    auth_line,
    encode_envelope,
    frame_line,
    new_msg_id,
    user_frame,
)
from ccpeer.sessions import PathLike, Session, resolve


@dataclass(frozen=True, slots=True)
class SendResult:
    msg_id: str
    pid: int
    name: str | None
    session_id: str | None
    socket_path: str
    bytes_sent: int


def check_protocol(session: Session) -> None:
    if session.peer_protocol not in SUPPORTED_PEER_PROTOCOLS:
        raise UnsupportedProtocol(
            f"session {session.label()} runs Claude Code {session.version or 'unknown'} with "
            f"peerProtocol={session.peer_protocol}; ccpeer speaks {sorted(SUPPORTED_PEER_PROTOCOLS)} "
            f"(verified on Claude Code {', '.join(VERIFIED_CLAUDE_CODE_VERSIONS)}). "
            "Re-verify the frame format before adding the new version to SUPPORTED_PEER_PROTOCOLS.")


def send(target: str | Session, body: str, *, sender_name: str, reply_addr: str | None = None,
         mode: str = DEFAULT_MODE, config_dirs: Iterable[PathLike] | None = None, timeout: float = 5.0,
         msg_id: str | None = None) -> SendResult:
    session = resolve(target, config_dirs) if isinstance(target, str) else target
    check_protocol(session)
    msg_id = msg_id or new_msg_id()
    content = encode_envelope(body, from_addr=reply_addr, from_name=sender_name, from_mode=mode)
    payload = frame_line(user_frame(content, msg_id=msg_id, from_addr=reply_addr))
    token = session.token()
    if token is not None:
        payload = auth_line(token) + payload
    _deliver(session, payload, timeout)
    return SendResult(msg_id=msg_id, pid=session.pid, name=session.name, session_id=session.session_id,
                      socket_path=session.socket_path, bytes_sent=len(payload))


def _deliver(session: Session, payload: bytes, timeout: float) -> None:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        try:
            sock.connect(session.socket_path)
        except FileNotFoundError as exc:
            raise TargetNotFound(f"socket of {session.label()} is gone: {session.socket_path}") from exc
        except ConnectionRefusedError as exc:
            raise PeerRefused(f"{session.label()} refused the connection on {session.socket_path}") from exc
        except TimeoutError as exc:
            raise SendTimeout(f"connecting to {session.label()} timed out after {timeout}s") from exc
        try:
            sock.sendall(payload)
            # Half-close so the peer sees end-of-stream, then give it a moment to drain before closing.
            sock.shutdown(socket.SHUT_WR)
        except TimeoutError as exc:
            raise SendTimeout(f"writing to {session.label()} timed out after {timeout}s") from exc
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise PeerRefused(f"{session.label()} closed the connection mid-message") from exc
        sock.settimeout(0.2)
        try:
            while sock.recv(4096):
                pass
        except OSError:
            pass
    finally:
        sock.close()
