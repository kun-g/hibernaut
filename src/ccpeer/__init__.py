"""Message local Claude Code sessions over their peer sockets, and receive their SendMessage replies.

Standalone: depends only on the standard library (`.importlinter` contract `ccpeer-zero-business-deps`).
"""

from ccpeer.errors import (
    AmbiguousTarget,
    CursorError,
    PeerError,
    PeerRefused,
    ReceiverConflict,
    SendTimeout,
    TargetNotFound,
    UnsupportedProtocol,
)
from ccpeer.inbox import Inbox, InboxPage, JsonlLog
from ccpeer.protocol import (
    PEER_MODES,
    SUPPORTED_PEER_PROTOCOLS,
    VERIFIED_CLAUDE_CODE_VERSIONS,
    Envelope,
    decode_envelope,
    encode_envelope,
    frame_to_record,
    uds_address,
)
from ccpeer.receiver import Receiver, ReceiverConfig, ReceiverInfo, receiver_info, state_dir
from ccpeer.sender import SendResult, send
from ccpeer.sessions import Session, default_config_dirs, discover, resolve

__all__ = [
    "PEER_MODES",
    "SUPPORTED_PEER_PROTOCOLS",
    "VERIFIED_CLAUDE_CODE_VERSIONS",
    "AmbiguousTarget",
    "CursorError",
    "Envelope",
    "Inbox",
    "InboxPage",
    "JsonlLog",
    "PeerError",
    "PeerRefused",
    "Receiver",
    "ReceiverConfig",
    "ReceiverConflict",
    "ReceiverInfo",
    "SendResult",
    "SendTimeout",
    "Session",
    "TargetNotFound",
    "UnsupportedProtocol",
    "decode_envelope",
    "default_config_dirs",
    "discover",
    "encode_envelope",
    "frame_to_record",
    "receiver_info",
    "resolve",
    "send",
    "state_dir",
    "uds_address",
]
