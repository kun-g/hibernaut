from __future__ import annotations


class PeerError(Exception):
    pass


class TargetNotFound(PeerError):
    pass


class AmbiguousTarget(PeerError):
    pass


class UnsupportedProtocol(PeerError):
    pass


class PeerRefused(PeerError):
    pass


class SendTimeout(PeerError):
    pass


class ReceiverConflict(PeerError):
    pass


class CursorError(PeerError):
    pass
