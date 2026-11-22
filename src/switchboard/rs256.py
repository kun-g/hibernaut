from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass

# RS256 (RSASSA-PKCS1-v1_5 + SHA-256) in the standard library only: Switchboard may not depend on
# third-party packages, and a GitHub App JWT is the only place it signs anything.

_SHA256_DIGEST_INFO = bytes.fromhex("3031300d060960864801650304020105000420")
_PEM = re.compile(r"-----BEGIN (RSA )?PRIVATE KEY-----(.+?)-----END (RSA )?PRIVATE KEY-----", re.S)
_RSA_ENCRYPTION_OID = bytes.fromhex("2a864886f70d010101")


class RsaKeyError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RsaPrivateKey:
    n: int
    d: int
    p: int
    q: int
    dp: int
    dq: int
    qinv: int

    @property
    def size_bytes(self) -> int:
        return (self.n.bit_length() + 7) // 8

    def sign_sha256(self, message: bytes) -> bytes:
        k = self.size_bytes
        t = _SHA256_DIGEST_INFO + hashlib.sha256(message).digest()
        if k < len(t) + 11:
            raise RsaKeyError("RSA key too short for SHA-256 signatures")
        em = b"\x00\x01" + b"\xff" * (k - len(t) - 3) + b"\x00" + t
        m = int.from_bytes(em, "big")
        # CRT keeps a 2048-bit signature in the low milliseconds.
        s1 = pow(m, self.dp, self.p)
        s2 = pow(m, self.dq, self.q)
        h = (self.qinv * (s1 - s2)) % self.p
        return (s2 + h * self.q).to_bytes(k, "big")


def load_private_key(pem: str) -> RsaPrivateKey:
    match = _PEM.search(pem)
    if match is None:
        raise RsaKeyError("not a PEM RSA private key")
    der = base64.b64decode("".join(match.group(2).split()))
    if match.group(1) is None:
        der = _unwrap_pkcs8(der)
    fields = _sequence(der)
    if len(fields) < 9:
        raise RsaKeyError("PKCS#1 RSAPrivateKey needs nine integers")
    _, n, _, d, p, q, dp, dq, qinv = (_integer(f) for f in fields[:9])
    return RsaPrivateKey(n=n, d=d, p=p, q=q, dp=dp, dq=dq, qinv=qinv)


def jwt_rs256(claims: dict[str, object], key: RsaPrivateKey) -> str:
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode("ascii")
    return f"{header}.{payload}.{_b64url(key.sign_sha256(signing_input))}"


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unwrap_pkcs8(der: bytes) -> bytes:
    fields = _sequence(der)
    if len(fields) < 3 or _RSA_ENCRYPTION_OID not in fields[1]:
        raise RsaKeyError("PKCS#8 key is not rsaEncryption")
    tag, value, _ = _read(fields[2], 0)
    if tag != 0x04:
        raise RsaKeyError("PKCS#8 privateKey must be an OCTET STRING")
    return value


def _sequence(der: bytes) -> list[bytes]:
    tag, body, _ = _read(der, 0)
    if tag != 0x30:
        raise RsaKeyError("expected a DER SEQUENCE")
    items: list[bytes] = []
    offset = 0
    while offset < len(body):
        start = offset
        _, _, offset = _read(body, offset)
        items.append(body[start:offset])
    return items


def _integer(tlv: bytes) -> int:
    tag, value, _ = _read(tlv, 0)
    if tag != 0x02:
        raise RsaKeyError("expected a DER INTEGER")
    return int.from_bytes(value, "big")


def _read(buf: bytes, offset: int) -> tuple[int, bytes, int]:
    if offset + 2 > len(buf):
        raise RsaKeyError("truncated DER")
    tag = buf[offset]
    length = buf[offset + 1]
    offset += 2
    if length & 0x80:
        count = length & 0x7F
        if count == 0 or count > 4 or offset + count > len(buf):
            raise RsaKeyError("bad DER length")
        length = int.from_bytes(buf[offset : offset + count], "big")
        offset += count
    end = offset + length
    if end > len(buf):
        raise RsaKeyError("truncated DER")
    return tag, buf[offset:end], end
