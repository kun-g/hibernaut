from __future__ import annotations

import base64
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from switchboard.github_app import GitHubAppError, GitHubAppTokenSource
from switchboard.http_client import HttpResponse
from switchboard.outbound.github_adapter import GitHubOutboundAdapter
from switchboard.ports import OutboundMessage
from switchboard.rs256 import jwt_rs256, load_private_key
from tests.support.switchboard_support import StaticCredentials, FakeHttpClient
from tests.support.switchboard_github_app import OPENSSL


def _message(repo: str, number: int) -> OutboundMessage:
    return OutboundMessage(
        msg_id="m1",
        idempotency_key="k1",
        channel="github",
        kind="comment",
        target={"repo": repo, "number": number},
        correlation_key=None,
        title="",
        body_md="hi",
    )


def _b64url_decode(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


@unittest.skipIf(OPENSSL is None, "openssl CLI is needed to produce and verify reference keys")
class RS256Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _openssl(self, *args: str, stdin: bytes | None = None) -> bytes:
        assert OPENSSL is not None
        return subprocess.run([OPENSSL, *args], input=stdin, capture_output=True, check=True).stdout

    def _keys(self) -> tuple[str, str, Path]:
        generated = self.dir / "key.pem"
        self._openssl("genrsa", "-out", str(generated), "2048")
        pkcs8 = self._openssl("pkcs8", "-topk8", "-nocrypt", "-in", str(generated)).decode()
        # OpenSSL 3 writes PKCS#8 unless asked for -traditional; LibreSSL has no such flag and writes PKCS#1.
        try:
            pkcs1 = self._openssl("rsa", "-traditional", "-in", str(generated)).decode()
        except subprocess.CalledProcessError:
            pkcs1 = self._openssl("rsa", "-in", str(generated)).decode()
        pub = self.dir / "pub.pem"
        self._openssl("rsa", "-in", str(generated), "-pubout", "-out", str(pub))
        return pkcs1, pkcs8, pub

    def test_jwt_signature_verifies_with_openssl_for_pkcs1_and_pkcs8(self) -> None:
        pkcs1, pkcs8, pub = self._keys()
        self.assertIn("BEGIN RSA PRIVATE KEY", pkcs1)
        self.assertIn("BEGIN PRIVATE KEY", pkcs8)
        for pem in (pkcs1, pkcs8):
            token = jwt_rs256({"iss": "42", "iat": 1, "exp": 2}, load_private_key(pem))
            header, payload, signature = token.split(".")
            self.assertEqual(json.loads(_b64url_decode(header)), {"alg": "RS256", "typ": "JWT"})
            self.assertEqual(json.loads(_b64url_decode(payload))["iss"], "42")
            sig = self.dir / "sig.bin"
            sig.write_bytes(_b64url_decode(signature))
            out = self._openssl(
                "dgst", "-sha256", "-verify", str(pub), "-signature", str(sig),
                stdin=f"{header}.{payload}".encode(),
            )
            self.assertIn(b"Verified OK", out)


@unittest.skipIf(OPENSSL is None, "openssl CLI is needed to produce a test key")
class GitHubAppTokenSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        assert OPENSSL is not None
        pem = subprocess.run([OPENSSL, "genrsa", "2048"], capture_output=True, check=True).stdout.decode()
        cls.key = load_private_key(pem)

    def setUp(self) -> None:
        self.clock = 1_800_000_000.0
        self.http = FakeHttpClient()

    def _source(self, owners: tuple[str, ...] = ("kun-g",)) -> GitHubAppTokenSource:
        return GitHubAppTokenSource(
            self.http, app_id="5344992", key=self.key, allowed_owners=owners, now=lambda: self.clock
        )

    def _grant(self, token: str, expires_in: float = 3600) -> None:
        expires = self.clock + expires_in
        from datetime import UTC, datetime

        stamp = datetime.fromtimestamp(expires, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.http.responses += [
            HttpResponse(200, json.dumps({"id": 172117789}).encode()),
            HttpResponse(201, json.dumps({"token": token, "expires_at": stamp}).encode()),
        ]

    def test_mints_a_token_narrowed_to_the_repo_and_comment_permissions(self) -> None:
        self._grant("ghs_one")
        self.assertEqual(self._source().token_for("Kun-G/Hibernaut"), "ghs_one")
        lookup, mint = self.http.calls
        self.assertEqual(lookup["url"], "https://api.github.com/repos/kun-g/hibernaut/installation")
        self.assertTrue(lookup["headers"]["Authorization"].startswith("Bearer ey"))
        self.assertEqual(mint["url"], "https://api.github.com/app/installations/172117789/access_tokens")
        self.assertEqual(
            mint["json"], {"repositories": ["hibernaut"], "permissions": {"issues": "write", "pull_requests": "write"}}
        )

    def test_caches_until_ten_minutes_before_expiry(self) -> None:
        source = self._source()
        self._grant("ghs_one")
        source.token_for("kun-g/hibernaut")
        self.clock += 2900
        self.assertEqual(source.token_for("kun-g/hibernaut"), "ghs_one")
        self.assertEqual(len(self.http.calls), 2)
        self.clock += 200
        self._grant("ghs_two")
        self.assertEqual(source.token_for("kun-g/hibernaut"), "ghs_two")
        self.assertEqual(len(self.http.calls), 4)

    def test_refuses_owners_outside_the_allow_list_without_calling_github(self) -> None:
        with self.assertRaises(GitHubAppError) as caught:
            self._source().token_for("LambdaTheory/team-context")
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(self.http.calls, [])

    def test_missing_installation_is_not_retryable_and_5xx_is(self) -> None:
        self.http.responses.append(HttpResponse(404, b'{"message":"Not Found"}'))
        with self.assertRaises(GitHubAppError) as missing:
            self._source().token_for("kun-g/private")
        self.assertFalse(missing.exception.retryable)
        self.http.responses.append(HttpResponse(502, b"bad gateway"))
        with self.assertRaises(GitHubAppError) as flaky:
            self._source().token_for("kun-g/private")
        self.assertTrue(flaky.exception.retryable)

    def test_outbound_adapter_comments_with_the_app_token(self) -> None:
        self._grant("ghs_app")
        adapter = GitHubOutboundAdapter(self.http, StaticCredentials(), tokens=self._source())
        result = adapter.send(
            _message("kun-g/hibernaut", 43)
        )
        self.assertTrue(result.ok)
        self.assertEqual(self.http.calls[-1]["headers"]["Authorization"], "Bearer ghs_app")

        refused = GitHubOutboundAdapter(self.http, StaticCredentials(), tokens=self._source()).send(
            _message("someone/else", 1)
        )
        self.assertFalse(refused.ok)
        self.assertFalse(refused.retryable)


if __name__ == "__main__":
    unittest.main()
