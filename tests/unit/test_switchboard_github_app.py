from __future__ import annotations

import base64
import subprocess
import tempfile
import unittest
from pathlib import Path

from switchboard.config import ConfigError, RoutingConfig, parse_config
from switchboard.credentials import github_app_credentials
from switchboard.router import ScopeResolver
from switchboard.rs256 import RsaKeyError, load_private_key
from tests.support.switchboard_github_app import OPENSSL


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

    def test_rejects_non_rsa_pem(self) -> None:
        with self.assertRaises(RsaKeyError):
            load_private_key("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----")


class OwnerRoutingTests(unittest.TestCase):
    def test_owner_template_routes_unlisted_repos_of_that_owner(self) -> None:
        resolver = ScopeResolver(
            RoutingConfig(
                unmatched_policy="reject",
                github_repos={"kun-g/hibernaut": "proj:hibernaut"},
                github_owners={"kun-g": "proj:{repo}"},
            )
        )
        self.assertEqual(resolver.github("Kun-G/New-Tool"), "proj:new-tool")
        self.assertEqual(resolver.github("kun-g/hibernaut"), "proj:hibernaut")
        self.assertIsNone(resolver.github("LambdaTheory/team-context"))

    def test_config_parses_owners_and_app_owners(self) -> None:
        config = parse_config(
            {"routing": {"github_owners": {"Kun-G": "proj:{repo}"}}, "outbound": {"github_app_owners": ["kun-g"]}}
        )
        self.assertEqual(config.routing.github_owners, {"kun-g": "proj:{repo}"})
        self.assertEqual(config.outbound.github_app_owners, ("kun-g",))
        with self.assertRaises(ConfigError):
            parse_config({"outbound": {"github_app_owners": "kun-g"}})


class AppCredentialTests(unittest.TestCase):
    PEM = "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n"

    def test_accepts_raw_or_base64_pem_and_needs_both_values(self) -> None:
        encoded = base64.b64encode(self.PEM.encode()).decode()
        for value in (self.PEM, encoded):
            env = {"SWITCHBOARD_GITHUB_APP_ID": "1", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY": value}
            self.assertEqual(github_app_credentials(env), ("1", self.PEM.strip()))
        self.assertIsNone(github_app_credentials({"SWITCHBOARD_GITHUB_APP_ID": "1"}))
        with self.assertRaises(ValueError):
            github_app_credentials({"SWITCHBOARD_GITHUB_APP_ID": "1", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY": "not pem!"})


if __name__ == "__main__":
    unittest.main()
